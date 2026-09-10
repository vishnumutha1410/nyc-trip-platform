"""Streamlit dashboard over the warehouse.

    streamlit run dashboards/app.py

Reads Snowflake credentials from .env, the same ones dbt uses. Nothing is
hard-coded to one schema: table locations are resolved from
INFORMATION_SCHEMA at startup, because phase 1's models land in a different
schema from phase 2's and hard-coding either would break the moment you
change a dbt target.

ONE THING TO UNDERSTAND BEFORE READING THE CODE
    Streamlit re-executes this entire file top to bottom on every widget
    interaction. Without caching, moving one dropdown fires every query in
    here again - which on Snowflake means waking the warehouse and paying for
    it, per click. Every query goes through @st.cache_data with a TTL. That
    single decorator is the difference between a dashboard that costs cents
    and one that quietly costs real money.
"""
from __future__ import annotations

import os

import altair as alt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="NYC Trip Platform", layout="wide")

# --------------------------------------------------------------------------
# Colour. Three categorical slots, validated for colour-vision deficiency
# separation and contrast against the chart surface before being used.
# Assigned in fixed order and mapped explicitly to entities, never cycled -
# so filtering the data cannot repaint the series that survive.
# --------------------------------------------------------------------------
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_MUTED = "#52514e"
GRID = "#e7e6e2"

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]  # blue, orange, aqua, yellow
TIER_COLOURS = {
    "bronze": SERIES[0],
    "silver": SERIES[1],
    "gold": SERIES[2],
    "platinum": SERIES[3],
}


def chart_theme():
    return {
        "config": {
            "background": SURFACE,
            "view": {"stroke": "transparent"},
            "axis": {
                "labelColor": INK_MUTED,
                "titleColor": INK_MUTED,
                "gridColor": GRID,
                "domainColor": GRID,
                "tickColor": GRID,
                "labelFontSize": 12,
                "titleFontSize": 12,
                "titleFontWeight": "normal",
            },
            "legend": {"labelColor": INK_MUTED, "titleColor": INK_MUTED},
            "title": {"color": INK, "fontSize": 15, "fontWeight": 600, "anchor": "start"},
        }
    }


alt.themes.register("platform", chart_theme)
alt.themes.enable("platform")


# --------------------------------------------------------------------------
# Snowflake
# --------------------------------------------------------------------------

@st.cache_resource
def connect():
    """One connection for the whole session.

    cache_resource, not cache_data: a connection is not a value to be copied
    per caller, it is a shared handle. Using cache_data here would try to
    pickle a socket.
    """
    import snowflake.connector

    missing = [k for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
               if not os.getenv(k)]
    if missing:
        st.error(f"Missing in .env: {', '.join(missing)}")
        st.stop()

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "NYC_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "NYC_PLATFORM"),
        role=os.getenv("SNOWFLAKE_ROLE") or None,
    )


@st.cache_data(ttl=600, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    cur = connect().cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()


@st.cache_data(ttl=3600, show_spinner=False)
def locate(table: str) -> str | None:
    """Fully qualified name for a table, wherever dbt put it."""
    db = os.getenv("SNOWFLAKE_DATABASE", "NYC_PLATFORM")
    df = q(
        f"select table_schema from {db}.information_schema.tables "
        f"where upper(table_name) = upper('{table}') "
        f"order by case when table_schema = 'RAW' then 1 else 0 end limit 1"
    )
    if df.empty:
        return None
    return f"{db}.{df.iloc[0, 0]}.{table}"


def missing(name: str) -> None:
    st.info(f"`{name}` not found in this database. Run `make dbt` first.")


def table_view(df: pd.DataFrame, label: str = "View the numbers") -> None:
    """Every chart ships with the table behind it.

    Two reasons, one of them not obvious. The obvious one is accessibility:
    some of these fills sit below 3:1 contrast against the surface, and the
    rule for that is to provide relief - visible labels or the table. The
    less obvious one is trust. A chart is an argument; the table is the
    evidence. Anyone who doubts a bar can check it in one click.
    """
    with st.expander(label):
        st.dataframe(df, use_container_width=True, hide_index=True)


# --------------------------------------------------------------------------

st.title("NYC Trip Platform")
st.caption("Batch and CDC pipelines, read straight from the warehouse.")

tab_batch, tab_quality, tab_cdc = st.tabs(
    ["Trips", "Data quality", "Change data capture"]
)

# ---------------------------------------------------------------- trips ---
with tab_batch:
    fct = locate("fct_trips")
    if not fct:
        missing("fct_trips")
    else:
        totals = q(f"""
            select count(*) as trips,
                   round(sum(total_amount)) as revenue,
                   round(avg(trip_distance), 2) as avg_miles,
                   round(avg(trip_duration_seconds) / 60, 1) as avg_minutes
            from {fct}
        """)
        r = totals.iloc[0]

        # Hero numbers, not a chart. Four single values have no shape to show;
        # a bar chart of four unrelated measures would be a chart for its own
        # sake. See references: sometimes the answer is not a chart.
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Trips", f"{int(r.TRIPS):,}")
        c2.metric("Revenue", f"${int(r.REVENUE):,}")
        c3.metric("Avg distance", f"{r.AVG_MILES} mi")
        c4.metric("Avg duration", f"{r.AVG_MINUTES} min")

        st.divider()

        monthly = q(f"""
            select to_char(date_trunc('month', pickup_date_key), 'YYYY-MM') as month,
                   count(*) as trips
            from {fct}
            group by 1 order by 1
        """)

        # Magnitude over an ordered time axis, one series. One hue, no legend -
        # the title names the series. Rounded data-ends, thin bars, recessive
        # grid.
        st.altair_chart(
            alt.Chart(monthly, title="Trips per month")
            .mark_bar(size=26, cornerRadiusTopLeft=4, cornerRadiusTopRight=4,
                      color=SERIES[0])
            .encode(
                x=alt.X("MONTH:N", title=None, axis=alt.Axis(labelAngle=0)),
                y=alt.Y("TRIPS:Q", title="trips", axis=alt.Axis(format="~s")),
                tooltip=[alt.Tooltip("MONTH:N", title="month"),
                         alt.Tooltip("TRIPS:Q", title="trips", format=",")],
            )
            .properties(height=300),
            use_container_width=True,
        )
        table_view(monthly)

        zones = q(f"""
            select z.zone_name as pickup_zone, z.borough, count(*) as trips
            from {fct} f
            join {locate('dim_zone')} z on f.pickup_zone_key = z.zone_id
            group by 1, 2 order by trips desc limit 12
        """) if locate("dim_zone") else pd.DataFrame()

        if not zones.empty:
            # Ranked categories with long names: horizontal bars, sorted by
            # value. Vertical bars here would need rotated labels, which are
            # harder to read and cost vertical space.
            st.altair_chart(
                alt.Chart(zones, title="Busiest pickup zones")
                .mark_bar(size=18, cornerRadiusTopRight=4, cornerRadiusBottomRight=4,
                          color=SERIES[0])
                .encode(
                    y=alt.Y("PICKUP_ZONE:N", sort="-x", title=None),
                    x=alt.X("TRIPS:Q", title="trips", axis=alt.Axis(format="~s")),
                    tooltip=["PICKUP_ZONE", "BOROUGH",
                             alt.Tooltip("TRIPS:Q", format=",")],
                )
                .properties(height=380),
                use_container_width=True,
            )
            table_view(zones)

# -------------------------------------------------------------- quality ---
with tab_quality:
    st.subheader("Quarantine")
    st.caption(
        "Rows failing a quality rule are written to a separate dataset with the "
        "name of the rule attached, never dropped. This is that dataset."
    )

    quar = locate("quarantine_summary")
    if quar:
        rules = q(f"select * from {quar} order by rows desc")
    else:
        # The quarantine dataset lives in S3, not Snowflake. Rather than
        # pretend otherwise, show the figures the curate job reported and say
        # plainly where they came from.
        rules = pd.DataFrame({
            "RULE": ["fare_not_negative", "distance_positive", "duration_plausible",
                     "total_not_negative", "pickup_before_dropoff",
                     "distance_plausible", "pickup_outside_source_month",
                     "total_plausible"],
            "ROWS": [731023, 720062, 19365, 2861, 2364, 1016, 49, 8],
        })
        st.caption("Source: the curate job's summary. The quarantine dataset "
                   "itself is in S3, not the warehouse.")

    # A dot plot, not a bar chart, and the reason is worth knowing.
    #
    # These counts span 8 to 731,023. On a linear axis the six smaller rules
    # are invisible slivers, and "we caught 8 implausible totals" is exactly
    # the kind of small number worth seeing. But a BAR cannot go on a log
    # axis: a bar is anchored to zero, and log(0) is negative infinity, so
    # the chart renders empty. (I found that out by rendering it and looking.)
    #
    # A dot plot has no baseline to anchor - the mark is the value itself - so
    # it reads correctly on a log scale. The rule behind each dot is drawn
    # from 1, not 0, for the same reason.
    order = rules.sort_values("ROWS", ascending=False)["RULE"].tolist()
    base = alt.Chart(rules).encode(
        y=alt.Y("RULE:N", sort=order, title=None),
        # Explicit decade ticks. A log scale's default minor gridlines put ten
        # lines between every decade, which is more ink than information.
        x=alt.X("ROWS:Q", title="rows (log scale)",
                scale=alt.Scale(type="log", domainMin=1),
                axis=alt.Axis(format="~s", values=[1, 10, 100, 1_000, 10_000,
                                                   100_000, 1_000_000])),
        tooltip=["RULE", alt.Tooltip("ROWS:Q", format=",")],
    )
    st.altair_chart(
        (
            base.mark_rule(size=2, color=GRID).encode(x2=alt.datum(1))
            + base.mark_circle(size=150, color=SERIES[1])
            # Direct labels on every dot. Only eight of them, and the whole
            # point of the log axis is that the exact magnitudes matter.
            + base.mark_text(align="left", dx=12, fontSize=12, color=INK_MUTED)
                  .encode(text=alt.Text("ROWS:Q", format=","))
        )
        .properties(height=300, title="Quarantined rows by rule",
                    padding={"right": 80}),
        use_container_width=True,
    )
    table_view(rules)

    total = int(rules["ROWS"].sum())
    c1, c2 = st.columns(2)
    c1.metric("Rows quarantined", f"{total:,}")
    c2.metric("Rules enforced", len(rules))

# ------------------------------------------------------------------ cdc ---
with tab_cdc:
    tier_fct = locate("fct_orders_with_tier_at_purchase")
    hist = locate("dim_customer_history")

    if not tier_fct or not hist:
        missing("fct_orders_with_tier_at_purchase / dim_customer_history")
    else:
        head = q(f"""
            select count(*) as orders,
                   count_if(tier_at_purchase <> tier_today) as misattributed,
                   round(sum(case when tier_at_purchase <> tier_today
                                  then amount else 0 end), 2) as misattributed_revenue
            from {tier_fct}
        """).iloc[0]

        c1, c2, c3 = st.columns(3)
        c1.metric("Orders", f"{int(head.ORDERS):,}")
        c2.metric("Would be misfiled without SCD2", f"{int(head.MISATTRIBUTED):,}")
        c3.metric("Revenue affected", f"${head.MISATTRIBUTED_REVENUE:,.0f}")

        st.caption(
            "The source here is a load generator that changes customer tiers far "
            "more often than a real business would, so this proportion is a "
            "property of the simulation. The real number is much smaller and "
            "never zero."
        )

        st.divider()

        moved = q(f"""
            select tier_at_purchase, tier_today, count(*) as orders,
                   round(sum(amount), 2) as revenue
            from {tier_fct}
            where tier_at_purchase <> tier_today
            group by 1, 2 order by orders desc
        """)

        # Two categorical dimensions and one measure. A heatmap reads this
        # better than grouped bars: the question is "which transitions are
        # common", which is a lookup on a grid, not a comparison of lengths.
        # Sequential = one hue, light to dark. Never a rainbow.
        st.altair_chart(
            alt.Chart(moved, title="Tier at purchase vs tier today")
            .mark_rect(stroke=SURFACE, strokeWidth=2, cornerRadius=4)
            .encode(
                x=alt.X("TIER_TODAY:N", title="tier today",
                        axis=alt.Axis(labelAngle=0)),
                y=alt.Y("TIER_AT_PURCHASE:N", title="tier at purchase"),
                color=alt.Color("ORDERS:Q", title="orders",
                                scale=alt.Scale(scheme="blues")),
                tooltip=["TIER_AT_PURCHASE", "TIER_TODAY",
                         alt.Tooltip("ORDERS:Q", format=","),
                         alt.Tooltip("REVENUE:Q", format=",.2f")],
            )
            .properties(height=280),
            use_container_width=True,
        )
        table_view(moved)

        st.divider()
        st.subheader("One customer's history")
        st.caption(
            "Reconstructed entirely from the Postgres write-ahead log. The "
            "database was never asked to remember any of it."
        )

        ids = q(f"select distinct customer_id from {hist} order by 1")
        chosen = st.selectbox("customer", ids["CUSTOMER_ID"].tolist())

        versions = q(f"""
            select customer_id, tier, city, valid_from,
                   coalesce(valid_to, current_timestamp()) as valid_to,
                   is_current, lsn
            from {hist}
            where customer_id = {int(chosen)}
            order by lsn
        """)

        # Validity windows are intervals, so the mark is a bar with a start and
        # an end on a time axis. Colour carries tier identity and is mapped
        # explicitly per tier, so picking a different customer cannot recolour
        # the tiers.
        present = [t for t in TIER_COLOURS if t in set(versions["TIER"])]
        st.altair_chart(
            alt.Chart(versions, title=f"Customer {chosen}: versions over time")
            .mark_bar(height=22, cornerRadius=4, stroke=SURFACE, strokeWidth=2)
            .encode(
                x=alt.X("VALID_FROM:T", title=None),
                x2="VALID_TO:T",
                y=alt.Y("CITY:N", title="city"),
                color=alt.Color(
                    "TIER:N", title="tier",
                    scale=alt.Scale(domain=present,
                                    range=[TIER_COLOURS[t] for t in present]),
                ),
                tooltip=["TIER", "CITY", "VALID_FROM", "VALID_TO", "IS_CURRENT",
                         alt.Tooltip("LSN:Q", format="d")],
            )
            .properties(height=240),
            use_container_width=True,
        )
        table_view(versions, "View the version rows")

        st.caption(
            "Windows are half-open: valid_from inclusive, valid_to exclusive, so "
            "no two versions are ever valid at the same instant. A dbt test "
            "asserts that on every build."
        )
