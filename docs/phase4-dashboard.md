# Phase 4 — The dashboard

```bash
make -f Makefile.dash dash-install
make -f Makefile.dash dash          # localhost:8501
```

Three tabs: trips, data quality, change data capture. It reads the same `.env`
dbt uses.

## Two decisions that matter more than the charts

**Every query is cached.** Streamlit re-executes the whole script top to bottom
on every widget interaction. Without `@st.cache_data`, moving one dropdown
fires every query again — which on Snowflake means waking the warehouse and
paying for it, per click. The connection uses `@st.cache_resource` instead,
because a connection is a shared handle, not a value to copy per caller.

**Table locations are resolved, not hard-coded.** Phase 1's models and phase
2's land in different schemas, and a dbt target change moves them again. The
app looks each table up in `INFORMATION_SCHEMA` once and caches the answer for
an hour.

## Chart choices

**Trips per month** — magnitude over an ordered time axis, one series, one hue,
no legend. The title names the series.

**Quarantined rows by rule** — a dot plot, and the reason is worth knowing.
The counts span 8 to 731,023, so a linear axis makes six of the eight rules
invisible slivers, and "we caught 8 implausible totals" is exactly the kind of
small number worth seeing. But a **bar cannot go on a log axis**: a bar is
anchored to zero, and log(0) is negative infinity, so the chart renders
completely empty. I only found that out by rendering it and looking at it —
which is the argument for always doing that. A dot has no baseline to anchor,
so it reads correctly on a log scale.

**Tier at purchase vs tier today** — a heatmap, because the question is "which
transitions are common", which is a lookup on a grid rather than a comparison
of lengths. One hue, light to dark. Never a rainbow.

**Customer versions over time** — validity windows are intervals, so the mark
is a bar with a start and an end on a time axis. Tier colours are mapped
explicitly per tier, so choosing a different customer cannot recolour them:
colour follows the entity, never its position in a filtered list.

## Colour and accessibility

Four categorical hues, assigned in fixed order and never cycled. They were
checked before use for colour-vision-deficiency separation and for contrast
against the chart surface, rather than picked by eye. One of them sits below
3:1 contrast on this surface, which obliges relief — so every chart ships with
the table behind it in an expander.

That expander earns its place twice over. A chart is an argument; the table is
the evidence. Anyone who doubts a bar can check it in one click.

## Limitation

The quarantine dataset lives in S3, not the warehouse, so that tab falls back
to the figures the curate job reported and says so on screen rather than
implying it queried them.
