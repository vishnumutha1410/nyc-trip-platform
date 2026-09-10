-- Runs automatically the first time the postgres container starts.
-- (Anything in /docker-entrypoint-initdb.d is executed once, against an empty
--  data directory. If you change this file you must `docker compose down -v`
--  to drop the volume, otherwise it will not run again.)

-- ---------------------------------------------------------------------------
-- The "application" database. Pretend this belongs to a team that has never
-- heard of you: they own these tables, they will not add an updated_at column
-- for your convenience, and they will not tell you when a row changes.
-- That constraint is the whole reason CDC exists.
-- ---------------------------------------------------------------------------

CREATE TABLE customers (
    customer_id   BIGSERIAL PRIMARY KEY,
    email         TEXT        NOT NULL,
    full_name     TEXT        NOT NULL,
    city          TEXT        NOT NULL,
    tier          TEXT        NOT NULL DEFAULT 'bronze',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id      BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT      NOT NULL REFERENCES customers(customer_id),
    status        TEXT        NOT NULL DEFAULT 'pending',
    amount        NUMERIC(10,2) NOT NULL,
    currency      TEXT        NOT NULL DEFAULT 'USD',
    placed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON orders (customer_id);
CREATE INDEX ON orders (status);

-- ---------------------------------------------------------------------------
-- REPLICA IDENTITY: the single most important line in this file.
--
-- By default Postgres writes only the PRIMARY KEY of a row into the WAL for an
-- UPDATE or DELETE. Debezium can then tell you the row changed, and what it
-- changed TO, but the "before" image is almost entirely null. You get:
--
--     before: {order_id: 41, customer_id: null, status: null, ...}
--     after:  {order_id: 41, customer_id: 7, status: "paid", ...}
--
-- which makes it impossible to answer "what was the status before?" - exactly
-- the question SCD Type 2 history is built on.
--
-- FULL tells Postgres to log the entire old row. It costs WAL volume, which is
-- why it is not the default, and it is the trade real teams argue about.
-- ---------------------------------------------------------------------------
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- A publication is Postgres's own concept: a named set of tables whose changes
-- are offered to logical replication subscribers. Debezium can create one for
-- itself, but that needs superuser and it silently picks FOR ALL TABLES.
-- Declaring it here means the set of replicated tables is in version control,
-- next to the schema, where a reviewer can see it.
-- ---------------------------------------------------------------------------
CREATE PUBLICATION dbz_publication FOR TABLE customers, orders;

-- Seed customers. Orders arrive from the load generator.
INSERT INTO customers (email, full_name, city, tier) VALUES
    ('ava.reed@example.com',     'Ava Reed',      'Brooklyn',  'bronze'),
    ('noah.patel@example.com',   'Noah Patel',    'Queens',    'bronze'),
    ('mia.chen@example.com',     'Mia Chen',      'Manhattan', 'silver'),
    ('liam.okafor@example.com',  'Liam Okafor',   'Bronx',     'bronze'),
    ('zoe.martins@example.com',  'Zoe Martins',   'Manhattan', 'gold'),
    ('kai.novak@example.com',    'Kai Novak',     'Jersey City','bronze'),
    ('ivy.rahman@example.com',   'Ivy Rahman',    'Queens',    'silver'),
    ('eli.torres@example.com',   'Eli Torres',    'Brooklyn',  'bronze'),
    ('nia.walsh@example.com',    'Nia Walsh',     'Hoboken',   'bronze'),
    ('omar.haddad@example.com',  'Omar Haddad',   'Manhattan', 'gold');
