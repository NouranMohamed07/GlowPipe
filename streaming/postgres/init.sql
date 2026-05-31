-- ============================================================
-- GlowPipe Streaming Layer — PostgreSQL Schema (Enhanced)
-- Answers 5 business questions that batch cannot answer
-- ============================================================

DROP TABLE IF EXISTS ingredient_spikes;
DROP TABLE IF EXISTS price_abandonment;
DROP TABLE IF EXISTS session_funnel;
DROP TABLE IF EXISTS allergen_outbreaks;
DROP TABLE IF EXISTS allergen_alerts;
DROP TABLE IF EXISTS trending_now;
DROP TABLE IF EXISTS live_events;

-- ── 1. live_events ────────────────────────────────────────────────────────────
-- Every user interaction. One row per event.
CREATE TABLE live_events (
    event_id            VARCHAR(64)      PRIMARY KEY,
    session_id          VARCHAR(16),
    event_type          VARCHAR(32)      NOT NULL,
    product_id          VARCHAR(64)      NOT NULL,
    user_id             VARCHAR(32),
    skin_type           VARCHAR(32),
    category            VARCHAR(64),
    price               NUMERIC(10,2),
    budget              NUMERIC(10,2),
    over_budget         BOOLEAN,
    rating              NUMERIC(3,1),
    source              VARCHAR(32),
    event_timestamp     TIMESTAMPTZ      NOT NULL,
    ingested_at         TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_le_time     ON live_events (event_timestamp DESC);
CREATE INDEX idx_le_product  ON live_events (product_id);
CREATE INDEX idx_le_skin     ON live_events (skin_type);
CREATE INDEX idx_le_type     ON live_events (event_type);
CREATE INDEX idx_le_session  ON live_events (session_id);


-- ── 2. trending_now ───────────────────────────────────────────────────────────
-- Q1: Which products are going viral right now?
-- 5-minute tumbling windows, updated every 30 seconds by Spark.
CREATE TABLE trending_now (
    id                  SERIAL           PRIMARY KEY,
    window_start        TIMESTAMPTZ      NOT NULL,
    window_end          TIMESTAMPTZ      NOT NULL,
    product_id          VARCHAR(64)      NOT NULL,
    skin_type           VARCHAR(32),
    category            VARCHAR(64),
    event_count         INTEGER          DEFAULT 0,
    view_count          INTEGER          DEFAULT 0,
    purchase_count      INTEGER          DEFAULT 0,
    cart_count          INTEGER          DEFAULT 0,
    abandon_count       INTEGER          DEFAULT 0,
    avg_rating          NUMERIC(3,2),
    avg_price           NUMERIC(10,2),
    trend_score         NUMERIC(10,2),
    ingested_at         TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    UNIQUE (window_start, product_id, skin_type)
);
CREATE INDEX idx_tn_window  ON trending_now (window_start DESC);
CREATE INDEX idx_tn_score   ON trending_now (trend_score DESC);
CREATE INDEX idx_tn_product ON trending_now (product_id);
CREATE INDEX idx_tn_skin    ON trending_now (skin_type);


-- ── 3. allergen_alerts ────────────────────────────────────────────────────────
-- Q2a: Raw unsafe product safety checks.
-- Every event where is_safe=false OR allergen_count>0 OR compat_score<60.
CREATE TABLE allergen_alerts (
    alert_id             SERIAL           PRIMARY KEY,
    event_id             VARCHAR(64)      NOT NULL,
    product_id           VARCHAR(64)      NOT NULL,
    skin_type            VARCHAR(32),
    compatibility_score  INTEGER,
    allergens_detected   TEXT[],
    allergen_count       INTEGER          DEFAULT 0,
    is_safe              BOOLEAN          DEFAULT TRUE,
    overall_safety_score INTEGER,
    pregnancy_safe       BOOLEAN,
    fungal_acne_safe     BOOLEAN,
    alert_timestamp      TIMESTAMPTZ      NOT NULL,
    ingested_at          TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_aa_time    ON allergen_alerts (alert_timestamp DESC);
CREATE INDEX idx_aa_product ON allergen_alerts (product_id);
CREATE INDEX idx_aa_safe    ON allergen_alerts (is_safe);
CREATE INDEX idx_aa_skin    ON allergen_alerts (skin_type);


-- ── 4. allergen_outbreaks ─────────────────────────────────────────────────────
-- Q2b: Outbreak detection — 3+ users report same product unsafe in 5 minutes.
-- This is the table that would trigger a push notification or product flag.
CREATE TABLE allergen_outbreaks (
    id                   SERIAL           PRIMARY KEY,
    window_start         TIMESTAMPTZ      NOT NULL,
    window_end           TIMESTAMPTZ      NOT NULL,
    product_id           VARCHAR(64)      NOT NULL,
    skin_type            VARCHAR(32),
    unsafe_report_count  INTEGER          NOT NULL,
    avg_compat_score     NUMERIC(5,1),
    avg_allergen_count   NUMERIC(4,1),
    outbreak_severity    VARCHAR(16),     -- medium | high | critical
    ingested_at          TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    UNIQUE (window_start, product_id, skin_type)
);
CREATE INDEX idx_ao_time     ON allergen_outbreaks (window_start DESC);
CREATE INDEX idx_ao_severity ON allergen_outbreaks (outbreak_severity);
CREATE INDEX idx_ao_product  ON allergen_outbreaks (product_id);


-- ── 5. session_funnel ─────────────────────────────────────────────────────────
-- Q3: Live conversion funnel — view → cart → purchase rates per skin type.
-- 10-minute windows. Shows if a checkout issue is happening RIGHT NOW.
CREATE TABLE session_funnel (
    id               SERIAL        PRIMARY KEY,
    window_start     TIMESTAMPTZ   NOT NULL,
    window_end       TIMESTAMPTZ   NOT NULL,
    skin_type        VARCHAR(32),
    category         VARCHAR(64),
    views            INTEGER       DEFAULT 0,
    cart_adds        INTEGER       DEFAULT 0,
    purchases        INTEGER       DEFAULT 0,
    abandons         INTEGER       DEFAULT 0,
    reviews          INTEGER       DEFAULT 0,
    cart_rate        NUMERIC(5,1), -- cart_adds / views × 100
    purchase_rate    NUMERIC(5,1), -- purchases / cart_adds × 100
    abandon_rate     NUMERIC(5,1), -- abandons / cart_adds × 100
    ingested_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (window_start, skin_type, category)
);
CREATE INDEX idx_sf_window ON session_funnel (window_start DESC);
CREATE INDEX idx_sf_skin   ON session_funnel (skin_type);


-- ── 6. price_abandonment ─────────────────────────────────────────────────────
-- Q4: Which price points are causing abandonment right now?
-- Retailers can react within the hour with a discount or bundle.
CREATE TABLE price_abandonment (
    id                   SERIAL         PRIMARY KEY,
    window_start         TIMESTAMPTZ    NOT NULL,
    window_end           TIMESTAMPTZ    NOT NULL,
    skin_type            VARCHAR(32),
    category             VARCHAR(64),
    avg_price            NUMERIC(10,2),
    avg_budget           NUMERIC(10,2),
    over_budget_count    INTEGER        DEFAULT 0,
    in_budget_count      INTEGER        DEFAULT 0,
    avg_abandon_price    NUMERIC(10,2),
    avg_purchase_price   NUMERIC(10,2),
    price_pressure       NUMERIC(5,1),  -- % of events where price > budget
    ingested_at          TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    UNIQUE (window_start, skin_type, category)
);
CREATE INDEX idx_pa_window   ON price_abandonment (window_start DESC);
CREATE INDEX idx_pa_pressure ON price_abandonment (price_pressure DESC);
CREATE INDEX idx_pa_skin     ON price_abandonment (skin_type);


-- ── 7. ingredient_spikes ──────────────────────────────────────────────────────
-- Q5: Cross-product ingredient concern spikes.
-- "Phenoxyethanol flagged in 6 products in 5 minutes" — invisible to batch.
CREATE TABLE ingredient_spikes (
    id                      SERIAL        PRIMARY KEY,
    window_start            TIMESTAMPTZ   NOT NULL,
    window_end              TIMESTAMPTZ   NOT NULL,
    ingredient_name         VARCHAR(255)  NOT NULL,
    concern_level           VARCHAR(16),  -- low | medium | high
    signal_count            INTEGER       DEFAULT 0,
    total_affected_products INTEGER       DEFAULT 0,
    ingested_at             TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (window_start, ingredient_name, concern_level)
);
CREATE INDEX idx_is_window      ON ingredient_spikes (window_start DESC);
CREATE INDEX idx_is_ingredient  ON ingredient_spikes (ingredient_name);
CREATE INDEX idx_is_concern     ON ingredient_spikes (concern_level);


-- ── Grafana read-only user ────────────────────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader WITH LOGIN PASSWORD 'grafana_readonly_pw';
    END IF;
END
$$;

GRANT CONNECT  ON DATABASE glowpipe_streaming TO grafana_reader;
GRANT USAGE    ON SCHEMA public               TO grafana_reader;
GRANT SELECT   ON ALL TABLES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO grafana_reader;
