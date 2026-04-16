-- ============================================================
-- TRADEFLOW NG — POSTGRESQL PRODUCTION SCHEMA
-- Run this on your PostgreSQL database ONCE to set up tables.
-- Compatible with: Supabase, Railway, Render, Neon, ElephantSQL
-- ============================================================


-- ------------------------------------------------------------
-- 1. REFERENCE TABLES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS states (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    zone        TEXT NOT NULL,
    is_hub      BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS markets (
    id          SERIAL PRIMARY KEY,
    state_id    INTEGER NOT NULL REFERENCES states(id),
    name        TEXT NOT NULL,
    city        TEXT NOT NULL,
    latitude    REAL,
    longitude   REAL,
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS commodities (
    id                  SERIAL PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    category            TEXT NOT NULL,
    perishability_class TEXT NOT NULL,
    unit_of_measure     TEXT NOT NULL,
    avg_weight_kg       REAL,
    notes               TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS vehicle_types (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    capacity_kg     REAL NOT NULL,
    capacity_units  INTEGER,
    fuel_efficiency REAL,
    notes           TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS corridors (
    id                  SERIAL PRIMARY KEY,
    origin_state_id     INTEGER NOT NULL REFERENCES states(id),
    dest_state_id       INTEGER NOT NULL REFERENCES states(id),
    distance_km         REAL,
    avg_travel_hours    REAL,
    road_quality        TEXT,
    is_active           BOOLEAN DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE(origin_state_id, dest_state_id)
);


-- ------------------------------------------------------------
-- 2. AGENTS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS agents (
    id              SERIAL PRIMARY KEY,
    full_name       TEXT NOT NULL,
    phone           TEXT UNIQUE NOT NULL,
    whatsapp        TEXT,
    state_id        INTEGER NOT NULL REFERENCES states(id),
    market_id       INTEGER REFERENCES markets(id),
    role            TEXT DEFAULT 'Reporter',
    is_active       BOOLEAN DEFAULT TRUE,
    onboarded_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);


-- ------------------------------------------------------------
-- 3. RAW DATA
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_submissions (
    id                  SERIAL PRIMARY KEY,
    kobo_submission_id  TEXT UNIQUE,
    agent_id            INTEGER REFERENCES agents(id),
    state_id            INTEGER REFERENCES states(id),
    market_id           INTEGER REFERENCES markets(id),
    commodity_id        INTEGER REFERENCES commodities(id),
    reported_price      REAL,
    reported_unit       TEXT,
    quantity_available  REAL,
    source_channel      TEXT DEFAULT 'Kobo',
    submission_date     DATE NOT NULL,
    raw_json            TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);


-- ------------------------------------------------------------
-- 4. CLEANED DATA
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cleaned_prices (
    id                  SERIAL PRIMARY KEY,
    raw_submission_id   INTEGER REFERENCES raw_submissions(id),
    state_id            INTEGER NOT NULL REFERENCES states(id),
    market_id           INTEGER REFERENCES markets(id),
    commodity_id        INTEGER NOT NULL REFERENCES commodities(id),
    price_per_unit      REAL NOT NULL,
    price_per_kg        REAL,
    quantity_available  REAL,
    price_date          DATE NOT NULL,
    is_outlier          BOOLEAN DEFAULT FALSE,
    outlier_reason      TEXT,
    is_confirmed        BOOLEAN DEFAULT FALSE,
    cleaning_notes      TEXT,
    cleaned_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE(state_id, commodity_id, price_date, market_id)
);


-- ------------------------------------------------------------
-- 5. TRANSPORT COSTS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transport_costs (
    id              SERIAL PRIMARY KEY,
    corridor_id     INTEGER NOT NULL REFERENCES corridors(id),
    commodity_id    INTEGER NOT NULL REFERENCES commodities(id),
    vehicle_type_id INTEGER REFERENCES vehicle_types(id),
    cost_per_unit   REAL NOT NULL,
    cost_per_kg     REAL,
    loading_cost    REAL DEFAULT 0,
    effective_date  DATE NOT NULL,
    expiry_date     DATE,
    source          TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);


-- ------------------------------------------------------------
-- 6. FORECASTS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS forecasts (
    id                  SERIAL PRIMARY KEY,
    state_id            INTEGER NOT NULL REFERENCES states(id),
    commodity_id        INTEGER NOT NULL REFERENCES commodities(id),
    forecast_date       DATE NOT NULL,
    generated_on        DATE NOT NULL,
    predicted_price     REAL NOT NULL,
    lower_bound         REAL,
    upper_bound         REAL,
    model_version       TEXT,
    is_shock_flagged    BOOLEAN DEFAULT FALSE,
    shock_reason        TEXT,
    created_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE(state_id, commodity_id, forecast_date, generated_on)
);


-- ------------------------------------------------------------
-- 7. OPTIMIZATION
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS optimization_runs (
    id                  SERIAL PRIMARY KEY,
    run_date            DATE NOT NULL,
    week_start          DATE NOT NULL,
    week_end            DATE NOT NULL,
    model_version       TEXT,
    solver_status       TEXT,
    total_profit_ngn    REAL,
    run_notes           TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS optimization_recommendations (
    id                      SERIAL PRIMARY KEY,
    run_id                  INTEGER NOT NULL REFERENCES optimization_runs(id),
    corridor_id             INTEGER REFERENCES corridors(id),
    commodity_id            INTEGER NOT NULL REFERENCES commodities(id),
    vehicle_type_id         INTEGER REFERENCES vehicle_types(id),
    recommended_quantity    REAL NOT NULL,
    recommended_quantity_kg REAL,
    buy_price               REAL,
    sell_price              REAL,
    transport_cost          REAL,
    expected_profit_ngn     REAL,
    profit_margin_pct       REAL,
    is_backhaul             BOOLEAN DEFAULT FALSE,
    is_shock_flagged        BOOLEAN DEFAULT FALSE,
    missing_cost_flag       BOOLEAN DEFAULT FALSE,
    shock_reason            TEXT,
    backhaul_pair_id        INTEGER,
    assigned_agent_id       INTEGER REFERENCES agents(id),
    status                  TEXT DEFAULT 'Pending',
    created_at              TIMESTAMP DEFAULT NOW()
);


-- ------------------------------------------------------------
-- 8. FEEDBACK
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS actual_outcomes (
    id                      SERIAL PRIMARY KEY,
    recommendation_id       INTEGER REFERENCES optimization_recommendations(id),
    agent_id                INTEGER REFERENCES agents(id),
    commodity_id            INTEGER NOT NULL REFERENCES commodities(id),
    corridor_id             INTEGER REFERENCES corridors(id),
    actual_buy_price        REAL,
    actual_sell_price       REAL,
    actual_transport_cost   REAL,
    actual_quantity         REAL,
    actual_profit_ngn       REAL,
    trip_date               DATE,
    outcome_notes           TEXT,
    data_source             TEXT DEFAULT 'Agent',
    submitted_at            TIMESTAMP DEFAULT NOW()
);


-- ------------------------------------------------------------
-- 9. PIPELINE LOGS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline_logs (
    id              SERIAL PRIMARY KEY,
    run_type        TEXT NOT NULL,
    status          TEXT NOT NULL,
    records_in      INTEGER,
    records_out     INTEGER,
    error_message   TEXT,
    duration_secs   REAL,
    run_at          TIMESTAMP DEFAULT NOW()
);


-- ============================================================
-- SEED DATA
-- ============================================================

INSERT INTO states (name, zone, is_hub) VALUES
    ('Lagos',    'South',  FALSE),
    ('Oyo',      'South',  TRUE),
    ('Ogun',     'South',  FALSE),
    ('Kwara',    'Bridge', FALSE),
    ('Kogi',     'Bridge', FALSE),
    ('Abuja',    'North',  TRUE),
    ('Nasarawa', 'North',  FALSE),
    ('Niger',    'North',  FALSE)
ON CONFLICT (name) DO NOTHING;

INSERT INTO commodities (name, category, perishability_class, unit_of_measure, avg_weight_kg) VALUES
    ('Yam',     'Tuber',     'Semi-Perishable', 'Bag (100kg)', 100),
    ('Maize',   'Grain',     'Durable',         'Bag (100kg)', 100),
    ('Rice',    'Grain',     'Durable',         'Bag (50kg)',   50),
    ('Tomato',  'Vegetable', 'Perishable',      'Crate',         8),
    ('Onion',   'Vegetable', 'Semi-Perishable', 'Bag (50kg)',   50),
    ('Pepper',  'Vegetable', 'Perishable',      'Basket',        10),
    ('Cassava', 'Tuber',     'Semi-Perishable', 'Bag (100kg)', 100),
    ('Garri',   'Tuber',     'Durable',         'Bag (50kg)',   50),
    ('Eggs',    'Protein',   'Perishable',      'Crate (30)',     2)
ON CONFLICT (name) DO NOTHING;

INSERT INTO vehicle_types (name, capacity_kg, capacity_units, fuel_efficiency) VALUES
    ('Keke / Tricycle',   300,   3, 35),
    ('Mini Truck (1T)',  1000,  10, 12),
    ('Medium Truck (3T)',3000,  30,  8),
    ('Large Truck (10T)',10000,100,  5)
ON CONFLICT (name) DO NOTHING;
