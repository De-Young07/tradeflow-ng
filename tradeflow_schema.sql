-- ============================================================
-- TRADEFLOW NG — COMPLETE DATABASE SCHEMA
-- Compatible with: SQLite (MVP) and PostgreSQL (Production)
-- Author: TradeFlow NG
-- Version: 1.0
-- ============================================================


-- ------------------------------------------------------------
-- 1. REFERENCE / LOOKUP TABLES
--    These are mostly static. Populate once, rarely change.
-- ------------------------------------------------------------

-- States covered by the network
CREATE TABLE states (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,           -- e.g. 'Oyo', 'Lagos'
    zone            TEXT NOT NULL,                  -- 'North', 'South', 'Bridge'
    is_hub          BOOLEAN DEFAULT FALSE,           -- TRUE for Abuja, Ibadan
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Markets within states
CREATE TABLE markets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    state_id        INTEGER NOT NULL REFERENCES states(id),
    name            TEXT NOT NULL,                  -- e.g. 'Bodija Market'
    city            TEXT NOT NULL,
    latitude        REAL,                           -- For future map layer
    longitude       REAL,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Commodity master list
CREATE TABLE commodities (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT NOT NULL UNIQUE,       -- e.g. 'Yam', 'Tomato'
    category            TEXT NOT NULL,              -- 'Tuber', 'Grain', 'Vegetable', 'Protein'
    perishability_class TEXT NOT NULL,              -- 'Durable', 'Semi-Perishable', 'Perishable'
    unit_of_measure     TEXT NOT NULL,              -- 'Bag (100kg)', 'Crate', 'Basket', 'Dozen'
    avg_weight_kg       REAL,                       -- Standard unit weight in kg
    notes               TEXT,                       -- e.g. 'Garri = processed cassava'
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Truck / vehicle types used by drivers
CREATE TABLE vehicle_types (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,           -- e.g. 'Mini Truck', 'Medium Truck', '18-Wheeler'
    capacity_kg     REAL NOT NULL,                  -- Max load in kg
    capacity_units  INTEGER,                        -- Max bags/crates (approx)
    fuel_efficiency REAL,                           -- km per litre (approx)
    notes           TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trade corridors (origin-destination pairs)
CREATE TABLE corridors (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    origin_state_id     INTEGER NOT NULL REFERENCES states(id),
    dest_state_id       INTEGER NOT NULL REFERENCES states(id),
    distance_km         REAL,
    avg_travel_hours    REAL,                       -- More useful than km in NG conditions
    road_quality        TEXT,                       -- 'Good', 'Fair', 'Poor'
    is_active           BOOLEAN DEFAULT TRUE,
    notes               TEXT,                       -- e.g. 'Avoid rainy season'
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(origin_state_id, dest_state_id)
);


-- ------------------------------------------------------------
-- 2. AGENTS TABLE
--    Your field network
-- ------------------------------------------------------------

CREATE TABLE agents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    full_name       TEXT NOT NULL,
    phone           TEXT UNIQUE NOT NULL,
    whatsapp        TEXT,
    state_id        INTEGER NOT NULL REFERENCES states(id),
    market_id       INTEGER REFERENCES markets(id),
    role            TEXT DEFAULT 'Reporter',        -- 'Reporter', 'Supervisor', 'Driver'
    is_active       BOOLEAN DEFAULT TRUE,
    onboarded_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ------------------------------------------------------------
-- 3. RAW DATA LAYER
--    Direct from KoboToolbox API — never modify this table
-- ------------------------------------------------------------

CREATE TABLE raw_submissions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    kobo_submission_id  TEXT UNIQUE,                -- Kobo's own UUID
    agent_id            INTEGER REFERENCES agents(id),
    state_id            INTEGER REFERENCES states(id),
    market_id           INTEGER REFERENCES markets(id),
    commodity_id        INTEGER REFERENCES commodities(id),
    reported_price      REAL,                       -- Price as agent reported it
    reported_unit       TEXT,                       -- Unit as agent described it
    quantity_available  REAL,                       -- Estimated supply in market
    source_channel      TEXT DEFAULT 'Kobo',        -- 'Kobo', 'WhatsApp', 'Manual'
    submission_date     DATE NOT NULL,
    raw_json            TEXT,                       -- Full Kobo JSON blob (for audit)
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ------------------------------------------------------------
-- 4. CLEANED DATA LAYER
--    Processed, validated, standardized prices
-- ------------------------------------------------------------

CREATE TABLE cleaned_prices (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_submission_id   INTEGER REFERENCES raw_submissions(id),
    state_id            INTEGER NOT NULL REFERENCES states(id),
    market_id           INTEGER REFERENCES markets(id),
    commodity_id        INTEGER NOT NULL REFERENCES commodities(id),
    price_per_unit      REAL NOT NULL,              -- Standardized to commodity unit
    price_per_kg        REAL,                       -- Normalized to per kg
    quantity_available  REAL,
    price_date          DATE NOT NULL,
    is_outlier          BOOLEAN DEFAULT FALSE,      -- Flagged by z-score check
    outlier_reason      TEXT,
    is_confirmed        BOOLEAN DEFAULT FALSE,      -- Confirmed by 2nd agent
    cleaning_notes      TEXT,
    cleaned_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(state_id, commodity_id, price_date, market_id)
);


-- ------------------------------------------------------------
-- 5. TRANSPORT COSTS TABLE
--    Cost to move 1 unit of commodity along each corridor
-- ------------------------------------------------------------

CREATE TABLE transport_costs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    corridor_id         INTEGER NOT NULL REFERENCES corridors(id),
    commodity_id        INTEGER NOT NULL REFERENCES commodities(id),
    vehicle_type_id     INTEGER REFERENCES vehicle_types(id),
    cost_per_unit       REAL NOT NULL,              -- NGN per commodity unit
    cost_per_kg         REAL,                       -- NGN per kg
    loading_cost        REAL DEFAULT 0,             -- Extra handling cost
    effective_date      DATE NOT NULL,
    expiry_date         DATE,                       -- NULL = still current
    source              TEXT,                       -- 'Agent Report', 'Survey', 'Estimate'
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ------------------------------------------------------------
-- 6. FORECASTS TABLE
--    Prophet model outputs
-- ------------------------------------------------------------

CREATE TABLE forecasts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    state_id            INTEGER NOT NULL REFERENCES states(id),
    commodity_id        INTEGER NOT NULL REFERENCES commodities(id),
    forecast_date       DATE NOT NULL,              -- The date being predicted
    generated_on        DATE NOT NULL,              -- When forecast was run
    predicted_price     REAL NOT NULL,
    lower_bound         REAL,                       -- Prophet uncertainty interval
    upper_bound         REAL,
    model_version       TEXT,                       -- e.g. 'prophet_v1.2'
    is_shock_flagged    BOOLEAN DEFAULT FALSE,      -- Z-score anomaly flag
    shock_reason        TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(state_id, commodity_id, forecast_date, generated_on)
);


-- ------------------------------------------------------------
-- 7. OPTIMIZATION RESULTS
--    PuLP solver outputs — the core recommendation
-- ------------------------------------------------------------

CREATE TABLE optimization_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date            DATE NOT NULL,
    week_start          DATE NOT NULL,              -- The week this covers
    week_end            DATE NOT NULL,
    model_version       TEXT,
    solver_status       TEXT,                       -- 'Optimal', 'Feasible', 'Infeasible'
    total_profit_ngn    REAL,                       -- Objective function value
    run_notes           TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE optimization_recommendations (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                  INTEGER NOT NULL REFERENCES optimization_runs(id),
    corridor_id             INTEGER NOT NULL REFERENCES corridors(id),
    commodity_id            INTEGER NOT NULL REFERENCES commodities(id),
    vehicle_type_id         INTEGER REFERENCES vehicle_types(id),
    recommended_quantity    REAL NOT NULL,          -- Units to move
    recommended_quantity_kg REAL,
    buy_price               REAL,                   -- Expected price at origin
    sell_price              REAL,                   -- Expected price at destination
    transport_cost          REAL,
    expected_profit_ngn     REAL,
    profit_margin_pct       REAL,
    is_backhaul             BOOLEAN DEFAULT FALSE,  -- Part of backhaul route
    backhaul_pair_id        INTEGER,                -- Links to the outbound rec
    assigned_agent_id       INTEGER REFERENCES agents(id),
    status                  TEXT DEFAULT 'Pending', -- 'Pending','Accepted','Rejected','Completed'
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ------------------------------------------------------------
-- 8. FEEDBACK / ACTUAL OUTCOMES
--    Closes the loop — feeds back into Prophet retraining
-- ------------------------------------------------------------

CREATE TABLE actual_outcomes (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
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
    outcome_notes           TEXT,                   -- Agent free text
    data_source             TEXT DEFAULT 'Agent',   -- 'Agent', 'WhatsApp', 'Kobo'
    submitted_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ------------------------------------------------------------
-- 9. SYSTEM / AUDIT LOG
--    Track every pipeline run for debugging
-- ------------------------------------------------------------

CREATE TABLE pipeline_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type        TEXT NOT NULL,                  -- 'Ingestion', 'Cleaning', 'Forecast', 'Optimization'
    status          TEXT NOT NULL,                  -- 'Success', 'Failed', 'Partial'
    records_in      INTEGER,
    records_out     INTEGER,
    error_message   TEXT,
    duration_secs   REAL,
    run_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- SEED DATA — Insert your initial reference data below
-- ============================================================

-- States
INSERT INTO states (name, zone, is_hub) VALUES
    ('Lagos',     'South',  FALSE),
    ('Oyo',       'South',  TRUE),   -- Ibadan is your southern hub
    ('Ogun',      'South',  FALSE),
    ('Kwara',     'Bridge', FALSE),
    ('Kogi',      'Bridge', FALSE),
    ('Abuja',     'North',  TRUE),   -- FCT is your northern hub
    ('Nasarawa',  'North',  FALSE),
    ('Niger',     'North',  FALSE);

-- Commodities
INSERT INTO commodities (name, category, perishability_class, unit_of_measure, avg_weight_kg) VALUES
    ('Yam',     'Tuber',     'Semi-Perishable', 'Bag (100kg)',  100),
    ('Maize',   'Grain',     'Durable',         'Bag (100kg)',  100),
    ('Rice',    'Grain',     'Durable',         'Bag (50kg)',    50),
    ('Tomato',  'Vegetable', 'Perishable',      'Crate',         8),
    ('Onion',   'Vegetable', 'Semi-Perishable', 'Bag (50kg)',    50),
    ('Pepper',  'Vegetable', 'Perishable',      'Basket',        10),
    ('Cassava', 'Tuber',     'Semi-Perishable', 'Bag (100kg)',  100),
    ('Garri',   'Tuber',     'Durable',         'Bag (50kg)',    50),
    ('Eggs',    'Protein',   'Perishable',      'Crate (30)',     2);

-- Vehicle types
INSERT INTO vehicle_types (name, capacity_kg, capacity_units, fuel_efficiency) VALUES
    ('Keke / Tricycle',  300,   3,  35),
    ('Mini Truck (1T)',  1000,  10, 12),
    ('Medium Truck (3T)',3000,  30,  8),
    ('Large Truck (10T)',10000, 100, 5);
