-- Weather ETL — Database initialization
-- Run once before the first DAG execution.
--
-- Context :
-- This table stores hourly weather snapshots fetched from OpenWeatherMap.
-- Each record represents the weather state of a city at a given timestamp.
-- The UNIQUE constraint on (city, recorded_at) prevents duplicate entries
-- on retries or backfills — safe to run multiple times via IF NOT EXISTS.
--
-- Scale consideration :
-- For a small number of cities polled hourly this table grows roughly
-- 24 x nb_cities rows per day — manageable with standard PostgreSQL indexes.
-- If the scope grows (hundreds of cities, higher frequency), consider :
--   - Partitioning by recorded_at (monthly or weekly partitions)
--   - A TimescaleDB extension for time-series optimisation
--   - An archiving strategy for data older than N months

CREATE TABLE IF NOT EXISTS weather_data (
    id          SERIAL PRIMARY KEY,
    city        VARCHAR(100) NOT NULL,
    temperature FLOAT NOT NULL,
    humidity    INTEGER NOT NULL,
    description VARCHAR(255),
    recorded_at TIMESTAMP NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE (city, recorded_at)
);

CREATE INDEX IF NOT EXISTS idx_weather_city
    ON weather_data (city);

CREATE INDEX IF NOT EXISTS idx_weather_recorded_at
    ON weather_data (recorded_at DESC);