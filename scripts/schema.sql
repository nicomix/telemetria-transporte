CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE TABLE IF NOT EXISTS quarantine_events (
    id SERIAL PRIMARY KEY,
    tracking_id VARCHAR(255),
    vehicle_id VARCHAR(255),
    company_id VARCHAR(255),
    route_id VARCHAR(255),
    driver_id VARCHAR(255),
    event_time VARCHAR(255),
    received_at VARCHAR(255),
    latitude VARCHAR(255),
    longitude VARCHAR(255),
    speed VARCHAR(255),
    fuel_level VARCHAR(255),
    battery_level VARCHAR(255),
    odometer_km VARCHAR(255),
    passenger_count VARCHAR(255),
    event_type VARCHAR(255),
    error_reason TEXT NOT NULL,
    quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS telemetry_history (
    tracking_id UUID PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL,
    company_id VARCHAR(50) NOT NULL,
    route_id VARCHAR(50),
    driver_id VARCHAR(50),
    event_time TIMESTAMP NOT NULL,
    received_at TIMESTAMP NOT NULL,
    latitude NUMERIC(9,6) NOT NULL,
    longitude NUMERIC(9,6) NOT NULL,
    speed NUMERIC(5,2) NOT NULL,
    fuel_level NUMERIC(5,2),
    battery_level NUMERIC(5,2),
    odometer_km NUMERIC(12,2) NOT NULL,
    passenger_count INT,
    event_type VARCHAR(10) NOT NULL,

    distance_diff_km NUMERIC(10,4) DEFAULT 0.0,
    time_diff_seconds INT DEFAULT 0,
    is_odometer_reset BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_telemetry_history_vehicle_time
ON telemetry_history (vehicle_id, event_time);

CREATE INDEX IF NOT EXISTS idx_telemetry_history_company
ON telemetry_history (company_id);


CREATE TABLE IF NOT EXISTS fleet_current_state (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    tracking_id UUID NOT NULL,
    event_time TIMESTAMP NOT NULL,
    latitude NUMERIC(9,6) NOT NULL,
    longitude NUMERIC(9,6) NOT NULL,
    operational_state VARCHAR(20) NOT NULL,
    driver_id VARCHAR(50),
    fuel_level NUMERIC(5,2),
    odometer_km NUMERIC(12,2) NOT NULL,

    has_low_fuel_alarm BOOLEAN DEFAULT FALSE,
    has_overspeed_alarm BOOLEAN DEFAULT FALSE,
    has_no_signal_alarm BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS daily_operational_metrics (
    vehicle_id VARCHAR(50) NOT NULL,
    company_id VARCHAR(50) NOT NULL,
    operational_date DATE NOT NULL,
    total_distance_km NUMERIC(12,2) NOT NULL DEFAULT 0.0,
    avg_speed_kmh NUMERIC(5,2) NOT NULL DEFAULT 0.0,
    max_speed_kmh NUMERIC(5,2) NOT NULL DEFAULT 0.0,
    estimated_fuel_consumption_pct NUMERIC(5,2) NOT NULL DEFAULT 0.0,
    overspeed_events_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (vehicle_id, operational_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_date
ON daily_operational_metrics (operational_date);