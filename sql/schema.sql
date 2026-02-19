CREATE DATABASE IF NOT EXISTS public_transport_dw;
USE public_transport_dw;

CREATE TABLE IF NOT EXISTS berlin_stops (
    stop_id VARCHAR(50) PRIMARY KEY,
    stop_name TEXT,
    stop_lat DOUBLE,
    stop_lon DOUBLE
);

CREATE TABLE IF NOT EXISTS berlin_routes (
    route_id VARCHAR(50) PRIMARY KEY,
    agency_id VARCHAR(50),
    route_short_name VARCHAR(50),
    route_long_name TEXT,
    route_type INT
);

CREATE TABLE IF NOT EXISTS berlin_trips (
    trip_id VARCHAR(50) PRIMARY KEY,
    route_id VARCHAR(50),
    service_id VARCHAR(50),
    trip_headsign TEXT,
    FOREIGN KEY (route_id) REFERENCES berlin_routes(route_id)
);

CREATE TABLE IF NOT EXISTS berlin_stop_times (
    trip_id VARCHAR(50),
    arrival_time TIME,
    departure_time TIME,
    stop_id VARCHAR(50),
    stop_sequence INT,
    PRIMARY KEY (trip_id, stop_sequence),
    FOREIGN KEY (trip_id) REFERENCES berlin_trips(trip_id),
    FOREIGN KEY (stop_id) REFERENCES berlin_stops(stop_id)
);

CREATE TABLE IF NOT EXISTS berlin_calendar (
    service_id VARCHAR(50) PRIMARY KEY,
    monday INT,
    tuesday INT,
    wednesday INT,
    thursday INT,
    friday INT,
    saturday INT,
    sunday INT,
    start_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS paris_stops (
    stop_id VARCHAR(50) PRIMARY KEY,
    stop_name TEXT,
    stop_lat DOUBLE,
    stop_lon DOUBLE
);

CREATE TABLE IF NOT EXISTS paris_routes (
    route_id VARCHAR(50) PRIMARY KEY,
    agency_id VARCHAR(50),
    route_short_name VARCHAR(50),
    route_long_name TEXT,
    route_type INT
);

CREATE TABLE IF NOT EXISTS paris_trips (
    trip_id VARCHAR(50) PRIMARY KEY,
    route_id VARCHAR(50),
    service_id VARCHAR(50),
    trip_headsign TEXT,
    FOREIGN KEY (route_id) REFERENCES paris_routes(route_id)
);

CREATE TABLE IF NOT EXISTS paris_stop_times (
    trip_id VARCHAR(50),
    arrival_time TIME,
    departure_time TIME,
    stop_id VARCHAR(50),
    stop_sequence INT,
    PRIMARY KEY (trip_id, stop_sequence),
    FOREIGN KEY (trip_id) REFERENCES paris_trips(trip_id),
    FOREIGN KEY (stop_id) REFERENCES paris_stops(stop_id)
);

CREATE TABLE IF NOT EXISTS paris_calendar (
    service_id VARCHAR(50) PRIMARY KEY,
    monday INT,
    tuesday INT,
    wednesday INT,
    thursday INT,
    friday INT,
    saturday INT,
    sunday INT,
    start_date DATE,
    end_date DATE
);

CREATE INDEX idx_berlin_stop_times_stop_id ON berlin_stop_times(stop_id);
CREATE INDEX idx_paris_stop_times_stop_id ON paris_stop_times(stop_id);

# We remove invalid service periods.
CREATE TABLE berlin_calendar_clean AS
SELECT *
FROM berlin_calendar
WHERE end_date >= start_date;

CREATE TABLE paris_calendar_clean AS
SELECT *
FROM paris_calendar
WHERE end_date >= start_date;

#Clean Stop Times (Normalize Time), GTFS allows 25:30:00
#MySQL TIME stores it but analytics tools break.
#We convert to seconds since midnight.

DROP TABLE IF EXISTS berlin_stop_times_clean;
DROP TABLE IF EXISTS paris_stop_times_clean;

CREATE TABLE berlin_stop_times_clean (
    trip_id VARCHAR(50),
    stop_id VARCHAR(50),
    stop_sequence INT,
    arrival_sec INT,
    departure_sec INT
);

CREATE TABLE paris_stop_times_clean (
    trip_id VARCHAR(50),
    stop_id VARCHAR(50),
    stop_sequence INT,
    arrival_sec INT,
    departure_sec INT
);

CREATE TABLE berlin_trip_service AS
SELECT
    t.trip_id,
    t.route_id,
    t.service_id,
    r.route_type
FROM berlin_trips t
JOIN berlin_routes r ON t.route_id = r.route_id
JOIN berlin_calendar_clean c ON t.service_id = c.service_id;


CREATE TABLE paris_trip_service AS
SELECT
    t.trip_id,
    t.route_id,
    t.service_id,
    r.route_type
FROM paris_trips t
JOIN paris_routes r ON t.route_id = r.route_id
JOIN paris_calendar_clean c ON t.service_id = c.service_id;


CREATE TABLE fact_stop_events (
    city VARCHAR(20),
    route_type INT,
    trip_id VARCHAR(50),
    stop_id VARCHAR(50),
    arrival_sec INT,
    departure_sec INT
);

ALTER TABLE fact_stop_events
ADD INDEX idx_city (city),
ALGORITHM=INPLACE,
LOCK=NONE;

ALTER TABLE fact_stop_events
ADD INDEX idx_trip (trip_id),
ALGORITHM=INPLACE,
LOCK=NONE;

ALTER TABLE fact_stop_events
ADD INDEX idx_route_type (route_type),
ALGORITHM=INPLACE,
LOCK=NONE;

ALTER TABLE fact_stop_events
ADD INDEX idx_stop (stop_id),
ALGORITHM=INPLACE,
LOCK=NONE;

ALTER TABLE fact_stop_events
ADD INDEX idx_arrival (arrival_sec),
ALGORITHM=INPLACE,
LOCK=NONE;

