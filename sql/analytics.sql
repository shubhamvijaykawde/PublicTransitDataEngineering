CREATE TABLE agg_hourly_traffic AS
SELECT
    city,
    route_type,
    FLOOR(arrival_sec / 3600) AS hour_of_day,
    COUNT(*) AS total_stop_events
FROM fact_stop_events
GROUP BY city, route_type, hour_of_day;

ALTER TABLE agg_hourly_traffic
ADD INDEX idx_hour (city, route_type, hour_of_day);

CREATE TABLE kpi_peak_hours AS
SELECT
    city,
    route_type,
    hour_of_day,
    total_stop_events,
    RANK() OVER (
        PARTITION BY city, route_type
        ORDER BY total_stop_events DESC
    ) AS congestion_rank
FROM agg_hourly_traffic;

CREATE TABLE dim_route_type (
    route_type INT PRIMARY KEY,
    route_name VARCHAR(20)
);

INSERT INTO dim_route_type VALUES
(0,'Tram'),
(1,'Subway'),
(2,'Rail'),
(3,'Bus'),
(4,'Ferry');

CREATE TABLE dim_city (
    city VARCHAR(20) PRIMARY KEY,
    country VARCHAR(50)
);

INSERT INTO dim_city VALUES
('berlin','Germany'),
('paris','France');

CREATE TABLE mart_transport_hourly AS
SELECT
    c.city,
    c.country,
    r.route_name,
    a.hour_of_day,
    a.total_stop_events,
    k.congestion_rank
FROM agg_hourly_traffic a
JOIN dim_city c
    ON a.city = c.city
JOIN dim_route_type r
    ON a.route_type = r.route_type
LEFT JOIN kpi_peak_hours k
    ON a.city = k.city
    AND a.route_type = k.route_type
    AND a.hour_of_day = k.hour_of_day;