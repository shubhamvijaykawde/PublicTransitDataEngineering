import pandas as pd
from .db import engine

def transform_stop_times(city, batch_size=200000):
    offset = 0
    while True:
        query = f"""
        SELECT trip_id, stop_id, stop_sequence,
               TIME_TO_SEC(arrival_time) AS arrival_sec,
               TIME_TO_SEC(departure_time) AS departure_sec
        FROM {city}_stop_times
        LIMIT {batch_size} OFFSET {offset}
        """
        df = pd.read_sql(query, engine)
        if df.empty:
            break

        df.to_sql(
            f"{city}_stop_times_clean",
            engine,
            if_exists="append",
            index=False,
            chunksize=50000
        )
        offset += batch_size

def build_fact(city, batch_size=200000):
    offset = 0
    while True:
        query = f"""
        SELECT
            '{city}' AS city,
            ts.route_type,
            st.trip_id,
            st.stop_id,
            st.arrival_sec,
            st.departure_sec
        FROM {city}_stop_times_clean st
        JOIN {city}_trip_service ts ON st.trip_id = ts.trip_id
        LIMIT {batch_size} OFFSET {offset}
        """
        df = pd.read_sql(query, engine)
        if df.empty:
            break

        df.to_sql(
            "fact_stop_events",
            engine,
            if_exists="append",
            index=False,
            chunksize=50000
        )
        offset += batch_size

def run_transforms():
    for city in ["berlin","paris"]:
        transform_stop_times(city)
        build_fact(city)
