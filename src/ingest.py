import zipfile
import pandas as pd
from pathlib import Path
from .db import engine

BASE_PATH = Path("data")

CITIES = {
    "berlin": BASE_PATH / "berlin" / "week.gtfs.zip",
    "paris": BASE_PATH / "paris" / "week.gtfs.zip"
}

GTFS_FILES = {
    "stops.txt": "stops",
    "routes.txt": "routes",
    "trips.txt": "trips",
    "stop_times.txt": "stop_times",
    "calendar.txt": "calendar"
}

COLUMN_MAP = {
    "stops": ["stop_id","stop_name","stop_lat","stop_lon"],
    "routes": ["route_id","agency_id","route_short_name","route_long_name","route_type"],
    "trips": ["trip_id","route_id","service_id","trip_headsign"],
    "stop_times": ["trip_id","arrival_time","departure_time","stop_id","stop_sequence"],
    "calendar": [
        "service_id","monday","tuesday","wednesday",
        "thursday","friday","saturday","sunday",
        "start_date","end_date"
    ]
}

def load_gtfs_file(zip_path, filename):
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(filename) as f:
            return pd.read_csv(f)

def run_ingestion():
    for city, zip_path in CITIES.items():
        print(f"Ingesting {city}")

        for file_name, table_suffix in GTFS_FILES.items():
            df = load_gtfs_file(zip_path, file_name)
            df = df[COLUMN_MAP[table_suffix]]

            table_name = f"{city}_{table_suffix}"

            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=50000
            )
