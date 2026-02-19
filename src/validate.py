import pandas as pd
from .db import engine

def run_validation():
    checks = [
        ("Missing stop coordinates",
         "SELECT COUNT(*) FROM berlin_stops WHERE stop_lat IS NULL OR stop_lon IS NULL"),

        ("Trips without routes",
         "SELECT COUNT(*) FROM berlin_trips t LEFT JOIN berlin_routes r ON t.route_id=r.route_id WHERE r.route_id IS NULL"),
    ]

    for name, query in checks:
        result = pd.read_sql(query, engine)
        print(name, result.iloc[0,0])
