import json
import pandas as pd
from pathlib import Path

def run_silver_transform(**context):
    # Get the Airflow DAG execution date in YYYYMMDD format
    execution_date = context["ds_nodash"]

    # Get the bronze file path created by the bronze task from XCom
    bronze_file = context["ti"].xcom_pull(
        key="bronze_file",
        task_ids="bronze_ingest"
    )

    if not bronze_file:
        raise ValueError("Bronze file path not found in XCom")

    silver_path = Path("/opt/airflow/data/silver")

    # Create the silver folder if it does not exist
    silver_path.mkdir(parents=True, exist_ok=True)

    # Read raw JSON data from the bronze file
    with open(bronze_file) as f:
        raw = json.load(f)

    df_raw = pd.DataFrame(raw["states"])

    df_raw.columns = [
        "icao24", "callsign", "origin_country", "time_position",
        "last_contact", "longitude", "latitude", "baro_altitude",
        "on_ground", "velocity", "true_track", "vertical_rate",
        "sensors", "geo_altitude", "squawk", "spi", "position_source"
    ]

    df = df_raw[
        ["icao24", "origin_country", "velocity", "on_ground"]
    ]

    output_file = silver_path / f"flights_silver_{execution_date}.csv"

    df.to_csv(output_file, index=False)

    context["ti"].xcom_push(
        key="silver_file",
        value=str(output_file)
    )