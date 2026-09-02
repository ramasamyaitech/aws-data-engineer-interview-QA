from airflow.decorators import dag, task
from datetime import datetime
from pathlib import Path
import pandas as pd

# Project Root
PROJECT_DIR = Path(__file__).resolve().parent.parent

# Input CSV
CSV_FILE = PROJECT_DIR / "data" / "employees.csv"

# Output Folder
OUTPUT_DIR = PROJECT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Output Parquet
PARQUET_FILE = OUTPUT_DIR / "employees.parquet"


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["csv", "parquet"],
)
def csv_to_parquet_pipeline():

    @task
    def read_csv():
        df = pd.read_csv(CSV_FILE)
        print(df)
        return df.to_json()

    @task
    def transform(data):
        df = pd.read_json(data)

        # Example transformations
        df["first_name"] = df["first_name"].str.upper()
        df["salary"] = df["salary"] * 1.10

        return df.to_json()

    @task
    def save_parquet(data):
        df = pd.read_json(data)

        df.to_parquet(
            PARQUET_FILE,
            engine="pyarrow",
            index=False
        )

        print(f"Parquet file created: {PARQUET_FILE}")

    data = read_csv()
    transformed = transform(data)
    save_parquet(transformed)


csv_to_parquet_pipeline()