import sqlite3
import pandas as pd
from pathlib import Path
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s"
)

def extract_from_sqlite(db_path: str, table_name: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name};", conn)
    finally:
        conn.close()

    logging.info(f"Extracted {len(df)} rows from {table_name}")
    return df


def load_to_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)


def main(input_db: str):
    table_name = "Processed_Data"
    output_path = Path("data/raw/processed_data.parquet")

    df = extract_from_sqlite(input_db, table_name)
    load_to_parquet(df, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest raw data")
    parser.add_argument(
        "--data-source",
        required=True,
        help="Path to database"
    )

    args = parser.parse_args()
    main(args.data_source)
