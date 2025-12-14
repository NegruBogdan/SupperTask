import pandas as pd
from pathlib import Path

WEATHER_INPUT_PATH = Path("data/processed/weather_hourly.parquet")
FLOWERING_INPUT_PATH = Path("data/processed/c2d2_flowering.parquet")
OUTPUT_PATH = Path("data/processed/temperature_by_month_year.parquet")

def main():
    weather_data = pd.read_parquet(WEATHER_INPUT_PATH)
    flowering_records = pd.read_parquet(FLOWERING_INPUT_PATH)[["RecordGroupID", "Year", "FloweringDate"]]

    aggregate_data = weather_data.merge(flowering_records, on=["RecordGroupID", "Year"], how="left")
    aggregate_data["Month"] = aggregate_data["FloweringDate"].dt.month

    monthly_split = aggregate_data.groupby(["Year", "Month"], as_index=False).agg(avg_temperature_week=("temperature_2m", "mean"))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    monthly_split.to_parquet(OUTPUT_PATH, index=False)

    print(f"Temperature aggregation successful")

if __name__ == "__main__":
    main()
