import pandas as pd
from pathlib import Path

WEATHER_INPUT_PATH = Path("data/processed/weather_hourly.parquet")
FLOWERING_INPUT_PATH = Path("data/processed/c2d2_flowering.parquet")
OUTPUT_PATH = Path("data/processed/temperature_report.parquet")


def main():

    weather_hourly = pd.read_parquet(WEATHER_INPUT_PATH)

    flowering_events = pd.read_parquet(
        FLOWERING_INPUT_PATH
    )[["RecordGroupID", "Year", "FloweringDate"]]

    flowering_counts = (
        flowering_events
        .assign(Month=lambda x: x["FloweringDate"].dt.month)
        .groupby(["Year", "Month"], as_index=False)
        .agg(
            flowering_record_groups=("RecordGroupID", "nunique")
        )
    )

    weather_with_flowering = weather_hourly.merge(
        flowering_events,
        on=["RecordGroupID", "Year"],
        how="left"
    )

    weather_with_flowering["Month"] = weather_with_flowering["FloweringDate"].dt.month
    weather_with_flowering["Date"] = weather_with_flowering["datetime"].dt.date

    daily_temp_stats = (
        weather_with_flowering
        .groupby(["RecordGroupID", "Year", "Month", "Date"], as_index=False)
        .agg(
            daily_mean_temperature=("temperature_2m", "mean"),
            daily_min_temperature=("temperature_2m", "min"),
            daily_max_temperature=("temperature_2m", "max"),
            hours_below_0=("temperature_2m", lambda x: (x < 0).sum())
        )
        .sort_values(["RecordGroupID", "Year", "Month", "Date"])
    )

    daily_temp_stats["day_to_day_temperature_diff"] = (
        daily_temp_stats
        .groupby(["RecordGroupID", "Year", "Month"])["daily_mean_temperature"]
        .diff()
        .abs()
    )

    monthly_summary = (
        daily_temp_stats
        .groupby(["Year", "Month"], as_index=False)
        .agg(
            avg_temperature_week=("daily_mean_temperature", "mean"),
            avg_daily_max=("daily_max_temperature", "mean"),
            avg_daily_min=("daily_min_temperature", "mean"),
            avg_day_to_day_temp_variation=("day_to_day_temperature_diff", "mean"),
            days_below_0=("daily_min_temperature", lambda x: (x < 0).sum()),
            hours_below_0=("hours_below_0", "sum")
        )
    )

    regional_temp_means = (
        weather_with_flowering
        .groupby(["Year", "Month", "Grid10km"], as_index=False)
        .agg(region_mean_temp=("temperature_2m", "mean"))
    )

    regional_variability = (
        regional_temp_means
        .groupby(["Year", "Month"], as_index=False)
        .agg(regional_temp_variability=("region_mean_temp", "std"))
    )

    final_summary = (
        monthly_summary
        .merge(regional_variability, on=["Year", "Month"], how="left")
        .merge(flowering_counts, on=["Year", "Month"], how="left")
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    final_summary.to_parquet(OUTPUT_PATH, index=False)

    print("Temperature aggregation successful")


if __name__ == "__main__":
    main()
