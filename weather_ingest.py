import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from pathlib import Path
import re
from pyproj import Transformer


INPUT_PATH = Path("data/processed/c2d2_flowering.parquet")
OUTPUT_PATH = Path("data/processed/weather_hourly.parquet")

API_URL = "https://archive-api.open-meteo.com/v1/archive"
GRID_RESOLUTION_M = 10_000
CACHE_FILE = ".cache"

cache_session = requests_cache.CachedSession(CACHE_FILE, expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

transformer = Transformer.from_crs(
    "EPSG:3035", "EPSG:4326", always_xy=True
)

GRID_REGEX = re.compile(r"E(\d+)N(\d+)")

def grid10km_to_latlon(grid10km: str) -> tuple[float, float]:
    match = GRID_REGEX.search(grid10km)
    if not match:
        raise ValueError(f"Invalid Grid10km value: {grid10km}")

    e_idx, n_idx = map(int, match.groups())

    x = e_idx * GRID_RESOLUTION_M + GRID_RESOLUTION_M / 2
    y = n_idx * GRID_RESOLUTION_M + GRID_RESOLUTION_M / 2

    lon, lat = transformer.transform(x, y)
    return lat, lon

def fetch_hourly_temperature(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str
) -> pd.DataFrame:

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m",
    }

    responses = openmeteo.weather_api(API_URL, params=params)
    response = responses[0]

    hourly = response.Hourly()
    temperature = hourly.Variables(0).ValuesAsNumpy()

    timestamps = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    return pd.DataFrame(
        {
            "datetime": timestamps,
            "temperature_2m": temperature,
        }
    )

def main():
    flowering_data = pd.read_parquet(INPUT_PATH)
    weather_results = []

    for row in flowering_data.itertuples(index=False):
        try:
            lat, lon = grid10km_to_latlon(row.Grid10km)
        except ValueError:
            continue

        end_date = row.FloweringDate.date().isoformat()
        start_date = (row.FloweringDate - pd.Timedelta(days=7)).date().isoformat()

        hourly_weather = fetch_hourly_temperature(
            lat=lat,
            lon=lon,
            start_date=start_date,
            end_date=end_date,
        )

        hourly_weather["RecordGroupID"] = row.RecordGroupID
        hourly_weather["Year"] = row.Year
        hourly_weather["Grid10km"] = row.Grid10km

        weather_results.append(hourly_weather)

    if not weather_results:
        raise RuntimeError("No weather data retrieved")

    final_weather_data = pd.concat(weather_results, ignore_index=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    final_weather_data.to_parquet(OUTPUT_PATH, index=False)

    print(
        f"Weather ingestion completed: {len(final_weather_data)} hourly rows written"
    )

if __name__ == "__main__":
    main()
