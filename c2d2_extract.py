import pandas as pd
from pathlib import Path

INPUT_PATH = Path("data/raw/c2d2/processed_data.parquet")
OUTPUT_PATH = Path("data/processed/c2d2_flowering.parquet")

APPLE_EPPO_CODES = {"MABSD", "MABSS", "MABPM"}

def load_raw_data(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def filter_apple_flowering(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        (
            df["EPPOCropCodeRaw"].isin(APPLE_EPPO_CODES)
            | (df["CropNameHarmonisedFinal"] == "Apple")
        )
        & df["GSFinal"].notna()
        & df["GSFinal"].astype(str).str.startswith("6")
    ].copy()

    df["GSDateFinal"] = pd.to_datetime(df["GSDateFinal"], errors="coerce")
    df = df[df["GSDateFinal"].notna()]

    print(f"Rows after filtering: {len(df)}")
    return df


def select_first_bloom_per_experiment_year(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Year"] = df["GSDateFinal"].dt.year

    df = df.sort_values(by=["RecordGroupID", "Year", "GSDateFinal"])
    first_bloom = df.groupby(["RecordGroupID", "Year"], as_index=False).first()

    return first_bloom


def select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        [
            "RecordGroupID",
            "Year",
            "GSDateFinal",
            "Grid10km",
            "EPPOCropCodeRaw",
            "CropNameHarmonisedFinal",
        ]
    ].copy()

    df.rename(columns={"GSDateFinal": "FloweringDate"}, inplace=True)
    df = df[df["Grid10km"].notna()]

    return df


def validate_output(df: pd.DataFrame) -> None:
    assert not df.empty, "Output dataset is empty"
    assert df["RecordGroupID"].notna().all(), "Missing RecordGroupID"
    assert df["Year"].notna().all(), "Missing Year"
    assert df["FloweringDate"].notna().all(), "Missing FloweringDate"
    assert df["Grid10km"].notna().all(), "Missing Grid10km"


def write_output(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def main():
    df_raw = load_raw_data(INPUT_PATH)
    df_filtered = filter_apple_flowering(df_raw)
    df_first_bloom = select_first_bloom_per_experiment_year(df_filtered)
    df_output = select_output_columns(df_first_bloom)

    validate_output(df_output)
    write_output(df_output, OUTPUT_PATH)


if __name__ == "__main__":
    main()
