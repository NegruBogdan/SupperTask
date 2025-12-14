import subprocess
from pathlib import Path
import argparse
import sys

SCRIPTS = [
    Path("c2d2_ingest.py"),
    Path("c2d2_extract.py"),
    Path("weather_ingest.py"),
    Path("weather_extract.py")
]

def main(c2d2_db_path: str):
    print("Starting full Apple flowering temperature pipeline")

    for script in SCRIPTS:
        if script.name == "c2d2_ingest.py":
            command = [
                sys.executable,
                str(script),
                "--data-source",
                c2d2_db_path
            ]
        else:
            command = [sys.executable, str(script)]

        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"Script {script} failed!\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")

    print("Pipeline finished successfully")

# --------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run full project pipeline")
    parser.add_argument(
        "--data-source",
        required=True,
        help="Path to database"
    )

    args = parser.parse_args()
    main(args.data_source)
