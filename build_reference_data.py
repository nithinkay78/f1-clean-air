"""One-off script: fetch all-time circuits and drivers from the Ergast/Jolpica
API and cache them as JSON for the static reference pages.

Usage:
    python build_reference_data.py
"""
import json
import time
from pathlib import Path

import requests

BASE_URL = "https://api.jolpi.ca/ergast/f1"
DATA_DIR = Path(__file__).parent / "data"


def fetch_all(endpoint: str, table_key: str, item_key: str) -> list[dict]:
    items = []
    offset = 0
    limit = 100
    while True:
        resp = requests.get(f"{BASE_URL}/{endpoint}.json", params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        data = resp.json()["MRData"]
        batch = data[table_key][item_key]
        items.extend(batch)
        total = int(data["total"])
        offset += limit
        if offset >= total:
            break
        time.sleep(0.3)
    return items


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)

    circuits = fetch_all("circuits", "CircuitTable", "Circuits")
    circuits.sort(key=lambda c: c["circuitName"])
    (DATA_DIR / "circuits.json").write_text(json.dumps(circuits, indent=2))
    print(f"Saved {len(circuits)} circuits")

    drivers = fetch_all("drivers", "DriverTable", "Drivers")
    drivers.sort(key=lambda d: (d["familyName"], d["givenName"]))
    (DATA_DIR / "drivers.json").write_text(json.dumps(drivers, indent=2))
    print(f"Saved {len(drivers)} drivers")


if __name__ == "__main__":
    main()
