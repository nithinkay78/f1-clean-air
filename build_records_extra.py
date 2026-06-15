"""One-off script: backfill pole positions into circuit_results.json and
fetch pit stop data (2011+) into pitstops.json.

Usage:
    python build_records_extra.py
"""
import json
import time
from pathlib import Path

import requests

BASE_URL = "https://api.jolpi.ca/ergast/f1"
DATA_DIR = Path(__file__).parent / "data"


def fetch_circuit_races(circuit_id: str) -> list[dict]:
    races = []
    offset = 0
    limit = 100
    while True:
        resp = requests.get(
            f"{BASE_URL}/circuits/{circuit_id}/results.json",
            params={"limit": limit, "offset": offset},
        )
        resp.raise_for_status()
        data = resp.json()["MRData"]
        races.extend(data["RaceTable"]["Races"])
        total = int(data["total"])
        offset += limit
        if offset >= total:
            break
        time.sleep(0.3)
    return races


def main() -> None:
    circuits = json.loads((DATA_DIR / "circuits.json").read_text())
    circuit_results = json.loads((DATA_DIR / "circuit_results.json").read_text())

    for i, circuit in enumerate(circuits):
        circuit_id = circuit["circuitId"]
        races = fetch_circuit_races(circuit_id)
        poles = []
        for race in races:
            for result in race["Results"]:
                if result.get("grid") == "1":
                    poles.append({
                        "season": race["season"],
                        "round": race["round"],
                        "raceName": race["raceName"],
                        "driverId": result["Driver"]["driverId"],
                        "driverName": f"{result['Driver']['givenName']} {result['Driver']['familyName']}",
                        "constructorName": result["Constructor"]["name"],
                    })
        circuit_results.setdefault(circuit_id, {})["poles"] = poles
        time.sleep(0.2)
        print(f"poles for {circuit_id} ({i + 1}/{len(circuits)}): {len(poles)}")

    (DATA_DIR / "circuit_results.json").write_text(json.dumps(circuit_results, indent=2))
    print("Saved circuit_results.json with poles")

    seasons = json.loads((DATA_DIR / "seasons.json").read_text())
    pitstops: dict[str, list[dict]] = {}
    for season in sorted(seasons, key=int):
        if int(season) < 2011:
            continue
        season_stops = []
        for race in seasons[season]:
            round_ = race["round"]
            try:
                resp = requests.get(
                    f"{BASE_URL}/{season}/{round_}/pitstops.json",
                    params={"limit": 100},
                    timeout=10,
                )
                resp.raise_for_status()
                races_data = resp.json()["MRData"]["RaceTable"]["Races"]
                if races_data:
                    for stop in races_data[0].get("PitStops", []):
                        season_stops.append({
                            "round": round_,
                            "raceName": race["raceName"],
                            "driverId": stop["driverId"],
                            "lap": stop["lap"],
                            "duration": stop.get("duration"),
                        })
            except requests.RequestException:
                pass
            time.sleep(0.2)
        if season_stops:
            pitstops[season] = season_stops
        print(f"pit stops for {season}: {len(season_stops)}")

    (DATA_DIR / "pitstops.json").write_text(json.dumps(pitstops, indent=2))
    print("Saved pitstops.json")


if __name__ == "__main__":
    main()
