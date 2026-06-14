"""One-off script: fetch all-time circuits, drivers, constructors, season
standings, and circuit results from the Ergast/Jolpica API and cache them as
JSON for the static reference pages.

Usage:
    python build_reference_data.py
"""
import json
import time
from pathlib import Path
from typing import Optional

import requests

BASE_URL = "https://api.jolpi.ca/ergast/f1"
DATA_DIR = Path(__file__).parent / "data"
FIRST_SEASON = 1950
LAST_SEASON = 2026


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


def fetch_season_standings(season: int, kind: str) -> Optional[list]:
    """kind is 'driver' or 'constructor'."""
    resp = requests.get(f"{BASE_URL}/{season}/{kind}Standings.json")
    resp.raise_for_status()
    lists = resp.json()["MRData"]["StandingsTable"]["StandingsLists"]
    if not lists:
        return None
    key = "DriverStandings" if kind == "driver" else "ConstructorStandings"
    return lists[0][key]


def fetch_season_races(season: int) -> list[dict]:
    races = []
    offset = 0
    limit = 100
    while True:
        resp = requests.get(f"{BASE_URL}/{season}.json", params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        data = resp.json()["MRData"]
        races.extend(data["RaceTable"]["Races"])
        total = int(data["total"])
        offset += limit
        if offset >= total:
            break
        time.sleep(0.3)
    return [
        {
            "round": r["round"],
            "raceName": r["raceName"],
            "date": r["date"],
            "circuitId": r["Circuit"]["circuitId"],
            "circuitName": r["Circuit"]["circuitName"],
        }
        for r in races
    ]


def fetch_circuit_results(circuit_id: str) -> list[dict]:
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
    DATA_DIR.mkdir(exist_ok=True)

    circuits = fetch_all("circuits", "CircuitTable", "Circuits")
    circuits.sort(key=lambda c: c["circuitName"])
    (DATA_DIR / "circuits.json").write_text(json.dumps(circuits, indent=2))
    print(f"Saved {len(circuits)} circuits")

    drivers = fetch_all("drivers", "DriverTable", "Drivers")
    drivers.sort(key=lambda d: (d["familyName"], d["givenName"]))
    (DATA_DIR / "drivers.json").write_text(json.dumps(drivers, indent=2))
    print(f"Saved {len(drivers)} drivers")

    constructors = fetch_all("constructors", "ConstructorTable", "Constructors")
    constructors.sort(key=lambda c: c["name"])
    (DATA_DIR / "constructors.json").write_text(json.dumps(constructors, indent=2))
    print(f"Saved {len(constructors)} constructors")

    driver_standings: dict[str, list[dict]] = {}
    constructor_standings: dict[str, list[dict]] = {}
    for season in range(FIRST_SEASON, LAST_SEASON + 1):
        ds = fetch_season_standings(season, "driver")
        if ds:
            driver_standings[str(season)] = ds
        time.sleep(0.2)
        cs = fetch_season_standings(season, "constructor")
        if cs:
            constructor_standings[str(season)] = cs
        time.sleep(0.2)
        print(f"Fetched standings for {season}")
    (DATA_DIR / "driver_standings.json").write_text(json.dumps(driver_standings, indent=2))
    (DATA_DIR / "constructor_standings.json").write_text(json.dumps(constructor_standings, indent=2))
    print(f"Saved standings for {len(driver_standings)} seasons")

    seasons: dict[str, list[dict]] = {}
    for season in range(FIRST_SEASON, LAST_SEASON + 1):
        races = fetch_season_races(season)
        if races:
            seasons[str(season)] = races
        time.sleep(0.2)
        print(f"Fetched race calendar for {season}")
    (DATA_DIR / "seasons.json").write_text(json.dumps(seasons, indent=2))
    print(f"Saved race calendars for {len(seasons)} seasons")

    circuit_results: dict[str, dict] = {}
    for i, circuit in enumerate(circuits):
        circuit_id = circuit["circuitId"]
        races = fetch_circuit_results(circuit_id)
        winners = []
        lap_record = None
        for race in races:
            for result in race["Results"]:
                if result["position"] == "1":
                    winners.append({
                        "season": race["season"],
                        "round": race["round"],
                        "raceName": race["raceName"],
                        "driverId": result["Driver"]["driverId"],
                        "driverName": f"{result['Driver']['givenName']} {result['Driver']['familyName']}",
                        "constructorName": result["Constructor"]["name"],
                    })
                fastest = result.get("FastestLap")
                if fastest and fastest.get("rank") == "1":
                    millis = fastest["Time"].get("time")
                    if lap_record is None or _lap_time_key(fastest["Time"]["time"]) < _lap_time_key(lap_record["time"]):
                        lap_record = {
                            "time": millis,
                            "season": race["season"],
                            "driverId": result["Driver"]["driverId"],
                            "driverName": f"{result['Driver']['givenName']} {result['Driver']['familyName']}",
                        }
        circuit_results[circuit_id] = {"winners": winners, "lap_record": lap_record}
        time.sleep(0.2)
        print(f"Fetched results for {circuit_id} ({i + 1}/{len(circuits)})")
    (DATA_DIR / "circuit_results.json").write_text(json.dumps(circuit_results, indent=2))
    print(f"Saved circuit results for {len(circuit_results)} circuits")


def _lap_time_key(time_str: str) -> float:
    if ":" not in time_str:
        return float(time_str)
    minutes, rest = time_str.split(":")
    return int(minutes) * 60 + float(rest)


if __name__ == "__main__":
    main()
