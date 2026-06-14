"""F1 Clean Air — live timing dashboard, single-process deployable app.

Runs the F1 public live timing collector in a background thread and serves
the dashboard + JSON snapshot over HTTP.

Usage:
    python app.py            # serves on $PORT (default 8000)
"""
import ast
import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests
from fastf1.livetiming.client import SignalRClient
from signalrcore.hub_connection_builder import HubConnectionBuilder

BASE_DIR = Path(__file__).parent
DATA_FILE = BASE_DIR / "live_data.txt"
SUBSCRIBERS_FILE = BASE_DIR / "data" / "subscribers.txt"
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

CIRCUITS = json.loads((BASE_DIR / "data" / "circuits.json").read_text())
DRIVERS = json.loads((BASE_DIR / "data" / "drivers.json").read_text())
CONSTRUCTORS = json.loads((BASE_DIR / "data" / "constructors.json").read_text())
DRIVER_STANDINGS = json.loads((BASE_DIR / "data" / "driver_standings.json").read_text())
CONSTRUCTOR_STANDINGS = json.loads((BASE_DIR / "data" / "constructor_standings.json").read_text())
CIRCUIT_RESULTS = json.loads((BASE_DIR / "data" / "circuit_results.json").read_text())
CIRCUITS_BY_ID = {c["circuitId"]: c for c in CIRCUITS}
DRIVERS_BY_ID = {d["driverId"]: d for d in DRIVERS}
CONSTRUCTORS_BY_ID = {c["constructorId"]: c for c in CONSTRUCTORS}


def _driver_career_stats(driver_id: str) -> dict:
    seasons = []
    wins = 0
    points = 0.0
    championships = 0
    teams = []
    for season in sorted(DRIVER_STANDINGS, key=int):
        for entry in DRIVER_STANDINGS[season]:
            if entry["Driver"]["driverId"] != driver_id:
                continue
            team_names = [c["name"] for c in entry["Constructors"]]
            seasons.append({
                "season": season,
                "position": entry["position"],
                "points": entry["points"],
                "wins": entry["wins"],
                "teams": team_names,
            })
            wins += int(entry["wins"])
            points += float(entry["points"])
            if entry["position"] == "1":
                championships += 1
            for name in team_names:
                if name not in teams:
                    teams.append(name)
    return {
        "seasons": seasons,
        "wins": wins,
        "points": points,
        "championships": championships,
        "teams": teams,
    }


def _constructor_career_stats(constructor_id: str) -> dict:
    seasons = []
    wins = 0
    points = 0.0
    championships = 0
    for season in sorted(CONSTRUCTOR_STANDINGS, key=int):
        for entry in CONSTRUCTOR_STANDINGS[season]:
            if entry["Constructor"]["constructorId"] != constructor_id:
                continue
            seasons.append({
                "season": season,
                "position": entry["position"],
                "points": entry["points"],
                "wins": entry["wins"],
            })
            wins += int(entry["wins"])
            points += float(entry["points"])
            if entry["position"] == "1":
                championships += 1
    return {
        "seasons": seasons,
        "wins": wins,
        "points": points,
        "championships": championships,
    }

PAGES = {
    "/": "index.html",
    "/index.html": "index.html",
    "/live": "live.html",
    "/live.html": "live.html",
    "/styles.css": "styles.css",
    "/theme.js": "theme.js",
}
CONTENT_TYPES = {
    ".html": "text/html",
    ".css": "text/css",
    ".js": "application/javascript",
}

_lock = threading.Lock()
_drivers: dict[str, dict] = {}
_session_info: dict = {}
_track_status: dict = {}
_file_pos = 0
_prev_positions: dict[str, int] = {}
_position_deltas: dict[str, int] = {}
_interval_samples: dict[str, list[float]] = {}
_stint_lap_times: dict[str, list[float]] = {}
_last_seen_lap: dict[str, int] = {}
_last_seen_compound: dict[str, str] = {}
_subscribers_lock = threading.Lock()

_standings_lock = threading.Lock()
_standings_cache: list[dict] = []
STANDINGS_URL = "https://api.jolpi.ca/ergast/f1/current/driverStandings.json"

_schedule_lock = threading.Lock()
_schedule_cache: list[dict] = []
SCHEDULE_URL = "https://api.jolpi.ca/ergast/f1/current.json"

COUNTRY_TO_ISO = {
    "Australia": "au", "China": "cn", "Japan": "jp", "USA": "us", "Canada": "ca",
    "Monaco": "mc", "Spain": "es", "Austria": "at", "UK": "gb", "Belgium": "be",
    "Hungary": "hu", "Netherlands": "nl", "Italy": "it", "Azerbaijan": "az",
    "Singapore": "sg", "Mexico": "mx", "Brazil": "br", "Qatar": "qa", "UAE": "ae",
    "Bahrain": "bh", "Saudi Arabia": "sa", "Germany": "de", "France": "fr",
    "Portugal": "pt", "Russia": "ru", "Turkey": "tr", "India": "in", "South Korea": "kr",
}


# --- Collector (F1 public live timing feed, no auth) -----------------------


class NoAuthSignalRClient(SignalRClient):
    """Workaround: fastf1 passes access_token_factory=None when no_auth=True,
    but signalrcore requires it to be callable or absent. Use a no-op token."""

    def _run(self):
        self._output_file = open(self.filename, self.filemode)

        r = requests.options(self._negotiate_url, headers=self.headers)
        self.headers.update({"Cookie": f"AWSALBCORS={r.cookies['AWSALBCORS']}"})

        options = {
            "verify_ssl": True,
            "access_token_factory": lambda: "",
            "headers": self.headers,
        }

        self._connection = (
            HubConnectionBuilder()
            .with_url(self._connection_url, options=options)
            .configure_logging(logging.INFO)
            .build()
        )

        self._connection.on_open(self._on_connect)
        self._connection.on_close(self._on_close)
        self._connection.on("feed", self._on_message)

        self._connection.start()

        while not self._is_connected:
            time.sleep(0.1)

        self._connection.send("Subscribe", [self.topics], on_invocation=self._on_message)


def run_collector_forever() -> None:
    """Run the collector, reconnecting if the connection drops."""
    while True:
        try:
            client = NoAuthSignalRClient(filename=str(DATA_FILE), filemode="w", no_auth=True)
            client.start()
        except Exception:
            logging.exception("live timing collector crashed, retrying in 10s")
        time.sleep(10)


def fetch_standings() -> None:
    try:
        resp = requests.get(STANDINGS_URL, timeout=10)
        resp.raise_for_status()
        lists = resp.json()["MRData"]["StandingsTable"]["StandingsLists"]
        rows = lists[0]["DriverStandings"] if lists else []
        standings = [
            {
                "position": row.get("position"),
                "points": row.get("points"),
                "wins": row.get("wins"),
                "driver_code": row["Driver"].get("code"),
                "driver_name": f"{row['Driver'].get('givenName', '')} {row['Driver'].get('familyName', '')}".strip(),
                "team": row["Constructors"][0]["name"] if row.get("Constructors") else None,
            }
            for row in rows
        ]
        with _standings_lock:
            _standings_cache[:] = standings
    except Exception:
        logging.exception("failed to fetch driver standings")


def fetch_schedule() -> None:
    try:
        resp = requests.get(SCHEDULE_URL, timeout=10)
        resp.raise_for_status()
        races = resp.json()["MRData"]["RaceTable"]["Races"]
        schedule = [
            {
                "round": race.get("round"),
                "race_name": race.get("raceName"),
                "date": race.get("date"),
                "circuit_id": race["Circuit"]["circuitId"],
                "country": race["Circuit"]["Location"]["country"],
            }
            for race in races
        ]
        with _schedule_lock:
            _schedule_cache[:] = schedule
    except Exception:
        logging.exception("failed to fetch race schedule")


def run_standings_refresher() -> None:
    while True:
        fetch_standings()
        fetch_schedule()
        time.sleep(1800)


# --- Snapshot building -------------------------------------------------------


def _merge(target: dict, updates: dict) -> None:
    for key, value in updates.items():
        existing = target.get(key)
        if isinstance(value, dict) and isinstance(existing, dict):
            _merge(existing, value)
        elif isinstance(value, dict) and isinstance(existing, list) and all(k.isdigit() for k in value):
            for index_str, item in value.items():
                index = int(index_str)
                while len(existing) <= index:
                    existing.append({})
                if isinstance(item, dict) and isinstance(existing[index], dict):
                    _merge(existing[index], item)
                else:
                    existing[index] = item
        else:
            target[key] = value


def _apply_message(topic: str, data) -> None:
    if topic == "DriverList":
        for num, info in data.items():
            if num == "_kf":
                continue
            entry = _drivers.setdefault(num, {})
            entry.setdefault("info", {})
            _merge(entry["info"], info)
    elif topic == "TimingData":
        for num, info in data.get("Lines", {}).items():
            entry = _drivers.setdefault(num, {})
            entry.setdefault("timing", {})
            _merge(entry["timing"], info)
    elif topic == "TimingAppData":
        for num, info in data.get("Lines", {}).items():
            entry = _drivers.setdefault(num, {})
            entry.setdefault("app", {})
            _merge(entry["app"], info)
    elif topic == "SessionInfo":
        _merge(_session_info, data)
    elif topic == "TrackStatus":
        _merge(_track_status, data)


def _read_new_lines() -> None:
    global _file_pos
    if not DATA_FILE.exists():
        return
    with open(DATA_FILE, "r") as f:
        file_size = f.seek(0, 2)
        if _file_pos > file_size:
            # collector reconnected and truncated the file (filemode="w")
            _file_pos = 0
        f.seek(_file_pos)
        lines = f.readlines()
        _file_pos = f.tell()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            entry = ast.literal_eval(line)
        except (ValueError, SyntaxError):
            continue
        if not isinstance(entry, list) or len(entry) < 2:
            continue
        topic, raw_data = entry[0], entry[1]
        try:
            data = json.loads(raw_data)
        except (TypeError, json.JSONDecodeError):
            continue
        _apply_message(topic, data)


def _parse_float(value) -> float | None:
    if not value:
        return None
    try:
        return float(value.lstrip("+"))
    except ValueError:
        return None


def _parse_lap_seconds(value) -> float | None:
    if not value:
        return None
    try:
        if ":" in value:
            minutes, seconds = value.split(":")
            return float(minutes) * 60 + float(seconds)
        return float(value)
    except ValueError:
        return None


def build_snapshot() -> dict:
    with _lock:
        _read_new_lines()

        rows = []
        for num, entry in _drivers.items():
            info = entry.get("info", {})
            timing = entry.get("timing", {})
            app = entry.get("app", {})

            stints = app.get("Stints", {})
            current_stint = None
            if isinstance(stints, dict) and stints:
                last_key = sorted(stints.keys(), key=lambda k: int(k))[-1]
                current_stint = stints[last_key]
            elif isinstance(stints, list) and stints:
                current_stint = stints[-1]

            try:
                position = int(timing.get("Position"))
            except (TypeError, ValueError):
                position = None

            if position is not None:
                prev = _prev_positions.get(num)
                if prev is not None and prev != position:
                    _position_deltas[num] = prev - position
                _prev_positions[num] = position

            # Gap trend: closing / opening / stable, based on recent interval samples.
            interval_val = _parse_float((timing.get("IntervalToPositionAhead") or {}).get("Value"))
            if interval_val is not None:
                samples = _interval_samples.setdefault(num, [])
                if not samples or samples[-1] != interval_val:
                    samples.append(interval_val)
                    del samples[:-5]

            gap_trend = None
            samples = _interval_samples.get(num, [])
            if len(samples) >= 3:
                delta = samples[-1] - samples[0]
                if delta < -0.05:
                    gap_trend = "closing"
                elif delta > 0.05:
                    gap_trend = "opening"
                else:
                    gap_trend = "stable"

            # Tyre degradation: current lap time vs. average of the rest of the stint.
            compound = current_stint.get("Compound") if current_stint else None
            if compound != _last_seen_compound.get(num):
                _stint_lap_times[num] = []
                _last_seen_compound[num] = compound

            laps_val = timing.get("NumberOfLaps")
            last_lap_val = _parse_lap_seconds((timing.get("LastLapTime") or {}).get("Value"))
            if last_lap_val is not None and laps_val != _last_seen_lap.get(num):
                stint_times = _stint_lap_times.setdefault(num, [])
                stint_times.append(last_lap_val)
                del stint_times[:-10]
                _last_seen_lap[num] = laps_val

            tyre_degradation = None
            stint_times = _stint_lap_times.get(num, [])
            if len(stint_times) >= 4:
                baseline = sum(stint_times[:-1]) / len(stint_times[:-1])
                tyre_degradation = "high" if stint_times[-1] > baseline * 1.03 else "normal"

            rows.append(
                {
                    "racing_number": num,
                    "tla": info.get("Tla"),
                    "full_name": info.get("FullName"),
                    "team": info.get("TeamName"),
                    "team_colour": info.get("TeamColour"),
                    "position": timing.get("Position"),
                    "gap_to_leader": timing.get("GapToLeader"),
                    "interval": (timing.get("IntervalToPositionAhead") or {}).get("Value"),
                    "last_lap": (timing.get("LastLapTime") or {}).get("Value"),
                    "best_lap": (timing.get("BestLapTime") or {}).get("Value"),
                    "laps": timing.get("NumberOfLaps"),
                    "in_pit": timing.get("InPit"),
                    "retired": timing.get("Retired"),
                    "compound": current_stint.get("Compound") if current_stint else None,
                    "tyre_age": current_stint.get("TotalLaps") if current_stint else None,
                    "position_change": _position_deltas.get(num, 0),
                    "gap_trend": gap_trend,
                    "tyre_degradation": tyre_degradation,
                }
            )

        def sort_key(row):
            try:
                return int(row["position"])
            except (TypeError, ValueError):
                return 999

        rows.sort(key=sort_key)

        return {
            "session": _session_info,
            "track_status": _track_status,
            "drivers": rows,
        }


def build_teams() -> list[dict]:
    with _lock:
        teams: dict[str, dict] = {}
        for num, entry in _drivers.items():
            info = entry.get("info", {})
            team_name = info.get("TeamName")
            if not team_name:
                continue
            team = teams.setdefault(
                team_name,
                {"team_name": team_name, "team_colour": info.get("TeamColour"), "drivers": []},
            )
            team["drivers"].append(
                {
                    "racing_number": num,
                    "tla": info.get("Tla"),
                    "full_name": info.get("FullName"),
                    "headshot_url": info.get("HeadshotUrl"),
                }
            )

        result = sorted(teams.values(), key=lambda t: t["team_name"])
        for team in result:
            team["drivers"].sort(key=lambda d: d["tla"] or "")
        return result


# --- Reference pages (circuits / drivers) -------------------------------------


import html as _html


def _page_shell(title: str, active: str, body: str) -> str:
    nav = [("/", "Home"), ("/live", "Live"), ("/circuits", "Circuits"), ("/drivers", "Drivers"), ("/constructors", "Constructors")]
    links = "".join(
        f'<a href="{href}" class="{"active" if href == active else ""}">{label}</a>'
        for href, label in nav
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>{_html.escape(title)}</title>
<link rel="stylesheet" href="/styles.css" />
</head>
<body>
  <nav>
    <div class="brand">F1 <span>Clean</span> Air</div>
    <div class="links">{links}</div>
  </nav>
  {body}
  <footer>
    F1 Clean Air &middot; Real analytics are only visible in clean air.
    <div class="social-links">
      <a href="#" target="_blank" rel="noopener">Telegram</a>
      <a href="#" target="_blank" rel="noopener">Discord</a>
      <a href="#" target="_blank" rel="noopener">Newsletter</a>
    </div>
  </footer>
  <script src="/theme.js"></script>
</body>
</html>"""


def render_circuits_list() -> str:
    with _schedule_lock:
        schedule = list(_schedule_cache)

    season_cards = "".join(
        f"""<a class="ref-card" href="/circuits/{_html.escape(race['circuit_id'])}">
          <img class="ref-flag" src="https://flagcdn.com/w80/{COUNTRY_TO_ISO.get(race['country'], 'xx')}.png" alt="{_html.escape(race['country'])}" />
          <h3>{_html.escape(race['race_name'])}</h3>
          <p>Round {_html.escape(str(race['round']))} &middot; {_html.escape(race['date'])}</p>
        </a>"""
        for race in schedule
        if race["circuit_id"] in CIRCUITS_BY_ID
    )
    season_section = (
        f"""<h1>2026 Season</h1>
      <div class="ref-grid">{season_cards}</div>
      <h1 style="margin-top: 40px;">All Circuits</h1>"""
        if season_cards
        else "<h1>Circuits</h1>"
    )

    cards = "".join(
        f"""<a class="ref-card" href="/circuits/{_html.escape(c['circuitId'])}">
          <h3>{_html.escape(c['circuitName'])}</h3>
          <p>{_html.escape(c['Location']['locality'])}, {_html.escape(c['Location']['country'])}</p>
        </a>"""
        for c in CIRCUITS
    )
    body = f"""<div class="ref-wrap">
      {season_section}
      <div class="ref-grid">{cards}</div>
    </div>"""
    return _page_shell("F1 Clean Air — Circuits", "/circuits", body)


def render_circuit_detail(circuit: dict) -> str:
    loc = circuit["Location"]
    results = CIRCUIT_RESULTS.get(circuit["circuitId"], {})
    lap_record = results.get("lap_record")
    lap_record_row = (
        f"""<tr><th>Lap record</th><td class="mono">{_html.escape(lap_record['time'])} &mdash; {_html.escape(lap_record['driverName'])} ({_html.escape(lap_record['season'])})</td></tr>"""
        if lap_record
        else ""
    )

    winners = results.get("winners", [])
    winner_rows = "".join(
        f"""<tr onclick="location.href='/drivers/{_html.escape(w['driverId'])}'">
          <td class="mono">{_html.escape(w['season'])}</td>
          <td>{_html.escape(w['raceName'])}</td>
          <td>{_html.escape(w['driverName'])}</td>
          <td>{_html.escape(w['constructorName'])}</td>
        </tr>"""
        for w in reversed(winners)
    )
    winners_section = (
        f"""<h1 style="margin-top: 40px;">Race winners</h1>
      <table class="ref-table ref-table-list">
        <thead><tr><th>Season</th><th>Race</th><th>Winner</th><th>Constructor</th></tr></thead>
        <tbody>{winner_rows}</tbody>
      </table>"""
        if winners
        else ""
    )

    body = f"""<div class="ref-wrap">
      <a class="back" href="/circuits">&larr; All circuits</a>
      <h1>{_html.escape(circuit['circuitName'])}</h1>
      <table class="ref-table">
        <tr><th>Locality</th><td>{_html.escape(loc['locality'])}</td></tr>
        <tr><th>Country</th><td>{_html.escape(loc['country'])}</td></tr>
        <tr><th>Coordinates</th><td class="mono">{_html.escape(loc['lat'])}, {_html.escape(loc['long'])}</td></tr>
        {lap_record_row}
        <tr><th>More info</th><td><a href="{_html.escape(circuit['url'])}" target="_blank" rel="noopener">Wikipedia</a></td></tr>
      </table>
      {winners_section}
    </div>"""
    return _page_shell(f"F1 Clean Air — {circuit['circuitName']}", "/circuits", body)


def render_drivers_list() -> str:
    rows = "".join(
        f"""<tr onclick="location.href='/drivers/{_html.escape(d['driverId'])}'">
          <td>{_html.escape(d['givenName'])} {_html.escape(d['familyName'])}</td>
          <td>{_html.escape(d.get('nationality', '—'))}</td>
          <td class="mono">{_html.escape(d.get('dateOfBirth', '—'))}</td>
        </tr>"""
        for d in DRIVERS
    )
    body = f"""<div class="ref-wrap">
      <h1>Current Grid</h1>
      <div class="ref-grid" id="current-grid"></div>
      <h1 style="margin-top: 40px;">All Drivers</h1>
      <table class="ref-table ref-table-list">
        <thead><tr><th>Name</th><th>Nationality</th><th>Date of Birth</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <script>
      fetch('/api/teams').then(r => r.json()).then(teams => {{
        const grid = document.getElementById('current-grid');
        if (!teams.length) {{ grid.innerHTML = '<p style="color: var(--silver);">Live grid will appear during a session.</p>'; return; }}
        grid.innerHTML = teams.flatMap(t => t.drivers.map(d => `
          <div class="ref-card driver-card">
            <span class="team-bar" style="background:#${{t.team_colour || '444'}}"></span>
            ${{d.headshot_url ? `<img class="driver-photo" src="${{d.headshot_url}}" alt="${{d.full_name || d.tla}}" />` : ''}}
            <h3>${{d.full_name || d.tla}}</h3>
            <p>${{t.team_name}}</p>
          </div>
        `)).join('');
      }}).catch(() => {{}});
    </script>"""
    return _page_shell("F1 Clean Air — Drivers", "/drivers", body)


def render_driver_detail(driver: dict) -> str:
    url = driver.get("url")
    more_info = (
        f'<tr><th>More info</th><td><a href="{_html.escape(url)}" target="_blank" rel="noopener">Wikipedia</a></td></tr>'
        if url
        else ""
    )

    career = _driver_career_stats(driver["driverId"])
    career_section = ""
    if career["seasons"]:
        championship_row = (
            f"<tr><th>World Championships</th><td>{career['championships']}</td></tr>"
            if career["championships"]
            else ""
        )
        career_section = f"""<h1 style="margin-top: 40px;">Career</h1>
      <table class="ref-table">
        <tr><th>Career wins</th><td>{career['wins']}</td></tr>
        <tr><th>Career points</th><td>{career['points']:g}</td></tr>
        {championship_row}
        <tr><th>Teams</th><td>{_html.escape(', '.join(career['teams']))}</td></tr>
      </table>
      <h1 style="margin-top: 40px;">Season by season</h1>
      <table class="ref-table ref-table-list">
        <thead><tr><th>Season</th><th>Position</th><th>Points</th><th>Wins</th><th>Team(s)</th></tr></thead>
        <tbody>{"".join(
            f'''<tr>
              <td class="mono">{_html.escape(s['season'])}</td>
              <td>{_html.escape(s['position'])}</td>
              <td>{_html.escape(s['points'])}</td>
              <td>{_html.escape(s['wins'])}</td>
              <td>{_html.escape(', '.join(s['teams']))}</td>
            </tr>'''
            for s in reversed(career['seasons'])
        )}</tbody>
      </table>"""

    body = f"""<div class="ref-wrap">
      <a class="back" href="/drivers">&larr; All drivers</a>
      <h1>{_html.escape(driver['givenName'])} {_html.escape(driver['familyName'])}</h1>
      <table class="ref-table">
        <tr><th>Nationality</th><td>{_html.escape(driver.get('nationality', '—'))}</td></tr>
        <tr><th>Date of Birth</th><td class="mono">{_html.escape(driver.get('dateOfBirth', '—'))}</td></tr>
        {more_info}
      </table>
      {career_section}
    </div>"""
    return _page_shell(f"F1 Clean Air — {driver['givenName']} {driver['familyName']}", "/drivers", body)


def render_constructors_list() -> str:
    cards = "".join(
        f"""<a class="ref-card" href="/constructors/{_html.escape(c['constructorId'])}">
          <h3>{_html.escape(c['name'])}</h3>
          <p>{_html.escape(c.get('nationality', '—'))}</p>
        </a>"""
        for c in CONSTRUCTORS
    )
    body = f"""<div class="ref-wrap">
      <h1>Constructors</h1>
      <div class="ref-grid">{cards}</div>
    </div>"""
    return _page_shell("F1 Clean Air — Constructors", "/constructors", body)


def render_constructor_detail(constructor: dict) -> str:
    url = constructor.get("url")
    more_info = (
        f'<tr><th>More info</th><td><a href="{_html.escape(url)}" target="_blank" rel="noopener">Wikipedia</a></td></tr>'
        if url
        else ""
    )

    career = _constructor_career_stats(constructor["constructorId"])
    career_section = ""
    if career["seasons"]:
        championship_row = (
            f"<tr><th>Constructors' Championships</th><td>{career['championships']}</td></tr>"
            if career["championships"]
            else ""
        )
        career_section = f"""<h1 style="margin-top: 40px;">Career</h1>
      <table class="ref-table">
        <tr><th>Career wins</th><td>{career['wins']}</td></tr>
        <tr><th>Career points</th><td>{career['points']:g}</td></tr>
        {championship_row}
      </table>
      <h1 style="margin-top: 40px;">Season by season</h1>
      <table class="ref-table ref-table-list">
        <thead><tr><th>Season</th><th>Position</th><th>Points</th><th>Wins</th></tr></thead>
        <tbody>{"".join(
            f'''<tr>
              <td class="mono">{_html.escape(s['season'])}</td>
              <td>{_html.escape(s['position'])}</td>
              <td>{_html.escape(s['points'])}</td>
              <td>{_html.escape(s['wins'])}</td>
            </tr>'''
            for s in reversed(career['seasons'])
        )}</tbody>
      </table>"""

    body = f"""<div class="ref-wrap">
      <a class="back" href="/constructors">&larr; All constructors</a>
      <h1>{_html.escape(constructor['name'])}</h1>
      <table class="ref-table">
        <tr><th>Nationality</th><td>{_html.escape(constructor.get('nationality', '—'))}</td></tr>
        {more_info}
      </table>
      {career_section}
    </div>"""
    return _page_shell(f"F1 Clean Air — {constructor['name']}", "/constructors", body)


# --- HTTP server --------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        if self.path.startswith("/api/state"):
            snapshot = build_snapshot()
            body = json.dumps(snapshot).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path.startswith("/api/teams"):
            body = json.dumps(build_teams()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path.startswith("/api/standings"):
            with _standings_lock:
                standings = list(_standings_cache)
            body = json.dumps(standings).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        page = PAGES.get(self.path)
        if page:
            file_path = BASE_DIR / page
            body = file_path.read_bytes()
            content_type = CONTENT_TYPES.get(file_path.suffix, "application/octet-stream")
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path in ("/circuits", "/circuits/"):
            return self._send_html(render_circuits_list())

        if self.path.startswith("/circuits/"):
            circuit = CIRCUITS_BY_ID.get(self.path[len("/circuits/"):])
            if circuit:
                return self._send_html(render_circuit_detail(circuit))
            self.send_response(404)
            self.end_headers()
            return

        if self.path in ("/drivers", "/drivers/"):
            return self._send_html(render_drivers_list())

        if self.path.startswith("/drivers/"):
            driver = DRIVERS_BY_ID.get(self.path[len("/drivers/"):])
            if driver:
                return self._send_html(render_driver_detail(driver))
            self.send_response(404)
            self.end_headers()
            return

        if self.path in ("/constructors", "/constructors/"):
            return self._send_html(render_constructors_list())

        if self.path.startswith("/constructors/"):
            constructor = CONSTRUCTORS_BY_ID.get(self.path[len("/constructors/"):])
            if constructor:
                return self._send_html(render_constructor_detail(constructor))
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if self.path == "/api/subscribe":
            length = int(self.headers.get("Content-Length", 0))
            try:
                payload = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError:
                payload = {}

            email = (payload.get("email") or "").strip()
            if not EMAIL_RE.match(email):
                return self._send_json({"error": "Please enter a valid email address."}, status=400)

            with _subscribers_lock:
                SUBSCRIBERS_FILE.parent.mkdir(exist_ok=True)
                with open(SUBSCRIBERS_FILE, "a") as f:
                    f.write(f"{email},{datetime.now(timezone.utc).isoformat()}\n")

            return self._send_json({"status": "ok"})

        self.send_response(404)
        self.end_headers()

    def _send_html(self, html_str: str) -> None:
        body = html_str.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, data: dict, status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    collector_thread = threading.Thread(target=run_collector_forever, daemon=True)
    collector_thread.start()

    standings_thread = threading.Thread(target=run_standings_refresher, daemon=True)
    standings_thread.start()

    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    logging.info(f"Serving on http://0.0.0.0:{port}/")
    server.serve_forever()


if __name__ == "__main__":
    main()
