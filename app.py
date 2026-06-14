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
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests
from fastf1.livetiming.client import SignalRClient
from signalrcore.hub_connection_builder import HubConnectionBuilder

BASE_DIR = Path(__file__).parent
DATA_FILE = BASE_DIR / "live_data.txt"

PAGES = {
    "/": "index.html",
    "/index.html": "index.html",
    "/live": "live.html",
    "/live.html": "live.html",
    "/styles.css": "styles.css",
}
CONTENT_TYPES = {
    ".html": "text/html",
    ".css": "text/css",
}

_lock = threading.Lock()
_drivers: dict[str, dict] = {}
_session_info: dict = {}
_track_status: dict = {}
_file_pos = 0
_prev_positions: dict[str, int] = {}
_position_deltas: dict[str, int] = {}


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

        self.send_response(404)
        self.end_headers()


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    collector_thread = threading.Thread(target=run_collector_forever, daemon=True)
    collector_thread.start()

    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    logging.info(f"Serving on http://0.0.0.0:{port}/")
    server.serve_forever()


if __name__ == "__main__":
    main()
