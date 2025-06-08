import click
import logging
import asyncio
import aiosqlite
import aiofiles as aiof
import queue
import time
import json
import statsd
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Tuple
from pathlib import Path
from grpc import aio
from flask import Flask, render_template, request, redirect, url_for, jsonify

from aapis.tactical.v1 import tactical_pb2_grpc, tactical_pb2

from tactical.click_types import LogLevel


DEFAULT_INSECURE_PORT = 60060
DEFAULT_UIUXPAGE_PORT = 60070


class TAR_KEYS:
    TRIAGE = "Triage"
    ACTION = "Action"
    RESULT = "Result"


CATEGORIES = [TAR_KEYS.TRIAGE, TAR_KEYS.ACTION, TAR_KEYS.RESULT]


def format_sigfigs(value, sigfigs=2):
    if value == 0:
        return "0"
    from math import log10, floor

    digits = sigfigs - int(floor(log10(abs(value)))) - 1
    return f"{round(value, digits):g}"


class TacticalState:
    class Stats:
        def __init__(self):
            self.page_visits = 0
            self.triage_clockins = 0
            self.action_clockins = 0
            self.result_clockins = 0

    def __init__(self, storage_path: str, db_path: str):
        self._lock = asyncio.Lock()
        self._stats = TacticalState.Stats()
        self._storage_path = Path(storage_path)
        self._db_path = db_path
        self._data_defs = {
            "vocab": lambda vocab: (
                {"word": vocab.word, "definition": vocab.definition}
                if vocab is not None
                else {"word": "", "definition": ""}
            ),
            "journal": lambda journal: (
                {"date": journal.date, "entry": journal.entry}
                if journal is not None
                else {"date": "", "entry": ""}
            ),
            "today_tasks": lambda tasks: (
                {"tasks": [task for task in tasks.tasks]}
                if tasks is not None
                else {"tasks": []}
            ),
            "tomorrow_tasks": lambda tasks: (
                {"tasks": [task for task in tasks.tasks]}
                if tasks is not None
                else {"tasks": []}
            ),
            "quote": lambda quote: (
                {"author": quote.author, "quote": quote.quote}
                if quote is not None
                else {"author": "", "quote": ""}
            ),
            "wiki_url": lambda wiki_url: (
                {"url": wiki_url.url} if wiki_url is not None else {"url": ""}
            ),
        }
        self._data = {}
        for data_key in self._data_defs.keys():
            self._data[data_key] = self._data_defs[data_key](None)
        self._data["clock_state"] = {
            cat: {"state": "out", "timestamp": "NONE"} for cat in CATEGORIES
        }

        self._storage_path.parent.mkdir(exist_ok=True, parents=True)
        try:
            with open(self._storage_path, "r") as storage:
                self._data = json.loads(storage.read())
        except FileNotFoundError:
            with open(self._storage_path, "w") as storage:
                storage.write(json.dumps(self._data))

    async def _init_db(self):
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tag TEXT NOT NULL,
                    event_type TEXT CHECK(event_type IN ('in', 'out')) NOT NULL,
                    timestamp TEXT NOT NULL
                )
            """
            )
            await db.commit()

    async def _clock_in(self, tag: str):
        await asyncio.create_task(self._init_db())
        return await self._record_event(tag, "in")

    async def _clock_out(self, tag: str):
        await asyncio.create_task(self._init_db())
        return await self._record_event(tag, "out")

    async def _record_event(self, tag: str, event_type: str):
        timestamp = datetime.utcnow().isoformat()
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO events (tag, event_type, timestamp) VALUES (?, ?, ?)",
                (tag, event_type, timestamp),
            )
            await db.commit()
        return timestamp

    async def _weekly_totals(self):
        await asyncio.create_task(self._init_db())
        now = datetime.utcnow()
        start_of_week = now - timedelta(days=now.weekday() + 1)  # Sunday is start
        start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)

        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "SELECT tag, event_type, timestamp FROM events WHERE timestamp >= ? ORDER BY timestamp",
                (start_of_week.isoformat(),),
            )
            rows = await cursor.fetchall()

        tag_sessions = defaultdict(list)
        for tag, event_type, ts_str in rows:
            ts = datetime.fromisoformat(ts_str)
            tag_sessions[tag].append((event_type, ts))

        tag_totals = {}
        for tag, events in tag_sessions.items():
            total = timedelta()
            stack = []
            for event_type, ts in events:
                if event_type == "in":
                    stack.append(ts)
                elif event_type == "out" and stack:
                    start_ts = stack.pop()
                    total += ts - start_ts
            tag_totals[tag] = total.total_seconds() / 3600.0  # convert to hours

        return tag_totals

    async def setVal(self, obj_key, obj_val) -> bool:
        async with self._lock:
            if obj_key not in self._data_defs.keys():
                return False
            self._data[obj_key] = self._data_defs[obj_key](obj_val)
            async with aiof.open(self._storage_path, "w") as storage:
                await storage.write(json.dumps(self._data))
        return True

    async def getVal(self, obj_key) -> dict:
        async with self._lock:
            if obj_key in self._data.keys():
                return self._data[obj_key]
            elif obj_key in self._data_defs.keys():
                self._data[obj_key] = self._data_defs[obj_key](None)
                return self._data[obj_key]
            else:
                return {}

    async def clockIn(self, itar_key) -> None:
        async with self._lock:
            if itar_key in [TAR_KEYS.TRIAGE, TAR_KEYS.ACTION, TAR_KEYS.RESULT]:
                if itar_key == TAR_KEYS.TRIAGE:
                    self._stats.triage_clockins += 1
                elif itar_key == TAR_KEYS.ACTION:
                    self._stats.action_clockins += 1
                else:
                    self._stats.result_clockins += 1
                self._data["clock_state"][itar_key]["timestamp"] = await self._clock_in(
                    itar_key
                )
                self._data["clock_state"][itar_key]["state"] = "in"
                async with aiof.open(self._storage_path, "w") as storage:
                    await storage.write(json.dumps(self._data))

    async def clockOut(self, itar_key) -> None:
        async with self._lock:
            if itar_key in [TAR_KEYS.TRIAGE, TAR_KEYS.ACTION, TAR_KEYS.RESULT]:
                self._data["clock_state"][itar_key]["timestamp"] = (
                    await self._clock_out(itar_key)
                )
                self._data["clock_state"][itar_key]["state"] = "out"
                async with aiof.open(self._storage_path, "w") as storage:
                    await storage.write(json.dumps(self._data))

    async def getWeekTimesheet(self) -> Tuple[float, float, float]:
        async with self._lock:
            tag_totals = await self._weekly_totals()
            return (
                tag_totals[TAR_KEYS.TRIAGE],
                tag_totals[TAR_KEYS.ACTION],
                tag_totals[TAR_KEYS.RESULT],
            )

    async def incrementPageVisits(self) -> None:
        async with self._lock:
            self._stats.page_visits += 1

    async def getStats(self):
        async with self._lock:
            return self._stats

    async def getData(self):
        async with self._lock:
            return self._data


class TacticalService(tactical_pb2_grpc.TacticalServiceServicer):
    def __init__(self, tactical_state, statsd_port=None):
        self._state = tactical_state

        if statsd_port is not None:
            try:
                statsd_port = int(statsd_port)
                if 1 <= statsd_port <= 65535:
                    self._statsd = statsd.StatsClient("localhost", statsd_port)
                    logging.info(f"StatsD client initialized on port {statsd_port}")
                else:
                    logging.warning(
                        f"Invalid StatsD port: {statsd_port}. Port must be between 1 and 65535."
                    )
                    self._statsd = None
            except ValueError:
                logging.warning(
                    f"Invalid StatsD port: {statsd_port}. Must be an integer."
                )
                self._statsd = None
        else:
            self._statsd = None
        self._statsd_prefix = "tactical"

        asyncio.get_event_loop().create_task(self.metrics_publish_thread())

    async def DailyRefresh(self, request, context):
        return tactical_pb2.DailyRefreshResponse(
            success=True
        )  # TODO for now this does nothing

    async def UpdateComponent(self, request, context):
        component_update = request.component_update
        success = False
        if component_update.WhichOneof("component") == "vocab":
            logging.info("Adding vocab component")
            success = await self._state.setVal("vocab", component_update.vocab)
        elif component_update.WhichOneof("component") == "journal":
            logging.info("Adding journal component")
            success = await self._state.setVal("journal", component_update.journal)
        elif component_update.WhichOneof("component") == "today_tasks":
            logging.info("Adding today_tasks component")
            success = await self._state.setVal(
                "today_tasks", component_update.today_tasks
            )
        elif component_update.WhichOneof("component") == "tomorrow_tasks":
            logging.info("Adding tomorrow_tasks component")
            success = await self._state.setVal(
                "tomorrow_tasks", component_update.tomorrow_tasks
            )
        elif component_update.WhichOneof("component") == "quote":
            logging.info("Adding quote component")
            success = await self._state.setVal("quote", component_update.quote)
        elif component_update.WhichOneof("component") == "wiki_url":
            logging.info("Adding wiki_url component")
            success = await self._state.setVal("wiki_url", component_update.wiki_url)
        return tactical_pb2.UpdateComponentResponse(success=success)

    async def metrics_publish_thread(self):
        if not self._statsd:
            return
        while True:
            stats = await self._state.getStats()
            self._statsd.gauge(f"{self._statsd_prefix}_page_visits", stats.page_visits)
            self._statsd.gauge(
                f"{self._statsd_prefix}_triage_clockins", stats.triage_clockins
            )
            self._statsd.gauge(
                f"{self._statsd_prefix}_action_clockins", stats.action_clockins
            )
            self._statsd.gauge(
                f"{self._statsd_prefix}_result_clockins", stats.result_clockins
            )
            await asyncio.sleep(10.0)


import signal


async def serve_grpc(port, state, statsd_port=None):
    server = aio.server()
    tactical_pb2_grpc.add_TacticalServiceServicer_to_server(
        TacticalService(state, statsd_port), server
    )
    listen_addr = f"[::]:{port}"
    server.add_insecure_port(listen_addr)
    logging.info(f"Starting daily server on {listen_addr}")
    await server.start()

    async def graceful_shutdown():
        logging.info("Shutting down server...")
        await server.stop(5)
        logging.info("Server stopped.")

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGINT, lambda: asyncio.create_task(graceful_shutdown())
    )

    await server.wait_for_termination()


def create_flask_app(shared_state):
    app = Flask(__name__)

    @app.route("/")
    def index():
        data = asyncio.run(shared_state.getData())
        ttime, atime, rtime = asyncio.run(shared_state.getWeekTimesheet())
        data["weekly_total"] = ttime + atime + rtime
        data["weekly_hours"] = {
            TAR_KEYS.TRIAGE: ttime,
            TAR_KEYS.ACTION: atime,
            TAR_KEYS.RESULT: rtime,
        }
        return render_template("dashboard.html", **data)

    @app.route("/api/clock", methods=["POST"])
    def api_clock():
        data = request.get_json()
        category = data["category"]
        action = data["action"]
        if action == "clock_in":
            asyncio.run(shared_state.clockIn(category))
            new_state = "in"
        else:
            asyncio.run(shared_state.clockOut(category))
            new_state = "out"
        return jsonify(success=True, new_state=new_state)

    @app.template_filter("sigfig")
    def sigfig_filter(value, sigfigs=2):
        return format_sigfigs(float(value), sigfigs)

    return app


def run_flask(port, state):
    app = create_flask_app(state)
    app.run(port=port, use_reloader=False)


@click.command()
@click.option(
    "-d",
    "--db-path",
    type=str,
    required=True,
)
@click.option(
    "-s",
    "--storage-path",
    type=str,
    required=True,
)
@click.option(
    "--server-port",
    type=int,
    default=DEFAULT_INSECURE_PORT,
)
@click.option(
    "--web-port",
    type=int,
    default=DEFAULT_UIUXPAGE_PORT,
)
@click.option(
    "--statsd-port",
    type=int,
    default=None,
    help="StatsD metrics publish port",
)
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default=logging.INFO,
)
def cli(db_path, storage_path, server_port, web_port, statsd_port, log_level):
    """Spawn the Daily tactical server."""
    logging.basicConfig(level=log_level)
    logging.info(f"Log level set to {log_level}")
    state = TacticalState(storage_path, db_path)
    flask_thread = threading.Thread(
        target=run_flask,
        args=(
            web_port,
            state,
        ),
    )
    flask_thread.start()
    asyncio.run(serve_grpc(server_port, state, statsd_port))


def main():
    cli()


if __name__ == "__main__":
    main()
