import click
import logging
import asyncio
import aiosqlite
import aiofiles as aiof
import os
import json
import statsd
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Tuple
from pathlib import Path
from grpc import aio
from flask import Flask, Blueprint, render_template, request, jsonify, Response
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker, scoped_session

from aapis.tactical.v1 import tactical_pb2_grpc, tactical_pb2

from tactical.click_types import LogLevel

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

DEFAULT_INSECURE_PORT = 60060
DEFAULT_UIUXPAGE_PORT = 60070


def init_sync_db(db_path):
    engine = create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )
    session_factory = sessionmaker(bind=engine)
    return scoped_session(session_factory)


def get_survey_heatmap_data(db):
    today = date.today()
    start_date = today - timedelta(days=14)
    end_date = today - timedelta(days=1)

    # Build empty response shape in case DB isn't ready
    empty_result = ([(start_date + timedelta(days=i)) for i in range(14)], {})

    query = text("""
        SELECT survey_name, question_name, date, result
        FROM survey_results
        WHERE date BETWEEN :start AND :end
        ORDER BY survey_name, question_name, date
    """)

    try:
        rows = db.execute(query, {"start": start_date, "end": end_date}).fetchall()
    except OperationalError:
        return empty_result

    date_range = empty_result[0]

    result = {}
    for survey, question, d, value in rows:
        result.setdefault(survey, {}).setdefault(question, {})[d] = value

    final = {}
    for survey, questions in result.items():
        final[survey] = [
            {
                "question": question,
                "results": [answers.get(dt.strftime("%Y-%m-%d")) for dt in date_range]
            }
            for question, answers in questions.items()
        ]

    return date_range, final


class TAR_KEYS:
    TRIAGE = "Triage"
    ACTION = "Action"
    RESULT = "Result"


CATEGORIES = [TAR_KEYS.TRIAGE, TAR_KEYS.ACTION, TAR_KEYS.RESULT]


class SURVEY_Q_STATUS:
    EMPTY = 0
    NO_CREDIT = 1
    PARTIAL_CREDIT = 2
    FULL_CREDIT = 3


def map_survey_result_to_status(result):
    if (
        result
        == tactical_pb2.SurveyQuestionResultType.SURVEY_QUESTION_RESULT_TYPE_NO_CREDIT
    ):
        return SURVEY_Q_STATUS.NO_CREDIT
    elif (
        result
        == tactical_pb2.SurveyQuestionResultType.SURVEY_QUESTION_RESULT_TYPE_PARTIAL_CREDIT
    ):
        return SURVEY_Q_STATUS.PARTIAL_CREDIT
    elif (
        result
        == tactical_pb2.SurveyQuestionResultType.SURVEY_QUESTION_RESULT_TYPE_FULL_CREDIT
    ):
        return SURVEY_Q_STATUS.FULL_CREDIT
    else:
        return SURVEY_Q_STATUS.EMPTY


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

    def __init__(self, storage_path: str, db_path: str, surveys_path: str):
        self._lock = asyncio.Lock()
        self._stats = TacticalState.Stats()
        self._storage_path = Path(os.path.expanduser(storage_path))
        self._db_path = os.path.expanduser(db_path)
        self._surveys_path = os.path.expanduser(surveys_path)
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
            "tar_links": lambda tar_links: (
                {
                    TAR_KEYS.TRIAGE: [
                        (
                            "ITNS",
                            "https://www.notion.so/ITNS-3ea6f1aa43564b0386bcaba6c7b79870",
                        ),
                        ("Trello", "https://trello.com/w/workspace69213858"),
                        ("Tasks", "https://calendar.google.com/calendar/u/0/r/tasks"),
                    ],
                    TAR_KEYS.ACTION: [
                        (
                            "ITNS",
                            "https://www.notion.so/ITNS-3ea6f1aa43564b0386bcaba6c7b79870",
                        ),
                        ("Trello", "https://trello.com/w/workspace69213858"),
                        ("Tasks", "https://calendar.google.com/calendar/u/0/r/tasks"),
                        ("Math", "https://github.com/goromal/scratchpad"),
                    ],
                    TAR_KEYS.RESULT: [
                        ("Wiki", "http://ats.local/wiki/"),
                        ("Projects", "https://github.com/goromal/projects"),
                        ("Software", "https://github.com/goromal/anixpkgs"),
                        ("Notes", "https://github.com/goromal/notes/tree/master"),
                        ("Math", "https://github.com/goromal/scratchpad"),
                        ("Art", "https://github.com/goromal/art"),
                        ("Resume", "https://andrewtorgesen.com/res/resume.pdf"),
                    ],
                }
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
                data = json.loads(storage.read())
                self._data = self._data | data
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
            ttotal = (
                tag_totals[TAR_KEYS.TRIAGE] if TAR_KEYS.TRIAGE in tag_totals else 0.0
            )
            atotal = (
                tag_totals[TAR_KEYS.ACTION] if TAR_KEYS.ACTION in tag_totals else 0.0
            )
            rtotal = (
                tag_totals[TAR_KEYS.RESULT] if TAR_KEYS.RESULT in tag_totals else 0.0
            )
            return (
                ttotal,
                atotal,
                rtotal,
            )

    async def incrementPageVisits(self) -> None:
        async with self._lock:
            self._stats.page_visits += 1

    async def getStats(self):
        async with self._lock:
            return self._stats

    async def getData(self):
        async with self._lock:
            surveys = []
            try:
                async with aiof.open(self._surveys_path, "r") as surveys_csv:
                    async for line in surveys_csv:
                        link_and_name = line.split("|")
                        surveys.append(
                            {"link": link_and_name[0], "name": link_and_name[1]}
                        )
            except:
                logging.warning("Unable to read and populate survey links")
            self._data["surveys"] = surveys
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
        )  # for now this does nothing

    async def SubmitSurveyResult(self, request, context):
        logging.info("Received survey submission")
        await self._state._init_db()

        year = int(request.result.year)
        month = int(request.result.month)
        day = int(request.result.day)
        date = f"{year:04d}-{month:02d}-{day:02d}"
        survey_name = request.result.survey_name

        results = [
            (r.question_name, map_survey_result_to_status(r.result))
            for r in request.result.results
        ]

        async with aiosqlite.connect(self._state._db_path) as db:
            # Create table + indexes if not exist
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS survey_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    survey_name TEXT NOT NULL,
                    question_name TEXT NOT NULL,
                    result INTEGER NOT NULL,
                    UNIQUE(date, survey_name, question_name)
                )
            """
            )

            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_survey_key ON survey_results (date, survey_name)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_survey_question_lookup ON survey_results (survey_name, question_name)"
            )

            # Fetch historical questions for this survey name (spanning any date)
            cursor = await db.execute(
                """
                SELECT DISTINCT question_name FROM survey_results
                WHERE survey_name = ?
            """,
                (survey_name,),
            )
            historical_questions = {row[0] for row in await cursor.fetchall()}

            # Fetch existing results for this same date
            cursor = await db.execute(
                """
                SELECT question_name, result FROM survey_results
                WHERE date = ? AND survey_name = ?
            """,
                (date, survey_name),
            )
            existing = {row[0]: row[1] for row in await cursor.fetchall()}

            submitted_names = {q for q, _ in results}

            # 1. Insert/update submitted questions
            for question_name, new_result in results:
                if question_name in existing:
                    if new_result > existing[question_name]:
                        await db.execute(
                            """
                            UPDATE survey_results
                            SET result = ?
                            WHERE date = ? AND survey_name = ? AND question_name = ?
                        """,
                            (new_result, date, survey_name, question_name),
                        )
                else:
                    await db.execute(
                        """
                        INSERT OR IGNORE INTO survey_results (date, survey_name, question_name, result)
                        VALUES (?, ?, ?, ?)
                    """,
                        (date, survey_name, question_name, new_result),
                    )

                historical_questions.add(question_name)  # Keep global list updated

            # 2. Any historical question missing today → record EMPTY
            missing_questions_today = historical_questions - submitted_names
            for question_name in missing_questions_today:
                if question_name not in existing:
                    await db.execute(
                        """
                        INSERT OR IGNORE INTO survey_results (date, survey_name, question_name, result)
                        VALUES (?, ?, ?, ?)
                    """,
                        (date, survey_name, question_name, SURVEY_Q_STATUS.EMPTY),
                    )

            await db.commit()

        return tactical_pb2.SubmitSurveyResultResponse(success=True)

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


def create_flask_app(shared_state, subdomain, main_loop):
    app = Flask(
        __name__,
        template_folder=os.path.join(BASE_DIR, "templates"),
        static_folder=os.path.join(BASE_DIR, "static"),
        static_url_path=f"/{subdomain}/static",
    )
    app.secret_key = os.urandom(24)
    db = init_sync_db(shared_state._db_path)
    bp = Blueprint("tactical", __name__, url_prefix=subdomain)

    @bp.route("/", methods=["GET", "POST"])
    def index():
        asyncio.run_coroutine_threadsafe(
            shared_state.incrementPageVisits(), main_loop
        ).result()
        dataFuture = asyncio.run_coroutine_threadsafe(shared_state.getData(), main_loop)
        data = dataFuture.result()
        timesheetFuture = asyncio.run_coroutine_threadsafe(
            shared_state.getWeekTimesheet(), main_loop
        )
        ttime, atime, rtime = timesheetFuture.result()
        date_range, survey_visualization = get_survey_heatmap_data(db)

        data["weekly_total"] = ttime + atime + rtime
        data["weekly_hours"] = {
            TAR_KEYS.TRIAGE: ttime,
            TAR_KEYS.ACTION: atime,
            TAR_KEYS.RESULT: rtime,
        }
        return render_template(
            "dashboard.html",
            survey_visualization=survey_visualization,
            date_range=date_range,
            **data,
        )

    @bp.route("/feed.xml")
    def rss_feed():
        dataFuture = asyncio.run_coroutine_threadsafe(shared_state.getData(), main_loop)
        data = dataFuture.result()
        rendered_xml = render_template(
            "feed.xml.j2",
            build_date=datetime.utcnow().strftime("%a, %d %b %Y 00:00:00 +0000"),
            **data,
        )
        return Response(rendered_xml, mimetype="application/rss+xml")

    @bp.route("/api/clock", methods=["POST"])
    def api_clock():
        data = request.get_json()
        category = data["category"]
        action = data["action"]
        if action == "clock_in":
            asyncio.run_coroutine_threadsafe(
                shared_state.clockIn(category), main_loop
            ).result()
            new_state = "in"
        else:
            asyncio.run_coroutine_threadsafe(
                shared_state.clockOut(category), main_loop
            ).result()
            new_state = "out"
        return jsonify(success=True, new_state=new_state)

    @app.template_filter("sigfig")
    def sigfig_filter(value, sigfigs=2):
        return format_sigfigs(float(value), sigfigs)

    @app.teardown_appcontext
    def remove_session(exception=None):
        db.remove()

    app.register_blueprint(bp)
    return app


def run_flask(port, state, subdomain, main_loop):
    app = create_flask_app(state, subdomain, main_loop)
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
@click.option(
    "--subdomain",
    type=str,
    default="/tactical",
    help="Subdomain for a reverse proxy",
)
@click.option(
    "--surveys",
    type=str,
    default="~/configs/survey-links.csv",
    help="Path to survey links",
)
def cli(
    db_path,
    storage_path,
    server_port,
    web_port,
    statsd_port,
    log_level,
    subdomain,
    surveys,
):
    """Spawn the Daily tactical server."""
    logging.basicConfig(level=log_level)
    logging.info(f"Log level set to {log_level}")
    state = TacticalState(storage_path, db_path, surveys)
    main_async_loop = asyncio.get_event_loop()
    flask_thread = threading.Thread(
        target=run_flask,
        args=(
            web_port,
            state,
            subdomain,
            main_async_loop,
        ),
    )
    flask_thread.start()
    main_async_loop.run_until_complete(serve_grpc(server_port, state, statsd_port))


def main():
    cli()


if __name__ == "__main__":
    main()
