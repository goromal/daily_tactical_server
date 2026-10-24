"""Archive and summarize UI usage heartbeats from every anix machine.

The metrics hub's Vector forwards each heartbeat here as one NDJSON line:
``{"timestamp": ..., "host": ..., "service": ..., "category": ...,
"device": ..., "seconds": ...}``. Events are appended to one file per month
(``usage-YYYY-MM.jsonl``) so a summary only reads the months its window
touches, and the raw record stays easy to inspect or replay.
"""

import json
import os
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone

USAGE_CATEGORIES = ("Aspiration", "Content", "Utilities")
USAGE_DEVICES = ("desktop", "mobile")
USAGE_RANGE_DAYS = (1, 7, 30, 90, 365)
# A pie past six segments stops reading as part-to-whole, so each category
# shows its top services and folds the rest into one slice.
TOP_SERVICES = 5
OTHER_LABEL = "Other"


def _parse_timestamp(value):
    try:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def normalize_event(raw):
    """The archived form of one heartbeat, or None when it is malformed.

    Vector validates heartbeats at the edge, but the archive is the durable
    record, so it re-checks rather than trusting whichever host forwarded it.
    """
    if not isinstance(raw, dict):
        return None
    ts = _parse_timestamp(raw.get("timestamp"))
    service = raw.get("service")
    category = raw.get("category")
    device = raw.get("device")
    host = raw.get("host")
    seconds = raw.get("seconds")
    if (
        ts is None
        or not isinstance(service, str)
        or not service
        or len(service) > 64
        or category not in USAGE_CATEGORIES
        or device not in USAGE_DEVICES
        or not isinstance(host, str)
        or isinstance(seconds, bool)
        or not isinstance(seconds, (int, float))
        or not 0 < seconds <= 60
    ):
        return None
    return {
        "timestamp": ts.isoformat(),
        "host": host,
        "service": service,
        "category": category,
        "device": device,
        "seconds": int(seconds),
    }


def parse_ingest_body(body):
    """Events from an ingest request: an NDJSON stream or one JSON array."""
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            items = json.loads(text)
        except ValueError:
            return []
        return items if isinstance(items, list) else []
    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            items.append(json.loads(line))
        except ValueError:
            continue
    return items


class UsageArchive:
    def __init__(self, directory):
        self._dir = directory
        self._lock = threading.Lock()
        # path -> (size, events): a month file is only re-read once it grows.
        self._cache = {}

    def _path(self, year, month):
        return os.path.join(self._dir, f"usage-{year:04d}-{month:02d}.jsonl")

    def append(self, raw_events):
        """Archive the valid events; returns (accepted, rejected) counts."""
        by_path = defaultdict(list)
        rejected = 0
        for raw in raw_events:
            event = normalize_event(raw)
            if event is None:
                rejected += 1
                continue
            ts = _parse_timestamp(event["timestamp"])
            by_path[self._path(ts.year, ts.month)].append(event)
        if by_path:
            with self._lock:
                os.makedirs(self._dir, exist_ok=True)
                for path, events in by_path.items():
                    with open(path, "a", encoding="utf-8") as f:
                        for event in events:
                            f.write(json.dumps(event, separators=(",", ":")) + "\n")
        accepted = sum(len(v) for v in by_path.values())
        return accepted, rejected

    def _month_events(self, path):
        try:
            size = os.path.getsize(path)
        except OSError:
            return []
        with self._lock:
            cached = self._cache.get(path)
            if cached and cached[0] == size:
                return cached[1]
            events = []
            with open(path, encoding="utf-8") as f:
                for line in f:
                    try:
                        event = normalize_event(json.loads(line))
                    except ValueError:
                        continue
                    if event is not None:
                        event["_ts"] = _parse_timestamp(event["timestamp"])
                        events.append(event)
            self._cache[path] = (size, events)
            return events

    def events_since(self, start):
        year, month = start.year, start.month
        now = datetime.now(timezone.utc)
        while (year, month) <= (now.year, now.month):
            for event in self._month_events(self._path(year, month)):
                if event["_ts"] >= start:
                    yield event
            year, month = (year + 1, 1) if month == 12 else (year, month + 1)

    def summary(self, days, device=None, now=None):
        now = now or datetime.now(timezone.utc)
        start = now - timedelta(days=days)
        return summarize(self.events_since(start), device)


def summarize(events, device=None):
    """Active seconds for the overview pie, each category's pie, and devices.

    `device` filters the pies; the device split itself always covers both, so
    switching the filter never hides how the window divides between them.
    """
    categories = {c: 0 for c in USAGE_CATEGORIES}
    services = {c: defaultdict(int) for c in USAGE_CATEGORIES}
    devices = {d: 0 for d in USAGE_DEVICES}
    hosts = set()
    for event in events:
        devices[event["device"]] += event["seconds"]
        if device and event["device"] != device:
            continue
        categories[event["category"]] += event["seconds"]
        services[event["category"]][event["service"]] += event["seconds"]
        hosts.add(event["host"])

    def fold(counts):
        ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        if len(ranked) <= TOP_SERVICES + 1:
            return [{"label": k, "seconds": v} for k, v in ranked]
        top = [{"label": k, "seconds": v} for k, v in ranked[:TOP_SERVICES]]
        rest = ranked[TOP_SERVICES:]
        top.append(
            {
                "label": OTHER_LABEL,
                "seconds": sum(v for _, v in rest),
                "members": [k for k, _ in rest],
            }
        )
        return top

    return {
        "total": sum(categories.values()),
        "categories": [
            {"label": c, "seconds": categories[c]} for c in USAGE_CATEGORIES
        ],
        "services": {c: fold(services[c]) for c in USAGE_CATEGORIES},
        "devices": [{"label": d, "seconds": devices[d]} for d in USAGE_DEVICES],
        "hosts": sorted(hosts),
    }
