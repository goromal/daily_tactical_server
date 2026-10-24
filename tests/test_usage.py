from datetime import datetime, timedelta, timezone
import json
import os
import tempfile
import unittest

from tactical.usage import OTHER_LABEL, UsageArchive, parse_ingest_body, summarize

NOW = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)


def event(service, category="Content", device="desktop", seconds=30, ago=0, host="ats"):
    return {
        "timestamp": (NOW - timedelta(seconds=ago)).isoformat().replace("+00:00", "Z"),
        "host": host,
        "service": service,
        "category": category,
        "device": device,
        "seconds": seconds,
    }


class UsageArchiveTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.archive = UsageArchive(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_rejects_malformed_events(self):
        accepted, rejected = self.archive.append(
            [
                event("folio"),
                event("folio", category="Games"),
                event("folio", device="tablet"),
                event("folio", seconds=3600),
                event("folio", seconds=True),
                event(""),
                {"service": "folio"},
                "not an event",
            ]
        )
        self.assertEqual((accepted, rejected), (1, 7))

    def test_files_events_by_month(self):
        self.archive.append([event("folio"), event("folio", ago=40 * 86400)])
        self.assertEqual(
            sorted(os.listdir(self._tmp.name)),
            ["usage-2026-09.jsonl", "usage-2026-10.jsonl"],
        )

    def test_summary_window_excludes_older_events(self):
        self.archive.append(
            [
                event("folio", ago=3600),
                event("Plex", ago=3 * 86400),
                event("Wiki", ago=40 * 86400),
            ]
        )
        day = self.archive.summary(1, now=NOW)
        month = self.archive.summary(30, now=NOW)
        self.assertEqual(day["total"], 30)
        self.assertEqual(month["total"], 60)

    def test_summary_sees_appends_after_first_read(self):
        self.archive.append([event("folio")])
        self.assertEqual(self.archive.summary(1, now=NOW)["total"], 30)
        self.archive.append([event("folio")])
        self.assertEqual(self.archive.summary(1, now=NOW)["total"], 60)


class SummarizeTest(unittest.TestCase):
    def test_device_filter_keeps_full_device_split(self):
        result = summarize(
            [
                event("folio", device="desktop"),
                event("folio", device="mobile", seconds=10),
            ],
            device="mobile",
        )
        self.assertEqual(result["total"], 10)
        self.assertEqual(
            {d["label"]: d["seconds"] for d in result["devices"]},
            {"desktop": 30, "mobile": 10},
        )

    def test_categories_are_always_present(self):
        result = summarize([event("folio", category="Aspiration")])
        self.assertEqual(
            [c["label"] for c in result["categories"]],
            ["Aspiration", "Content", "Utilities"],
        )
        self.assertEqual(result["services"]["Utilities"], [])

    def test_small_tail_folds_into_other(self):
        events = [event(f"svc{i}", seconds=60 - i) for i in range(8)]
        slices = summarize(events)["services"]["Content"]
        self.assertEqual(len(slices), 6)
        self.assertEqual(slices[0]["label"], "svc0")
        self.assertEqual(slices[-1]["label"], OTHER_LABEL)
        self.assertEqual(slices[-1]["members"], ["svc5", "svc6", "svc7"])
        self.assertEqual(slices[-1]["seconds"], 55 + 54 + 53)

    def test_six_services_need_no_other_slice(self):
        events = [event(f"svc{i}") for i in range(6)]
        slices = summarize(events)["services"]["Content"]
        self.assertNotIn(OTHER_LABEL, [s["label"] for s in slices])


class IngestBodyTest(unittest.TestCase):
    def test_parses_ndjson_and_skips_bad_lines(self):
        body = (
            json.dumps(event("folio")) + "\nnot json\n\n" + json.dumps(event("Plex"))
        ).encode()
        self.assertEqual(
            [e["service"] for e in parse_ingest_body(body)], ["folio", "Plex"]
        )

    def test_parses_json_array(self):
        body = json.dumps([event("folio"), event("Plex")]).encode()
        self.assertEqual(len(parse_ingest_body(body)), 2)


if __name__ == "__main__":
    unittest.main()
