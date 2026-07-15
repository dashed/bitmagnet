from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


runner = load("graphql_rss_runner", "run.py")
helper = load("graphql_rss_helper", "helper.py")


class HarnessTests(unittest.TestCase):
    def test_profile_limits_match_container_helper(self):
        for name, limits in runner.PROFILE_LIMITS.items():
            profile = helper.PROFILES[name]
            self.assertEqual(limits["max_decompressed_bytes"], profile.max_decompressed_bytes)
            self.assertEqual(limits["decoded_byte_budget"], profile.decoded_byte_budget)
            self.assertEqual(limits["retained_byte_budget"], profile.retained_byte_budget)
            self.assertEqual(
                limits["accepted_files_per_blob"], profile.accepted_files_per_blob
            )

    def test_projection_controls_graphql_files_field(self):
        minimal = helper.graphql_query("minimal")
        files = helper.graphql_query("files")
        self.assertNotIn("files {", minimal)
        self.assertIn("files { infoHash index path extension size }", files)
        self.assertIn("sources { key name seenCount firstSeenAt lastSeenAt }", minimal)

    def test_parse_memory_events_ignores_malformed_lines(self):
        self.assertEqual(
            runner.parse_memory_events("low 3\noom 0\nbad\nmax nope\noom_kill 1\n"),
            {"low": 3, "oom": 0, "oom_kill": 1},
        )

    def test_barrier_evidence_requires_one_complete_four_party_generation(self):
        events = []
        for arrival in range(1, 5):
            events.append(
                {
                    "event": "arrive",
                    "key": "accepted",
                    "generation": 7,
                    "arrival": arrival,
                }
            )
        for arrival in range(1, 5):
            events.append(
                {
                    "event": "release",
                    "key": "accepted",
                    "generation": 7,
                    "arrival": arrival,
                }
            )
            events.append(
                {
                    "event": "respond",
                    "key": "accepted",
                    "generation": 7,
                }
            )
        evidence = runner.barrier_evidence(events, "accepted")
        self.assertTrue(evidence["ok"])
        events.append(
            {"event": "timeout", "key": "accepted", "generation": 7, "arrival": 1}
        )
        self.assertFalse(runner.barrier_evidence(events, "accepted")["ok"])

    def test_evaluation_passes_only_valid_responses_and_headroom(self):
        driver = {
            "metrics_error": None,
            "metrics_samples": [
                "bitmagnet_search_pathsearch_healthy 1",
                'bitmagnet_search_pathsearch_route_total{result="served"} 4',
                "bitmagnet_search_pathsearch_refine_retained_capped_total 0",
            ],
            "responses": [
                {
                    "http_status": 200,
                    "graphql_errors": [],
                    "item_count": 4,
                    "file_count": 56_000,
                    "handler_duration_us": "123",
                }
                for _ in range(4)
            ],
        }
        barrier = {"ok": True}
        cgroup = {
            "memory_peak": 5 * runner.GIB,
            "memory_events_local": {"oom": 0, "oom_kill": 0},
        }
        state = {"OOMKilled": False}
        result = runner.evaluate_run(
            driver=driver,
            barrier=barrier,
            cgroup=cgroup,
            state=state,
            scenario="accepted",
            projection="files",
            accepted_files_per_blob=14_000,
            max_peak_bytes=6 * runner.GIB,
        )
        self.assertTrue(result["passed"])

        cgroup["memory_events_local"]["oom_kill"] = 1
        failed = runner.evaluate_run(
            driver=driver,
            barrier=barrier,
            cgroup=cgroup,
            state=state,
            scenario="accepted",
            projection="files",
            accepted_files_per_blob=14_000,
            max_peak_bytes=6 * runner.GIB,
        )
        self.assertFalse(failed["passed"])
        self.assertFalse(failed["checks"]["no_cgroup_oom"])

    def test_minimal_schema_has_every_integration_boundary(self):
        schema = (HERE / "schema.sql").read_text()
        for fragment in (
            "CREATE TABLE torrents (",
            "files_data bytea",
            "CREATE TABLE torrent_contents (",
            "CREATE TABLE torrent_file_summary (",
            "CREATE TABLE torrents_torrent_sources (",
            "CREATE TABLE goose_db_version (",
        ):
            self.assertIn(fragment, schema)


if __name__ == "__main__":
    unittest.main()
