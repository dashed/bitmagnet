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
            self.assertEqual(
                limits["accepted_path_payload_bytes"],
                profile.accepted_path_payload_bytes,
            )
            self.assertEqual(
                limits["adversarial_extra_bytes"], profile.adversarial_extra_bytes
            )
            self.assertEqual(
                limits["min_retained_fill_percent"],
                profile.min_retained_fill_percent,
            )

    def test_accepted_fixture_has_retained_budget_pressure(self):
        for profile in helper.PROFILES.values():
            # Payload bytes alone are a conservative lower bound; path prefixes,
            # suffixes, and decoded extension strings increase the actual charge.
            payload_bytes = (
                len(helper.ACCEPTED_HASHES)
                * profile.accepted_files_per_blob
                * profile.accepted_path_payload_bytes
            )
            self.assertLess(payload_bytes, profile.retained_byte_budget)
            self.assertGreaterEqual(
                payload_bytes * 100,
                profile.retained_byte_budget * profile.min_retained_fill_percent,
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
                    "total_count": 4,
                    "total_count_is_estimate": True,
                    "has_next_page": False,
                    "handler_duration_us": "123",
                }
                for _ in range(4)
            ],
        }
        barrier = {"ok": True}
        refine_barrier = {"ok": True, "arrivals": 4}
        cgroup = {
            "memory_peak": 5 * runner.GIB,
            "intentional_stop_requested": True,
            "app_exit_status": 143,
            "memory_events_local": {
                "oom": 0,
                "oom_kill": 0,
                "oom_group_kill": 0,
            },
        }
        live_state = {"Running": True, "OOMKilled": False, "Dead": False}
        state = {
            "OOMKilled": False,
            "Dead": False,
            "Status": "exited",
            "ExitCode": 0,
            "Error": "",
        }
        result = runner.evaluate_run(
            driver=driver,
            barrier=barrier,
            refine_barrier=refine_barrier,
            cgroup=cgroup,
            live_state=live_state,
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
            refine_barrier=refine_barrier,
            cgroup=cgroup,
            live_state=live_state,
            state=state,
            scenario="accepted",
            projection="files",
            accepted_files_per_blob=14_000,
            max_peak_bytes=6 * runner.GIB,
        )
        self.assertFalse(failed["passed"])
        self.assertFalse(failed["checks"]["no_cgroup_oom"])

        cgroup["memory_events_local"]["oom_kill"] = 0
        cgroup["app_exit_status"] = 137
        failed = runner.evaluate_run(
            driver=driver,
            barrier=barrier,
            refine_barrier=refine_barrier,
            cgroup=cgroup,
            live_state=live_state,
            state=state,
            scenario="accepted",
            projection="files",
            accepted_files_per_blob=14_000,
            max_peak_bytes=6 * runner.GIB,
        )
        self.assertFalse(failed["passed"])
        self.assertFalse(failed["checks"]["intentional_child_exit_expected"])

    def test_evaluation_fails_closed_on_missing_kernel_or_docker_state(self):
        driver = {
            "metrics_error": None,
            "metrics_samples": [
                "bitmagnet_search_pathsearch_healthy 1",
                'bitmagnet_search_pathsearch_route_total{result="served"} 4',
                "bitmagnet_search_pathsearch_refine_retained_capped_total 4",
            ],
            "responses": [
                {
                    "http_status": 200,
                    "graphql_errors": [],
                    "item_count": 0,
                    "file_count": 0,
                    "total_count": 0,
                    "total_count_is_estimate": True,
                    "has_next_page": False,
                    "handler_duration_us": "1",
                }
                for _ in range(4)
            ],
        }
        result = runner.evaluate_run(
            driver=driver,
            barrier={"ok": True},
            refine_barrier={"ok": True},
            cgroup={"memory_peak": runner.GIB},
            live_state={},
            state={},
            scenario="adversarial",
            projection="minimal",
            accepted_files_per_blob=14_000,
            max_peak_bytes=6 * runner.GIB,
        )
        self.assertFalse(result["passed"])
        self.assertFalse(result["checks"]["cgroup_oom_evidence_complete"])
        self.assertFalse(result["checks"]["docker_state_complete"])

    def test_architecture_and_provenance_checks_are_explicit(self):
        self.assertEqual(runner.normalize_architecture("x86_64"), "amd64")
        self.assertEqual(runner.normalize_architecture("aarch64"), "arm64")
        expected = {key: "same" for key in runner.PROVENANCE_STABILITY_KEYS}
        current = dict(expected)
        self.assertTrue(runner.provenance_stability(expected, current, "test")["ok"])
        current["workspace_sha256"] = "changed"
        check = runner.provenance_stability(expected, current, "test")
        self.assertFalse(check["ok"])
        self.assertEqual(check["changed_keys"], ["workspace_sha256"])

    def test_helper_context_excludes_rust_target(self):
        ignore = (HERE / "Dockerfile.harness.dockerignore").read_text()
        self.assertIn("bitmagnet-rs/*", ignore)
        self.assertIn("!bitmagnet-rs/proto/**", ignore)
        self.assertNotIn("!bitmagnet-rs/target", ignore)

    def test_minimal_schema_has_every_integration_boundary(self):
        schema = (HERE / "schema.sql").read_text()
        for fragment in (
            "CREATE TABLE torrents (",
            "files_data bytea",
            "CREATE TABLE torrent_contents (",
            "CREATE TABLE torrent_file_summary (",
            "CREATE TABLE torrents_torrent_sources (",
            "CREATE TABLE goose_db_version (",
            "CREATE FUNCTION rss_refine_barrier_wait()",
            "ALTER TABLE torrent_contents FORCE ROW LEVEL SECURITY",
            "TO bitmagnet_rss_app",
        ):
            self.assertIn(fragment, schema)


if __name__ == "__main__":
    unittest.main()
