from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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

    def test_docker_builder_backend_is_explicit_and_overrides_ambient_env(self):
        parser = runner.argument_parser()
        buildkit_args = parser.parse_args([])
        self.assertEqual(buildkit_args.graphql_docker_builder, "buildkit")
        with mock.patch.dict(runner.os.environ, {"DOCKER_BUILDKIT": "0"}):
            buildkit = runner.DockerHarness(
                buildkit_args, "session", HERE, "amd64"
            )
        self.assertEqual(
            buildkit.graphql_build_environment["DOCKER_BUILDKIT"], "1"
        )
        self.assertEqual(
            buildkit.helper_build_environment["DOCKER_BUILDKIT"], "1"
        )

        legacy_args = parser.parse_args(["--graphql-docker-builder", "legacy"])
        runner.validate_arguments(legacy_args)
        legacy = runner.DockerHarness(legacy_args, "session", HERE, "amd64")
        self.assertEqual(
            legacy.graphql_build_environment["DOCKER_BUILDKIT"], "0"
        )
        self.assertEqual(legacy.helper_build_environment["DOCKER_BUILDKIT"], "1")
        self.assertEqual(
            runner.docker_builders(legacy_args),
            {
                "graphql": {"backend": "legacy", "DOCKER_BUILDKIT": "0"},
                "helper": {"backend": "buildkit", "DOCKER_BUILDKIT": "1"},
            },
        )

    def test_each_source_build_uses_its_explicit_builder_environment(self):
        args = runner.argument_parser().parse_args(
            ["--graphql-docker-builder", "legacy"]
        )
        harness = runner.DockerHarness(args, "session", HERE, "amd64")
        calls = []

        def record(*parts, **kwargs):
            calls.append((parts, kwargs))
            return runner.subprocess.CompletedProcess(parts, 0, b"", b"")

        harness.docker = record
        harness.build_images()
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(parts[0] == "build" for parts, _ in calls))
        self.assertIn("--force-rm", calls[0][0])
        self.assertNotIn("--force-rm", calls[1][0])
        self.assertEqual(
            [kwargs["environment"]["DOCKER_BUILDKIT"] for _, kwargs in calls],
            ["0", "1"],
        )

    def test_evidence_commands_use_docker_volume_not_host_bind(self):
        args = runner.argument_parser().parse_args(
            ["--profile", "smoke", "--repeat", "1"]
        )
        harness = runner.DockerHarness(args, "session", HERE, "amd64")
        harness.helper_image_id = "sha256:helper"
        harness.graphql_image_id = "sha256:graphql"
        volume = "fixture-evidence"
        mount = f"type=volume,src={volume},dst=/evidence"
        commands = (
            harness.mock_run_command("mock", "events.jsonl", volume),
            harness.graphql_run_command("graphql", volume, "watcher"),
        )
        for command in commands:
            self.assertIn(mount, command)
            self.assertFalse(any(part.startswith("type=bind") for part in command))
        self.assertNotIn("type=bind", (HERE / "run.py").read_text())

    def test_evidence_volume_is_initialized_and_copied(self):
        args = runner.argument_parser().parse_args(
            ["--profile", "smoke", "--repeat", "1"]
        )
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "case"
            destination.mkdir()
            harness = runner.DockerHarness(args, "session", HERE, "amd64")
            harness.helper_image_id = "sha256:helper"
            calls = []

            def record(*parts, **kwargs):
                calls.append((parts, kwargs))
                return runner.subprocess.CompletedProcess(parts, 0, b"", b"")

            harness.docker = record
            harness.volume_state = lambda name: {"Missing": True}
            harness.container_state = lambda name: {"Running": True}
            store = harness.create_evidence_store("accepted-minimal-r1", destination)
            harness.collect_evidence(store)

        operations = [parts[:2] for parts, _ in calls]
        self.assertEqual(operations[0], ("volume", "create"))
        self.assertEqual(operations[1][0], "run")
        self.assertEqual(operations[2][0], "exec")
        self.assertEqual(operations[3][0], "cp")
        carrier_command = calls[1][0]
        self.assertIn(harness.evidence_mount(store.volume), carrier_command)
        self.assertIn("os.chmod('/evidence', 0o777)", carrier_command[-1])
        self.assertEqual(
            calls[3][0],
            ("cp", f"{store.carrier}:/evidence/.", str(store.destination)),
        )
        self.assertTrue(store.collected)

    def test_evidence_volume_rejects_preexisting_name(self):
        args = runner.argument_parser().parse_args(
            ["--profile", "smoke", "--repeat", "1"]
        )
        harness = runner.DockerHarness(args, "session", HERE, "amd64")
        harness.helper_image_id = "sha256:helper"
        harness.volume_state = lambda name: {"Name": name}
        with self.assertRaisesRegex(
            runner.HarnessError, "Docker evidence volume already exists"
        ):
            harness.create_evidence_store("accepted-minimal-r1", HERE)
        self.assertEqual(harness.volumes, set())
        self.assertEqual(harness.evidence_stores, {})

    def test_cleanup_copies_before_removing_case_resources(self):
        harness = object.__new__(runner.DockerHarness)
        harness.args = runner.argparse.Namespace(keep=False)
        harness.containers = {"fixture-carrier", "fixture-graphql"}
        harness.volumes = {"fixture-volume"}
        harness.network_created = False
        store = runner.EvidenceStore(
            "fixture-volume", "fixture-carrier", HERE / "evidence" / "fixture"
        )
        harness.evidence_stores = {store.volume: store}
        actions = []

        def collect(value):
            actions.append(f"copy:{value.volume}")
            value.collected = True

        def remove_container(name, strict=True):
            actions.append(f"container:{name}")
            harness.containers.discard(name)
            return True

        def remove_volume(name, strict=True):
            actions.append(f"volume:{name}")
            harness.volumes.discard(name)
            harness.evidence_stores.pop(name, None)
            return True

        harness.collect_evidence = collect
        harness.remove_container = remove_container
        harness.remove_volume = remove_volume
        cleanup = harness.cleanup()
        self.assertTrue(cleanup["ok"])
        self.assertEqual(actions[0], "copy:fixture-volume")
        self.assertLess(
            actions.index("copy:fixture-volume"), actions.index("volume:fixture-volume")
        )
        self.assertTrue(
            all(
                actions.index("copy:fixture-volume") < actions.index(action)
                for action in actions
                if action.startswith("container:")
            )
        )

    def test_cleanup_copy_failure_is_terminal_but_resources_are_removed(self):
        harness = object.__new__(runner.DockerHarness)
        harness.args = runner.argparse.Namespace(keep=False)
        harness.containers = {"fixture-carrier"}
        harness.volumes = {"fixture-volume"}
        harness.network_created = False
        store = runner.EvidenceStore(
            "fixture-volume", "fixture-carrier", HERE / "evidence" / "fixture"
        )
        harness.evidence_stores = {store.volume: store}

        def fail_copy(value):
            raise runner.HarnessError(f"copy failed for {value.volume}")

        def remove_container(name, strict=True):
            harness.containers.discard(name)
            return True

        def remove_volume(name, strict=True):
            harness.volumes.discard(name)
            harness.evidence_stores.pop(name, None)
            return True

        harness.collect_evidence = fail_copy
        harness.remove_container = remove_container
        harness.remove_volume = remove_volume
        cleanup = harness.cleanup()
        self.assertFalse(cleanup["ok"])
        self.assertEqual(cleanup["evidence_copy_failures"], ["fixture-volume"])
        self.assertEqual(cleanup["remaining_containers"], [])
        self.assertEqual(cleanup["remaining_volumes"], [])
        self.assertIn("copy failed", cleanup["errors"][0])

    def test_gate_profile_rejects_admission_downgrades(self):
        parser = runner.argument_parser()
        runner.validate_arguments(parser.parse_args([]))
        rejected = (
            ["--repeat", "1"],
            ["--max-peak-bytes", str(runner.DEFAULT_GATE_PEAK_BYTES + 1)],
            ["--graphql-image", "unlinked:latest"],
            ["--helper-image", "unlinked:latest"],
            ["--postgres-image", "postgres:latest"],
            ["--keep"],
        )
        for options in rejected:
            with self.subTest(options=options), self.assertRaises(runner.HarnessError):
                runner.validate_arguments(parser.parse_args(options))

        smoke = parser.parse_args(
            [
                "--profile",
                "smoke",
                "--repeat",
                "1",
                "--max-peak-bytes",
                str(7 * runner.GIB),
                "--graphql-image",
                "debug-graphql:latest",
                "--helper-image",
                "debug-helper:latest",
                "--postgres-image",
                "postgres:latest",
                "--keep",
            ]
        )
        runner.validate_arguments(smoke)

    def test_cleanup_rejects_unknown_docker_absence(self):
        missing = runner.subprocess.CompletedProcess(
            ["docker", "inspect"],
            1,
            b"",
            b"Error response from daemon: No such container: fixture\n",
        )
        unavailable = runner.subprocess.CompletedProcess(
            ["docker", "inspect"],
            125,
            b"",
            b"Cannot connect to the Docker daemon\n",
        )
        self.assertTrue(
            runner.docker_object_missing(missing, kind="container", name="fixture")
        )
        self.assertFalse(
            runner.docker_object_missing(
                unavailable, kind="container", name="fixture"
            )
        )

        harness = object.__new__(runner.DockerHarness)
        harness.args = runner.argparse.Namespace(keep=False)
        harness.containers = {"fixture"}
        harness.volumes = {"fixture-volume"}
        harness.evidence_stores = {}
        harness.network_created = True
        harness.network = "fixture-net"
        harness.docker = lambda *args, **kwargs: unavailable
        cleanup = harness.cleanup()
        self.assertFalse(cleanup["ok"])
        self.assertEqual(cleanup["remaining_containers"], ["fixture"])
        self.assertEqual(cleanup["remaining_volumes"], ["fixture-volume"])
        self.assertTrue(cleanup["network_remaining"])
        self.assertEqual(len(cleanup["errors"]), 3)

        missing_volume = runner.subprocess.CompletedProcess(
            ["docker", "volume", "inspect"],
            1,
            b"",
            b"Error response from daemon: get fixture-volume: no such volume\n",
        )
        self.assertTrue(
            runner.docker_object_missing(
                missing_volume, kind="volume", name="fixture-volume"
            )
        )

    def test_jsonl_writer_rejects_existing_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "evidence.jsonl"
            output.write_text("sentinel\n")
            with self.assertRaisesRegex(
                runner.HarnessError, "evidence output already exists"
            ):
                runner.JsonlWriter(output)
            self.assertEqual(output.read_text(), "sentinel\n")

            fresh = Path(temporary) / "fresh.jsonl"
            writer = runner.JsonlWriter(fresh)
            writer.close()
            self.assertTrue(fresh.exists())

    def test_ignored_evidence_does_not_change_workspace_digest(self):
        evidence_dir = HERE / "evidence"
        directory_existed = evidence_dir.exists()
        evidence_dir.mkdir(exist_ok=True)
        ignored = evidence_dir / f"digest-test-{runner.uuid.uuid4().hex}.jsonl"
        before = runner.workspace_digest()
        try:
            ignored.write_text('{"kind":"test"}\n')
            self.assertEqual(runner.workspace_digest(), before)
        finally:
            ignored.unlink(missing_ok=True)
            if not directory_existed:
                evidence_dir.rmdir()

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
