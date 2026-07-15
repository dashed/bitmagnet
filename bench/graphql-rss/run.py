#!/usr/bin/env python3
"""Run the isolated four-concurrent GraphQL/sqlx cgroup RSS gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

MIB = 1024 * 1024
GIB = 1024 * MIB
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]

PROFILE_LIMITS = {
    "gate": {
        "max_decompressed_bytes": 64 * MIB,
        "decoded_byte_budget": 128 * MIB,
        "retained_byte_budget": 64 * MIB,
        "accepted_files_per_blob": 14_000,
        "accepted_path_payload_bytes": 1_000,
        "adversarial_extra_bytes": 8 * 1024,
        "min_retained_fill_percent": 80,
    },
    "smoke": {
        "max_decompressed_bytes": 2 * MIB,
        "decoded_byte_budget": 4 * MIB,
        "retained_byte_budget": 2 * MIB,
        "accepted_files_per_blob": 450,
        "accepted_path_payload_bytes": 1_000,
        "adversarial_extra_bytes": 8 * 1024,
        "min_retained_fill_percent": 80,
    },
}


class HarnessError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def run_command(
    command: list[str],
    *,
    cwd: Path = REPO_ROOT,
    input_bytes: bytes | None = None,
    timeout: float | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(
        command,
        cwd=cwd,
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        stdout = result.stdout.decode(errors="replace")[-4000:]
        stderr = result.stderr.decode(errors="replace")[-8000:]
        rendered = " ".join(shlex.quote(part) for part in command)
        raise HarnessError(
            f"command failed ({result.returncode}): {rendered}\n"
            f"stdout tail:\n{stdout}\nstderr tail:\n{stderr}"
        )
    return result


def git_text(*args: str) -> str:
    return run_command(["git", *args]).stdout.decode(errors="replace").strip()


def workspace_digest() -> tuple[str, int]:
    listed = run_command(
        ["git", "ls-files", "-co", "--exclude-standard", "-z"]
    ).stdout.split(b"\0")
    paths = sorted(path for path in listed if path)
    digest = hashlib.sha256()
    count = 0
    for raw_path in paths:
        relative = raw_path.decode()
        path = REPO_ROOT / relative
        if not path.is_file():
            continue
        digest.update(len(raw_path).to_bytes(8, "big"))
        digest.update(raw_path)
        data = path.read_bytes()
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
        count += 1
    return digest.hexdigest(), count


def repository_provenance() -> dict[str, Any]:
    digest, files = workspace_digest()
    status = git_text("status", "--porcelain=v1", "--untracked-files=all")
    diff = run_command(["git", "diff", "--binary", "HEAD"]).stdout
    migrations = sorted((REPO_ROOT / "migrations").glob("*.sql"))
    migration_digest = hashlib.sha256()
    for path in migrations:
        migration_digest.update(path.name.encode())
        migration_digest.update(path.read_bytes())
    return {
        "commit": git_text("rev-parse", "HEAD"),
        "branch": git_text("branch", "--show-current") or None,
        "describe": git_text("describe", "--always", "--dirty", "--tags"),
        "status_porcelain": status.splitlines(),
        "tracked_diff_sha256": sha256_bytes(diff),
        "workspace_sha256": digest,
        "workspace_file_count": files,
        "graphql_dockerfile_sha256": sha256_file(
            REPO_ROOT / "bitmagnet-rs/docker/Dockerfile.graphql"
        ),
        "rust_cargo_lock_sha256": sha256_file(REPO_ROOT / "bitmagnet-rs/Cargo.lock"),
        "rust_toolchain_sha256": sha256_file(
            REPO_ROOT / "bitmagnet-rs/rust-toolchain.toml"
        ),
        "migrations_sha256": migration_digest.hexdigest(),
        "harness_schema_sha256": sha256_file(SCRIPT_DIR / "schema.sql"),
        "harness_runner_sha256": sha256_file(Path(__file__)),
        "harness_helper_sha256": sha256_file(SCRIPT_DIR / "helper.py"),
    }


PROVENANCE_STABILITY_KEYS = (
    "commit",
    "branch",
    "describe",
    "status_porcelain",
    "tracked_diff_sha256",
    "workspace_sha256",
    "workspace_file_count",
    "graphql_dockerfile_sha256",
    "rust_cargo_lock_sha256",
    "rust_toolchain_sha256",
    "migrations_sha256",
    "harness_schema_sha256",
    "harness_runner_sha256",
    "harness_helper_sha256",
)


def provenance_stability(
    expected: dict[str, Any], current: dict[str, Any], stage: str
) -> dict[str, Any]:
    changed = [
        key for key in PROVENANCE_STABILITY_KEYS if expected.get(key) != current.get(key)
    ]
    return {
        "stage": stage,
        "ok": not changed,
        "changed_keys": changed,
        "current": current,
    }


def normalize_architecture(value: Any) -> str:
    architecture = str(value or "").lower()
    if architecture in {"amd64", "x86_64"}:
        return "amd64"
    if architecture in {"arm64", "aarch64"}:
        return "arm64"
    return architecture


def docker_info(runtime: str, *, require_amd64: bool) -> dict[str, Any]:
    if shutil.which(runtime) is None:
        raise HarnessError(f"container runtime {runtime!r} is not on PATH")
    result = run_command(
        [runtime, "info", "--format", "{{json .}}"], timeout=30
    )
    try:
        info = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise HarnessError(f"could not decode {runtime} info JSON: {error}") from error

    failures = []
    for server_error in info.get("ServerErrors") or []:
        failures.append(f"Docker server error: {server_error}")
    if str(info.get("OSType", "")).lower() != "linux":
        failures.append(f"server OSType is {info.get('OSType')!r}, not linux")
    if str(info.get("CgroupVersion", "")) != "2":
        failures.append(
            f"server cgroup version is {info.get('CgroupVersion')!r}, not v2"
        )
    if int(info.get("NCPU") or 0) < 4:
        failures.append(f"server exposes {info.get('NCPU')!r} CPUs; at least 4 required")
    architecture = normalize_architecture(info.get("Architecture"))
    if require_amd64 and architecture != "amd64":
        failures.append(
            f"server architecture is {info.get('Architecture')!r}, not production amd64"
        )
    # The measured service gets 8 GiB. PostgreSQL, the mock, and the response
    # driver must not force host-level pressure into that cgroup measurement.
    if int(info.get("MemTotal") or 0) < 12 * GIB:
        failures.append(
            f"server exposes {info.get('MemTotal')!r} bytes; 12 GiB required "
            "for an 8 GiB service plus isolated dependencies"
        )
    if failures:
        raise HarnessError("unsupported container runtime:\n- " + "\n- ".join(failures))
    return info


def parse_memory_events(text: str) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            values[fields[0]] = int(fields[1])
        except ValueError:
            continue
    return values


def read_cgroup_snapshot(directory: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    scalar_files = ("memory.current", "memory.peak", "memory.swap.peak")
    for name in scalar_files:
        path = directory / name
        if path.exists():
            try:
                result[name.replace(".", "_")] = int(path.read_text().strip())
            except ValueError:
                result[name.replace(".", "_")] = None
    for name in ("memory.events.local", "memory.events", "memory.stat"):
        path = directory / name
        if path.exists():
            result[name.replace(".", "_")] = parse_memory_events(path.read_text())
    sampled_at = directory / "sample_time_unix_ns"
    if sampled_at.exists():
        try:
            result["sample_time_unix_ns"] = int(sampled_at.read_text().strip())
        except ValueError:
            result["sample_time_unix_ns"] = None
    return result


def new_mock_events(path: Path, offset: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("rb") as stream:
        stream.seek(offset)
        data = stream.read()
    rows = []
    for line in data.splitlines():
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            rows.append({"event": "invalid_json", "raw": line.decode(errors="replace")})
    return rows


def barrier_evidence(events: list[dict[str, Any]], key: str) -> dict[str, Any]:
    matching = [row for row in events if row.get("key") == key]
    generations = sorted(
        {row.get("generation") for row in matching if row.get("generation") is not None}
    )
    arrivals = [row for row in matching if row.get("event") == "arrive"]
    releases = [row for row in matching if row.get("event") == "release"]
    responses = [row for row in matching if row.get("event") == "respond"]
    timeouts = [row for row in matching if row.get("event") == "timeout"]
    ok = (
        len(generations) == 1
        and len(arrivals) == 4
        and sorted(row.get("arrival") for row in arrivals) == [1, 2, 3, 4]
        and len(releases) == 4
        and len(responses) == 4
        and not timeouts
    )
    return {
        "ok": ok,
        "generations": generations,
        "arrivals": len(arrivals),
        "releases": len(releases),
        "responses": len(responses),
        "timeouts": len(timeouts),
        "events": matching,
    }


def evaluate_run(
    *,
    driver: dict[str, Any],
    barrier: dict[str, Any],
    refine_barrier: dict[str, Any],
    cgroup: dict[str, Any],
    live_state: dict[str, Any],
    state: dict[str, Any],
    scenario: str,
    projection: str,
    accepted_files_per_blob: int,
    max_peak_bytes: int,
) -> dict[str, Any]:
    expected_items = 4 if scenario == "accepted" else 0
    expected_total = expected_items
    expected_files = (
        4 * accepted_files_per_blob
        if scenario == "accepted" and projection == "files"
        else 0
    )
    response_checks = []
    for response in driver.get("responses", []):
        try:
            handler_duration_valid = int(response.get("handler_duration_us") or 0) > 0
        except (TypeError, ValueError):
            handler_duration_valid = False
        response_checks.append(
            response.get("http_status") == 200
            and not response.get("transport_error")
            and not response.get("parse_error")
            and not response.get("graphql_errors")
            and response.get("item_count") == expected_items
            and response.get("file_count") == expected_files
            and response.get("total_count") == expected_total
            and response.get("total_count_is_estimate") is True
            and response.get("has_next_page") is False
            and handler_duration_valid
        )

    local_events = cgroup.get("memory_events_local")
    if not isinstance(local_events, dict):
        local_events = cgroup.get("memory_events")
    required_event_keys = {"oom", "oom_kill", "oom_group_kill"}
    events_complete = isinstance(local_events, dict) and required_event_keys.issubset(
        local_events
    )
    if not isinstance(local_events, dict):
        local_events = {}
    peak = cgroup.get("memory_peak")
    samples = driver.get("metrics_samples", [])
    route_served = prometheus_sample(
        samples, 'bitmagnet_search_pathsearch_route_total{result="served"}'
    )
    retained_capped = prometheus_sample(
        samples, "bitmagnet_search_pathsearch_refine_retained_capped_total"
    )
    checks = {
        "four_responses_valid": len(response_checks) == 4 and all(response_checks),
        "four_client_mock_barrier": bool(barrier.get("ok")),
        "four_request_refine_barrier": bool(refine_barrier.get("ok")),
        "metrics_scrape_valid": driver.get("metrics_error") is None,
        "pathsearch_healthy": any(
            line == "bitmagnet_search_pathsearch_healthy 1"
            for line in samples
        ),
        "four_routes_served": route_served == 4.0,
        "byte_cap_metric_matches_scenario": retained_capped
        == (4.0 if scenario == "adversarial" else 0.0),
        "cgroup_oom_evidence_complete": events_complete,
        "no_cgroup_oom": events_complete
        and int(local_events["oom"]) == 0
        and int(local_events["oom_kill"]) == 0
        and int(local_events["oom_group_kill"]) == 0,
        "docker_state_complete": state.get("OOMKilled") is False
        and state.get("Dead") is False
        and state.get("Status") == "exited"
        and state.get("ExitCode") == 0
        and state.get("Error") in {"", None},
        "container_running_before_stop": live_state.get("Running") is True
        and live_state.get("OOMKilled") is False
        and live_state.get("Dead") is False,
        "docker_not_oom_killed": state.get("OOMKilled") is False,
        "memory_peak_captured": isinstance(peak, int) and peak > 0,
        "memory_peak_within_8gib": isinstance(peak, int) and peak < 8 * GIB,
        "memory_peak_within_gate": isinstance(peak, int) and peak <= max_peak_bytes,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "expected_item_count_per_response": expected_items,
        "expected_file_count_per_response": expected_files,
        "expected_total_count_per_response": expected_total,
        "expected_total_count_is_estimate": True,
        "expected_has_next_page": False,
    }


def prometheus_sample(samples: Iterable[str], metric: str) -> float | None:
    for line in samples:
        try:
            name, value = line.rsplit(None, 1)
        except ValueError:
            continue
        if name != metric:
            continue
        try:
            return float(value)
        except ValueError:
            return None
    return None


class JsonlWriter:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.stream = path.open("a", encoding="utf-8")

    def write(self, value: dict[str, Any]) -> None:
        self.stream.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
        self.stream.flush()
        os.fsync(self.stream.fileno())

    def close(self) -> None:
        self.stream.close()


class DockerHarness:
    def __init__(
        self,
        args: argparse.Namespace,
        session_id: str,
        evidence_dir: Path,
        runtime_architecture: str,
    ):
        self.args = args
        self.runtime = args.runtime
        self.session_id = session_id
        self.prefix = f"bm-rss-{session_id[:10]}"
        self.network = f"{self.prefix}-net"
        self.pg = f"{self.prefix}-pg"
        self.evidence_dir = evidence_dir
        self.containers: set[str] = set()
        self.network_created = False
        self.graphql_image = args.graphql_image or f"{self.prefix}-graphql:local"
        self.helper_image = args.helper_image or f"{self.prefix}-helper:local"
        self.pg_password = uuid.uuid4().hex
        self.runtime_architecture = runtime_architecture
        self.graphql_image_id: str | None = None
        self.helper_image_id: str | None = None
        self.postgres_image_id: str | None = None

    def docker(self, *parts: str, **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        return run_command([self.runtime, *parts], **kwargs)

    def build_images(self) -> None:
        if self.args.graphql_image is None:
            print("building exact Dockerfile.graphql image", file=sys.stderr, flush=True)
            self.docker(
                "build",
                "--file",
                "bitmagnet-rs/docker/Dockerfile.graphql",
                "--tag",
                self.graphql_image,
                "bitmagnet-rs",
                timeout=self.args.build_timeout,
            )
        if self.args.helper_image is None:
            print("building pinned harness helper image", file=sys.stderr, flush=True)
            self.docker(
                "build",
                "--file",
                "bench/graphql-rss/Dockerfile.harness",
                "--tag",
                self.helper_image,
                ".",
                timeout=self.args.build_timeout,
            )

    def image_info(self, image: str) -> dict[str, Any]:
        raw = self.docker("image", "inspect", image).stdout
        rows = json.loads(raw)
        if len(rows) != 1:
            raise HarnessError(f"expected one image inspection row for {image}")
        row = rows[0]
        return {
            "requested": image,
            "id": row.get("Id"),
            "repo_digests": row.get("RepoDigests") or [],
            "created": row.get("Created"),
            "architecture": row.get("Architecture"),
            "os": row.get("Os"),
            "rootfs_layers": (row.get("RootFS") or {}).get("Layers") or [],
        }

    def resolve_images(self) -> dict[str, dict[str, Any]]:
        postgres = self.docker("image", "inspect", self.args.postgres_image, check=False)
        if postgres.returncode != 0:
            self.docker("pull", self.args.postgres_image, timeout=600)
        images = {
            "graphql": self.image_info(self.graphql_image),
            "helper": self.image_info(self.helper_image),
            "postgres": self.image_info(self.args.postgres_image),
        }
        self.graphql_image_id = images["graphql"]["id"]
        self.helper_image_id = images["helper"]["id"]
        self.postgres_image_id = images["postgres"]["id"]
        if not all(
            isinstance(image_id, str) and image_id.startswith("sha256:")
            for image_id in (
                self.graphql_image_id,
                self.helper_image_id,
                self.postgres_image_id,
            )
        ):
            raise HarnessError(f"images did not resolve to immutable IDs: {images}")
        return images

    def remove_container(self, name: str, *, strict: bool = True) -> bool:
        self.docker("rm", "--force", name, check=False)
        removed = bool(self.container_state(name).get("Missing"))
        if removed:
            self.containers.discard(name)
            return True
        if strict:
            raise HarnessError(f"container {name} still exists after forced removal")
        return False

    def run_helper(
        self,
        suffix: str,
        command: list[str],
        *,
        cpus: str | None = None,
        memory: str | None = None,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        if self.helper_image_id is None:
            raise HarnessError("helper image was not resolved")
        name = f"{self.prefix}-{suffix}-{uuid.uuid4().hex[:6]}"
        parts = ["run", "--name", name, "--network", self.network]
        if cpus is not None:
            parts.extend(["--cpus", cpus])
        if memory is not None:
            parts.extend(["--memory", memory])
        parts.extend([self.helper_image_id, *command])
        self.containers.add(name)
        try:
            result = self.docker(*parts, timeout=timeout)
        except BaseException:
            self.remove_container(name, strict=False)
            raise
        self.remove_container(name)
        return result

    def start_dependencies(self) -> dict[str, Any]:
        if self.postgres_image_id is None:
            raise HarnessError("PostgreSQL image was not resolved")
        self.docker("network", "create", self.network)
        self.network_created = True

        self.containers.add(self.pg)
        self.docker(
            "run",
            "--detach",
            "--name",
            self.pg,
            "--network",
            self.network,
            "--network-alias",
            "postgres",
            "--cpus",
            "1",
            "--memory",
            "1g",
            "--shm-size",
            "256m",
            "--tmpfs",
            "/var/lib/postgresql/data:rw,size=768m",
            "--env",
            "POSTGRES_DB=bitmagnet",
            "--env",
            "POSTGRES_USER=bitmagnet",
            "--env",
            f"POSTGRES_PASSWORD={self.pg_password}",
            self.postgres_image_id,
        )
        for _ in range(120):
            ready = self.docker(
                "exec",
                self.pg,
                "pg_isready",
                "-U",
                "bitmagnet",
                "-d",
                "bitmagnet",
                check=False,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise HarnessError("disposable PostgreSQL did not become ready")

        self.docker(
            "exec",
            "--interactive",
            self.pg,
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "bitmagnet",
            "-d",
            "bitmagnet",
            input_bytes=(SCRIPT_DIR / "schema.sql").read_bytes(),
            timeout=120,
        )
        dsn = f"postgresql://bitmagnet:{self.pg_password}@postgres:5432/bitmagnet"
        seeded = self.run_helper(
            "seed",
            [
                "seed",
                "--dsn",
                dsn,
                "--profile",
                self.args.profile,
            ],
            memory="2g",
            timeout=300,
        )
        seed_lines = [line for line in seeded.stdout.decode().splitlines() if line.strip()]
        seed_summary = json.loads(seed_lines[-1])

        return {"seed": seed_summary}

    def start_mock(self, case_id: str) -> tuple[str, Path]:
        if self.helper_image_id is None:
            raise HarnessError("helper image was not resolved")
        name = f"{self.prefix}-mock-{case_id}"
        events = self.evidence_dir / f"{name}-events.jsonl"
        if events.exists():
            raise HarnessError(f"mock evidence path already exists: {events}")
        self.containers.add(name)
        self.docker(
            "run",
            "--detach",
            "--name",
            name,
            "--network",
            self.network,
            "--network-alias",
            "pathsearch-mock",
            "--cpus",
            "1",
            "--memory",
            "256m",
            "--mount",
            f"type=bind,src={self.evidence_dir},dst=/evidence",
            self.helper_image_id,
            "mock",
            "--events",
            f"/evidence/{events.name}",
            "--barrier",
            "4",
            "--barrier-timeout",
            "60",
        )
        for _ in range(120):
            state = self.container_state(name)
            if state.get("Running") and events.exists():
                break
            time.sleep(0.25)
        else:
            raise HarnessError("gRPC barrier mock did not become ready")
        return name, events

    def container_state(self, name: str) -> dict[str, Any]:
        result = self.docker(
            "inspect", "--format", "{{json .State}}", name, check=False
        )
        if result.returncode != 0:
            return {"Missing": True}
        return json.loads(result.stdout)

    def wait_url(self, url: str, contains: str | None = None) -> None:
        command = [
            "wait",
            "--url",
            url,
            "--timeout",
            "60",
        ]
        if contains is not None:
            command.extend(["--contains", contains])
        self.run_helper("wait", command, memory="256m", timeout=75)

    def graphql_config(self) -> dict[str, str]:
        limits = PROFILE_LIMITS[self.args.profile]
        return {
            "BITMAGNET_POSTGRES_MAX_CONNECTIONS": "16",
            "BITMAGNET_METRICS_ADDR": "0.0.0.0:9090",
            "BITMAGNET_VERSION": f"graphql-rss-{self.session_id}",
            "SEARCH_FILE_SEARCH_ENABLED": "false",
            "SEARCH_PATHSEARCH_ENABLED": "true",
            "SEARCH_PATH_TYPEAHEAD_ENABLED": "true",
            "SEARCH_PATH_COLLAPSE_ENABLED": "false",
            "SEARCH_PATHSEARCH_ADDRESS": "pathsearch-mock:50053",
            "SEARCH_PATHSEARCH_TIMEOUT": "65s",
            "SEARCH_PATHSEARCH_HEALTH_INTERVAL": "1s",
            "SEARCH_PATHSEARCH_MAX_WATERMARK_LAG": "0",
            "SEARCH_PATHSEARCH_MIN_QUERY_LENGTH": "3",
            "SEARCH_PATHSEARCH_OVERSAMPLE": "1",
            "SEARCH_PATHSEARCH_MAX_CANDIDATES": "16",
            "SEARCH_PATHSEARCH_MAX_DECODE_CANDIDATES": "16",
            "SEARCH_PATHSEARCH_MAX_REFINE_FILES": "100000",
            "SEARCH_PATHSEARCH_REFINE_FILE_BUDGET": "300000",
            "SEARCH_PATHSEARCH_MAX_CHUNK_TORRENTS": "16",
            "SEARCH_PATHSEARCH_RETAINED_FILE_BUDGET": "1000000",
            "SEARCH_PATHSEARCH_MAX_REFINE_DECOMPRESSED_BYTES": str(
                limits["max_decompressed_bytes"]
            ),
            "SEARCH_PATHSEARCH_REFINE_DECODED_BYTE_BUDGET": str(
                limits["decoded_byte_budget"]
            ),
            "SEARCH_PATHSEARCH_RETAINED_BYTE_BUDGET": str(
                limits["retained_byte_budget"]
            ),
            "SEARCH_PATHSEARCH_ROUTE_TIMEOUT": "170s",
            "SEARCH_PATHSEARCH_MAX_CONCURRENT_REFINES": "4",
            "SEARCH_PATHSEARCH_SLOT_WAIT": "10s",
            "SEARCH_FEATURES_POPULARITY_SORT_DEFAULT": "false",
            "RUST_LOG": "info",
        }

    def graphql_environment(self) -> list[str]:
        values = {
            "BITMAGNET_POSTGRES_DSN": (
                "postgresql://bitmagnet_rss_app:rss-gate-only@postgres:5432/bitmagnet"
            ),
            **self.graphql_config(),
        }
        environment: list[str] = []
        for key, value in values.items():
            environment.extend(["--env", f"{key}={value}"])
        return environment

    def reset_refine_barrier(self) -> int:
        result = self.docker(
            "exec",
            self.pg,
            "psql",
            "-qAtX",
            "-U",
            "bitmagnet",
            "-d",
            "bitmagnet",
            "-c",
            "SELECT nextval('rss_refine_barrier_generation'); "
            "SELECT setval('rss_refine_barrier_arrivals', 1, false);",
        )
        values = [line.strip() for line in result.stdout.decode().splitlines() if line.strip()]
        if len(values) != 2:
            raise HarnessError(f"unexpected refine-barrier reset output: {values!r}")
        return int(values[0])

    def refine_barrier_evidence(self, expected_generation: int) -> dict[str, Any]:
        result = self.docker(
            "exec",
            self.pg,
            "psql",
            "-qAtX",
            "--field-separator=,",
            "-U",
            "bitmagnet",
            "-d",
            "bitmagnet",
            "-c",
            "SELECT generation.last_value, arrivals.last_value, arrivals.is_called "
            "FROM rss_refine_barrier_generation AS generation "
            "CROSS JOIN rss_refine_barrier_arrivals AS arrivals;",
        )
        fields = result.stdout.decode().strip().split(",")
        if len(fields) != 3:
            return {"ok": False, "parse_error": fields}
        generation, arrivals, is_called = int(fields[0]), int(fields[1]), fields[2] == "t"
        return {
            "ok": generation == expected_generation and is_called and arrivals == 4,
            "expected_generation": expected_generation,
            "generation": generation,
            "arrivals": arrivals if is_called else 0,
            "is_called": is_called,
        }

    def run_case(
        self,
        *,
        scenario: str,
        projection: str,
        repeat: int,
    ) -> dict[str, Any]:
        if self.graphql_image_id is None:
            raise HarnessError("GraphQL image was not resolved")
        case_id = f"{scenario}-{projection}-r{repeat}"
        name = f"{self.prefix}-{case_id}"
        run_dir = self.evidence_dir / f"{self.prefix}-{case_id}-cgroup"
        run_dir.mkdir(parents=True, exist_ok=False)
        run_dir.chmod(0o777)
        mock_name, events_path = self.start_mock(case_id)
        refine_generation = self.reset_refine_barrier()

        watcher = r"""
snapshot() {
  for f in memory.current memory.peak memory.events memory.events.local memory.stat memory.swap.peak; do
    if [ -r "/sys/fs/cgroup/$f" ]; then
      cat "/sys/fs/cgroup/$f" > "/evidence/$f.tmp" && mv "/evidence/$f.tmp" "/evidence/$f"
    fi
  done
  date +%s%N > /evidence/sample_time_unix_ns.tmp && mv /evidence/sample_time_unix_ns.tmp /evidence/sample_time_unix_ns
}
app_pid=0
terminate() {
  if [ "$app_pid" -gt 0 ]; then kill -TERM "$app_pid" 2>/dev/null || true; fi
}
trap terminate TERM INT
/usr/local/bin/bitmagnet-graphql &
app_pid=$!
(
  while kill -0 "$app_pid" 2>/dev/null; do snapshot; sleep 0.10; done
  snapshot
) &
watcher_pid=$!
set +e
wait "$app_pid"
app_status=$?
wait "$watcher_pid" 2>/dev/null || true
snapshot
exit "$app_status"
""".strip()

        self.containers.add(name)
        self.docker(
            "run",
            "--detach",
            "--name",
            name,
            "--network",
            self.network,
            "--cpus",
            "4",
            "--memory",
            "8g",
            "--memory-swap",
            "8g",
            "--pids-limit",
            "512",
            "--mount",
            f"type=bind,src={run_dir},dst=/evidence",
            *self.graphql_environment(),
            "--entrypoint",
            "/bin/sh",
            self.graphql_image_id,
            "-c",
            watcher,
        )
        inspected = json.loads(self.docker("inspect", name).stdout)[0]
        host_config = inspected.get("HostConfig") or {}
        actual_contract = {
            "image_id": inspected.get("Image"),
            "image_architecture": normalize_architecture(
                self.image_info(self.graphql_image_id)["architecture"]
            ),
            "nano_cpus": host_config.get("NanoCpus"),
            "memory_bytes": host_config.get("Memory"),
            "memory_swap_bytes": host_config.get("MemorySwap"),
            "pids_limit": host_config.get("PidsLimit"),
        }

        driver: dict[str, Any]
        drive_error = None
        try:
            self.wait_url(f"http://{name}:3337/status")
            self.wait_url(
                f"http://{name}:9090/metrics",
                "bitmagnet_search_pathsearch_healthy 1",
            )
            driven = self.run_helper(
                "drive",
                [
                    "drive",
                    "--url",
                    f"http://{name}:3337/graphql",
                    "--metrics-url",
                    f"http://{name}:9090/metrics",
                    "--query",
                    scenario,
                    "--projection",
                    projection,
                    "--clients",
                    "4",
                    "--timeout",
                    "180",
                ],
                cpus="2",
                memory="2g",
                timeout=210,
            )
            lines = [line for line in driven.stdout.decode().splitlines() if line.strip()]
            driver = json.loads(lines[-1])
        except Exception as error:
            drive_error = f"{type(error).__name__}: {error}"
            driver = {"responses": [], "metrics_error": drive_error}

        time.sleep(0.5)
        live_state = self.container_state(name)
        cgroup = read_cgroup_snapshot(run_dir)
        if live_state.get("Running"):
            self.docker("stop", "--time", "15", name, check=False, timeout=30)
        state = self.container_state(name)
        # The watcher can capture a last sample after our first read.
        cgroup = read_cgroup_snapshot(run_dir) or cgroup
        logged = self.docker("logs", name, check=False)
        logs = logged.stdout + logged.stderr
        self.remove_container(name)

        events = new_mock_events(events_path, 0)
        barrier = barrier_evidence(events, scenario)
        refine_barrier = self.refine_barrier_evidence(refine_generation)
        self.remove_container(mock_name)
        evaluation = evaluate_run(
            driver=driver,
            barrier=barrier,
            refine_barrier=refine_barrier,
            cgroup=cgroup,
            live_state=live_state,
            state=state,
            scenario=scenario,
            projection=projection,
            accepted_files_per_blob=PROFILE_LIMITS[self.args.profile][
                "accepted_files_per_blob"
            ],
            max_peak_bytes=self.args.max_peak_bytes,
        )
        evaluation["checks"]["actual_resource_contract"] = actual_contract == {
            "image_id": self.graphql_image_id,
            "image_architecture": self.runtime_architecture,
            "nano_cpus": 4_000_000_000,
            "memory_bytes": 8 * GIB,
            "memory_swap_bytes": 8 * GIB,
            "pids_limit": 512,
        }
        evaluation["passed"] = all(evaluation["checks"].values())
        return {
            "kind": "run",
            "recorded_at": utc_now(),
            "session_id": self.session_id,
            "case_id": case_id,
            "scenario": scenario,
            "projection": projection,
            "repeat": repeat,
            "resource_limits": {
                "cpus": 4,
                "memory_bytes": 8 * GIB,
                "memory_swap_bytes": 8 * GIB,
                "acceptance_peak_bytes": self.args.max_peak_bytes,
            },
            "actual_container_contract": actual_contract,
            "driver": driver,
            "driver_error": drive_error,
            "mock_barrier": barrier,
            "refine_barrier": refine_barrier,
            "cgroup_v2": cgroup,
            "container_state_before_stop": live_state,
            "container_state_after_stop": state,
            "container_log_sha256": sha256_bytes(logs),
            "container_log_tail": logs.decode(errors="replace").splitlines()[-200:],
            "evaluation": evaluation,
        }

    def cleanup(self) -> dict[str, Any]:
        if self.args.keep:
            print(
                f"--keep selected; preserving containers/network with prefix {self.prefix}",
                file=sys.stderr,
            )
            return {
                "ok": True,
                "preserved": True,
                "remaining_containers": sorted(self.containers),
                "network_remaining": self.network_created,
                "errors": [],
            }
        errors = []
        for name in sorted(tuple(self.containers), reverse=True):
            try:
                if not self.remove_container(name, strict=False):
                    errors.append(f"container still exists: {name}")
            except Exception as error:
                errors.append(f"container cleanup {name}: {type(error).__name__}: {error}")
        if self.network_created:
            try:
                self.docker("network", "rm", self.network, check=False)
                present = self.docker(
                    "network", "inspect", self.network, check=False
                ).returncode == 0
                if present:
                    errors.append(f"network still exists: {self.network}")
                else:
                    self.network_created = False
            except Exception as error:
                errors.append(
                    f"network cleanup {self.network}: {type(error).__name__}: {error}"
                )
        return {
            "ok": not errors,
            "preserved": False,
            "remaining_containers": sorted(self.containers),
            "network_remaining": self.network_created,
            "errors": errors,
        }


def default_output() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return SCRIPT_DIR / "evidence" / f"graphql-rss-{stamp}.jsonl"


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", default="docker", choices=("docker",))
    parser.add_argument("--profile", choices=sorted(PROFILE_LIMITS), default="gate")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--max-peak-bytes", type=int, default=6 * GIB)
    parser.add_argument("--graphql-image")
    parser.add_argument("--helper-image")
    parser.add_argument("--postgres-image", default="postgres:17.5-bookworm")
    parser.add_argument("--build-timeout", type=float, default=3600)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--preflight-only", action="store_true")
    return parser


def main() -> int:
    args = argument_parser().parse_args()
    if args.repeat < 1:
        raise HarnessError("--repeat must be at least 1")
    if not 0 < args.max_peak_bytes < 8 * GIB:
        raise HarnessError("--max-peak-bytes must be greater than zero and below 8 GiB")

    try:
        info = docker_info(args.runtime, require_amd64=args.profile == "gate")
    except Exception as error:
        failure = {
            "kind": "unsupported_runtime",
            "recorded_at": utc_now(),
            "status": "unsupported",
            "runtime": args.runtime,
            "error": f"{type(error).__name__}: {error}",
        }
        if args.preflight_only:
            print(json.dumps(failure, indent=2, sort_keys=True))
        else:
            output = (args.output or default_output()).resolve()
            writer = JsonlWriter(output)
            writer.write(failure)
            writer.close()
            print(f"evidence: {output}", file=sys.stderr)
        return 2
    provenance = repository_provenance()
    if args.preflight_only:
        print(
            json.dumps(
                {
                    "status": "supported",
                    "runtime": {
                        key: info.get(key)
                        for key in (
                            "ServerVersion",
                            "OSType",
                            "OperatingSystem",
                            "Architecture",
                            "CgroupVersion",
                            "NCPU",
                            "MemTotal",
                        )
                    },
                    "provenance": provenance,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    output = (args.output or default_output()).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    session_id = uuid.uuid4().hex
    writer = JsonlWriter(output)
    harness = DockerHarness(
        args,
        session_id,
        output.parent,
        normalize_architecture(info.get("Architecture")),
    )
    started = time.monotonic()
    run_records: list[dict[str, Any]] = []
    exit_code = 2
    try:
        writer.write(
            {
                "kind": "session_start",
                "recorded_at": utc_now(),
                "session_id": session_id,
                "profile": args.profile,
                "repeat_count": args.repeat,
                "cases": [
                    {"scenario": scenario, "projection": projection}
                    for scenario in ("accepted", "adversarial")
                    for projection in ("minimal", "files")
                ],
                "limits": PROFILE_LIMITS[args.profile],
                "resource_contract": {
                    "graphql_cpus": 4,
                    "graphql_memory_bytes": 8 * GIB,
                    "max_peak_bytes": args.max_peak_bytes,
                    "cgroup_version": 2,
                },
                "host": {
                    "platform": platform.platform(),
                    "python": sys.version,
                },
                "runtime": {
                    key: info.get(key)
                    for key in (
                        "ServerVersion",
                        "OSType",
                        "OperatingSystem",
                        "Architecture",
                        "CgroupVersion",
                        "NCPU",
                        "MemTotal",
                    )
                },
                "repository": provenance,
            }
        )

        harness.build_images()
        images = harness.resolve_images()
        post_build_provenance = provenance_stability(
            provenance, repository_provenance(), "post_build"
        )
        writer.write(
            {
                "kind": "provenance_check",
                "recorded_at": utc_now(),
                "session_id": session_id,
                **post_build_provenance,
            }
        )
        if not post_build_provenance["ok"]:
            raise HarnessError(
                "workspace changed during image build: "
                f"{post_build_provenance['changed_keys']}"
            )
        setup = harness.start_dependencies()
        writer.write(
            {
                "kind": "setup",
                "recorded_at": utc_now(),
                "session_id": session_id,
                "images": images,
                "graphql_environment_without_credentials": harness.graphql_config(),
                **setup,
            }
        )

        total = args.repeat * 4
        current = 0
        for repeat in range(1, args.repeat + 1):
            for scenario in ("accepted", "adversarial"):
                for projection in ("minimal", "files"):
                    current += 1
                    print(
                        f"running case {current}/{total}: {scenario}/{projection} repeat {repeat}",
                        file=sys.stderr,
                        flush=True,
                    )
                    record = harness.run_case(
                        scenario=scenario,
                        projection=projection,
                        repeat=repeat,
                    )
                    run_records.append(record)
                    writer.write(record)

        final_provenance = provenance_stability(
            provenance, repository_provenance(), "pre_summary"
        )
        writer.write(
            {
                "kind": "provenance_check",
                "recorded_at": utc_now(),
                "session_id": session_id,
                **final_provenance,
            }
        )
        if not final_provenance["ok"]:
            raise HarnessError(
                "workspace changed during RSS run: "
                f"{final_provenance['changed_keys']}"
            )
        passed = all(row["evaluation"]["passed"] for row in run_records)
        peaks = [
            row["cgroup_v2"].get("memory_peak")
            for row in run_records
            if isinstance(row["cgroup_v2"].get("memory_peak"), int)
        ]
        writer.write(
            {
                "kind": "summary",
                "recorded_at": utc_now(),
                "session_id": session_id,
                "passed": passed,
                "runs": len(run_records),
                "failed_cases": [
                    row["case_id"]
                    for row in run_records
                    if not row["evaluation"]["passed"]
                ],
                "peak_bytes_max": max(peaks) if peaks else None,
                "elapsed_seconds": time.monotonic() - started,
            }
        )
        exit_code = 0 if passed else 1
    except Exception as error:
        writer.write(
            {
                "kind": "fatal",
                "recorded_at": utc_now(),
                "session_id": session_id,
                "error": f"{type(error).__name__}: {error}",
                "elapsed_seconds": time.monotonic() - started,
            }
        )
        print(f"fatal: {error}", file=sys.stderr)
        exit_code = 2
    finally:
        try:
            cleanup = harness.cleanup()
        except Exception as error:
            cleanup = {
                "ok": False,
                "preserved": False,
                "remaining_containers": sorted(harness.containers),
                "network_remaining": harness.network_created,
                "errors": [f"{type(error).__name__}: {error}"],
            }
        writer.write(
            {
                "kind": "cleanup",
                "recorded_at": utc_now(),
                "session_id": session_id,
                **cleanup,
            }
        )
        if not cleanup["ok"]:
            exit_code = 2
        writer.close()

    print(f"evidence: {output}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except HarnessError as error:
        print(f"fatal: {error}", file=sys.stderr)
        raise SystemExit(2) from None
