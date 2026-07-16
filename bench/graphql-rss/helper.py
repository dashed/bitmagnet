#!/usr/bin/env python3
"""Container-only helpers for the isolated GraphQL RSS gate.

The host orchestrator deliberately has no third-party Python dependencies. This
image owns blob construction, PostgreSQL seeding, the gRPC barrier, and the
four-client HTTP driver.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent import futures
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

GENERATED = Path("/app/generated")
if GENERATED.is_dir():
    sys.path.insert(0, str(GENERATED))


MIB = 1024 * 1024
ACCEPTED_HASHES = [bytes([value]) * 20 for value in range(1, 5)]
ADVERSARIAL_HASHES = [bytes([0xAA]) * 20]


@dataclass(frozen=True)
class Profile:
    max_decompressed_bytes: int
    decoded_byte_budget: int
    retained_byte_budget: int
    accepted_files_per_blob: int
    accepted_path_payload_bytes: int
    adversarial_extra_bytes: int
    min_retained_fill_percent: int


PROFILES = {
    # Four accepted blobs together sit below both cumulative byte limits, while
    # the adversarial frame expands beyond the one-blob decompression ceiling.
    "gate": Profile(
        max_decompressed_bytes=64 * MIB,
        decoded_byte_budget=128 * MIB,
        retained_byte_budget=64 * MIB,
        accepted_files_per_blob=14_000,
        accepted_path_payload_bytes=1_000,
        adversarial_extra_bytes=8 * 1024,
        min_retained_fill_percent=80,
    ),
    # Fast structural check using the same ratios and all the same code paths.
    "smoke": Profile(
        max_decompressed_bytes=2 * MIB,
        decoded_byte_budget=4 * MIB,
        retained_byte_budget=2 * MIB,
        accepted_files_per_blob=450,
        accepted_path_payload_bytes=1_000,
        adversarial_extra_bytes=8 * 1024,
        min_retained_fill_percent=80,
    ),
}


def json_line(value: Any) -> None:
    print(json.dumps(value, sort_keys=True, separators=(",", ":")), flush=True)


def encode_files(files: list[dict[str, Any]]) -> tuple[bytes, int, int, int]:
    import msgpack
    import zstandard

    raw = msgpack.packb(files, use_bin_type=True)
    blob = zstandard.ZstdCompressor(level=3).compress(raw)
    owned = sum(len(row["p"].encode()) + len(row["e"].encode()) for row in files)
    # The composer's retained-byte budget charges decoded owned strings. The
    # GraphQL mapper derives another extension later, outside that budget.
    graphql_derived = sum(len(row["e"].encode()) for row in files)
    return blob, len(raw), owned, graphql_derived


def accepted_files(profile: Profile, ordinal: int) -> list[dict[str, Any]]:
    payload = chr(ord("a") + ordinal) * profile.accepted_path_payload_bytes
    return [
        {
            "i": index,
            "p": f"accepted/{ordinal:02d}/{index:05d}-{payload}.mkv",
            "e": "mkv",
            "s": 1_048_576 + index,
        }
        for index in range(profile.accepted_files_per_blob)
    ]


def adversarial_files(profile: Profile) -> list[dict[str, Any]]:
    # MessagePack framing adds bytes, so this is strictly over the configured
    # decompression limit without depending on an encoder implementation detail.
    payload = "x" * (profile.max_decompressed_bytes + profile.adversarial_extra_bytes)
    return [{"i": 0, "p": f"adversarial/{payload}.mkv", "e": "mkv", "s": 1}]


def seed(args: argparse.Namespace) -> int:
    import psycopg

    profile = PROFILES[args.profile]
    created = datetime(2026, 7, 14, tzinfo=timezone.utc)
    summaries: list[dict[str, Any]] = []
    accepted_decoded = 0
    accepted_composer_retained = 0
    accepted_graphql_derived = 0

    with psycopg.connect(args.dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE torrent_file_summary, torrent_tags, "
                "torrents_torrent_sources, torrent_contents, torrents CASCADE"
            )
            cursor.execute(
                "INSERT INTO torrent_sources (key, name) VALUES ('dht', 'DHT') "
                "ON CONFLICT (key) DO UPDATE SET name = EXCLUDED.name"
            )

            datasets: list[tuple[str, bytes, list[dict[str, Any]]]] = []
            for ordinal, info_hash in enumerate(ACCEPTED_HASHES):
                datasets.append(("accepted", info_hash, accepted_files(profile, ordinal)))
            datasets.append(("adversarial", ADVERSARIAL_HASHES[0], adversarial_files(profile)))

            for dataset, info_hash, files in datasets:
                blob, raw_bytes, owned_bytes, graphql_derived_bytes = encode_files(
                    files
                )
                if dataset == "accepted":
                    accepted_decoded += raw_bytes + owned_bytes
                    accepted_composer_retained += owned_bytes
                    accepted_graphql_derived += graphql_derived_bytes

                hex_hash = info_hash.hex()
                name = f"graphql-rss-{dataset}-{hex_hash[:4]}"
                total_size = sum(row["s"] for row in files)
                cursor.execute(
                    """
                    INSERT INTO torrents (
                      info_hash, info_hash_v1, meta_version, name, size, private,
                      files_status, files_count, files_data, file_extensions,
                      created_at, updated_at
                    ) VALUES (
                      %s, %s, 1, %s, %s, false, 'multi', %s, %s, '[\"mkv\"]'::jsonb,
                      %s, %s
                    )
                    """,
                    (
                        info_hash,
                        info_hash,
                        name,
                        total_size,
                        len(files),
                        blob,
                        created,
                        created,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO torrent_contents (
                      id, info_hash, languages, episodes, published_at, size,
                      files_count, created_at, updated_at, seeders, leechers
                    ) VALUES (%s, %s, '[]'::jsonb, '{}'::jsonb, %s, %s, %s, %s, %s, 10, 1)
                    """,
                    (
                        f"{hex_hash}:?:?:?",
                        info_hash,
                        created,
                        total_size,
                        len(files),
                        created,
                        created,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO torrent_file_summary (
                      info_hash, file_count, total_size, largest_file_size,
                      extensions, has_video, has_subtitle, has_audio,
                      created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, '[\"mkv\"]'::jsonb, true, false, false, %s, %s)
                    """,
                    (
                        info_hash,
                        len(files),
                        total_size,
                        max(row["s"] for row in files),
                        created,
                        created,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO torrents_torrent_sources (
                      source, info_hash, seeders, leechers, published_at,
                      created_at, updated_at, seen_count
                    ) VALUES ('dht', %s, 10, 1, %s, %s, %s, 1)
                    """,
                    (info_hash, created, created, created),
                )
                summaries.append(
                    {
                        "dataset": dataset,
                        "info_hash": hex_hash,
                        "files": len(files),
                        "raw_bytes": raw_bytes,
                        "owned_string_bytes": owned_bytes,
                        "composer_retained_string_bytes": owned_bytes,
                        "graphql_derived_extension_bytes": graphql_derived_bytes,
                        "graphql_string_bytes_after_mapping": (
                            owned_bytes + graphql_derived_bytes
                        ),
                        "compressed_bytes": len(blob),
                        "blob_sha256": hashlib.sha256(blob).hexdigest(),
                    }
                )
                del files, blob

        connection.commit()

    adversarial = next(row for row in summaries if row["dataset"] == "adversarial")
    invariants = {
        "accepted_decoded_within_budget": accepted_decoded
        <= profile.decoded_byte_budget,
        "accepted_composer_retained_within_budget": accepted_composer_retained
        <= profile.retained_byte_budget,
        "accepted_composer_retained_fills_budget": (
            accepted_composer_retained * 100
            >= profile.retained_byte_budget * profile.min_retained_fill_percent
        ),
        "adversarial_exceeds_decompressed_limit": adversarial["raw_bytes"]
        > profile.max_decompressed_bytes,
    }
    if not all(invariants.values()):
        raise RuntimeError(f"seed profile violates byte-budget invariants: {invariants}")

    json_line(
        {
            "profile": args.profile,
            "limits": profile.__dict__,
            "accepted_decoded_bytes": accepted_decoded,
            "accepted_composer_retained_bytes": accepted_composer_retained,
            "accepted_graphql_derived_extension_bytes": accepted_graphql_derived,
            "accepted_graphql_string_bytes_after_mapping": (
                accepted_composer_retained + accepted_graphql_derived
            ),
            "accepted_composer_retained_fill_ratio": (
                accepted_composer_retained / profile.retained_byte_budget
            ),
            "invariants": invariants,
            "blobs": summaries,
        }
    )
    return 0


class ReusableKeyedBarrier:
    """A reusable N-party barrier keyed by the exact candidate query."""

    def __init__(self, parties: int, timeout: float, event_writer: "EventWriter"):
        self.parties = parties
        self.timeout = timeout
        self.event_writer = event_writer
        self.condition = threading.Condition()
        self.state: dict[str, dict[str, int]] = {}

    def wait(self, key: str) -> int:
        with self.condition:
            state = self.state.setdefault(key, {"generation": 0, "arrivals": 0})
            generation = state["generation"]
            state["arrivals"] += 1
            arrival = state["arrivals"]
            self.event_writer.write(
                "arrive", key=key, generation=generation, arrival=arrival
            )
            if arrival == self.parties:
                state["arrivals"] = 0
                state["generation"] += 1
                self.condition.notify_all()
            else:
                deadline = time.monotonic() + self.timeout
                while state["generation"] == generation:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self.event_writer.write(
                            "timeout", key=key, generation=generation, arrival=arrival
                        )
                        raise TimeoutError(
                            f"barrier {key!r} generation {generation} reached "
                            f"{arrival}/{self.parties}"
                        )
                    self.condition.wait(remaining)
            self.event_writer.write(
                "release", key=key, generation=generation, arrival=arrival
            )
            return generation


class EventWriter:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def write(self, event: str, **fields: Any) -> None:
        row = {
            "event": event,
            "monotonic_ns": time.monotonic_ns(),
            "time_unix_ns": time.time_ns(),
            **fields,
        }
        with self.lock, self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())


def mock(args: argparse.Namespace) -> int:
    import grpc
    from bitmagnet import path_search_pb2, path_search_pb2_grpc

    events = EventWriter(args.events)
    barrier = ReusableKeyedBarrier(args.barrier, args.barrier_timeout, events)
    candidates = {
        "accepted": ACCEPTED_HASHES,
        "adversarial": ADVERSARIAL_HASHES,
    }

    class Servicer(path_search_pb2_grpc.PathSearchServiceServicer):
        def PathCandidates(self, request, context):  # noqa: N802
            query = request.query.strip().lower()
            try:
                generation = barrier.wait(query)
            except TimeoutError as error:
                context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, str(error))
            hashes = candidates.get(query, [])
            events.write(
                "respond",
                key=query,
                generation=generation,
                candidates=len(hashes),
            )
            return path_search_pb2.PathCandidatesResponse(
                candidates=[
                    path_search_pb2.PathCandidate(
                        info_hash=info_hash, score=1.0, sort_value=0
                    )
                    for info_hash in hashes
                ],
                candidate_total=len(hashes),
                estimated=True,
            )

        def Suggest(self, request, context):  # noqa: N802
            del request, context
            return path_search_pb2.SuggestResponse()

        def HealthCheck(self, request, context):  # noqa: N802
            del request, context
            return path_search_pb2.PathSearchHealth(
                status=1,
                doc_count=len(ACCEPTED_HASHES) + len(ADVERSARIAL_HASHES),
                index_bytes=1,
                watermark_epoch=int(time.time()),
                writable=True,
                suggest_ready=True,
                suggest_entries=2,
            )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max(16, args.barrier * 2)))
    path_search_pb2_grpc.add_PathSearchServiceServicer_to_server(Servicer(), server)
    port = server.add_insecure_port(args.bind)
    if port == 0:
        raise RuntimeError(f"could not bind gRPC mock to {args.bind}")
    server.start()
    events.write("started", bind=args.bind, barrier=args.barrier)
    server.wait_for_termination()
    return 0


MINIMAL_SELECTION = """
      id
      infoHash
      title
      torrent {
        infoHash
        name
        size
        filesStatus
        filesCount
        hasFilesInfo
        singleFile
        seeders
        leechers
        tagNames
        magnetUri
        createdAt
        updatedAt
        sources { key name seenCount firstSeenAt lastSeenAt }
      }
"""

FILES_SELECTION = MINIMAL_SELECTION.replace(
    "        sources { key name seenCount firstSeenAt lastSeenAt }",
    """        sources { key name seenCount firstSeenAt lastSeenAt }
        files { infoHash index path extension size }""",
)


def graphql_query(projection: str) -> str:
    selection = FILES_SELECTION if projection == "files" else MINIMAL_SELECTION
    return f"""query GraphqlRss($input: TorrentContentSearchQueryInput!) {{
  torrentContent {{
    search(input: $input) {{
      items {{
{selection}
      }}
      totalCount
      totalCountIsEstimate
      hasNextPage
    }}
  }}
}}"""


def request_once(
    url: str,
    query: str,
    variables: dict[str, Any],
    start: threading.Barrier,
    client: int,
    timeout: float,
) -> dict[str, Any]:
    request_body = json.dumps({"query": query, "variables": variables}).encode()
    start.wait(timeout=timeout)
    started = time.monotonic_ns()
    status = 0
    headers: dict[str, str] = {}
    try:
        request = urllib.request.Request(
            url,
            data=request_body,
            headers={"content-type": "application/json", "user-agent": "bm-graphql-rss/1"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            headers = {key.lower(): value for key, value in response.headers.items()}
            body = response.read()
    except urllib.error.HTTPError as error:
        status = error.code
        headers = {key.lower(): value for key, value in error.headers.items()}
        body = error.read()
    except Exception as error:  # The evidence must preserve transport failures.
        return {
            "client": client,
            "transport_error": f"{type(error).__name__}: {error}",
            "latency_ms": (time.monotonic_ns() - started) / 1_000_000,
        }

    latency_ms = (time.monotonic_ns() - started) / 1_000_000
    result: dict[str, Any] = {
        "client": client,
        "http_status": status,
        "response_bytes": len(body),
        "response_sha256": hashlib.sha256(body).hexdigest(),
        "latency_ms": latency_ms,
        "handler_duration_us": headers.get("x-bitmagnet-graphql-handler-duration-us"),
    }
    try:
        payload = json.loads(body)
        errors = payload.get("errors") or []
        search = ((payload.get("data") or {}).get("torrentContent") or {}).get("search")
        items = (search or {}).get("items") or []
        result.update(
            {
                "graphql_errors": [
                    {"message": row.get("message"), "path": row.get("path")}
                    for row in errors
                ],
                "item_count": len(items),
                "file_count": sum(
                    len(((item.get("torrent") or {}).get("files") or []))
                    for item in items
                ),
                "total_count": (search or {}).get("totalCount"),
                "total_count_is_estimate": (search or {}).get("totalCountIsEstimate"),
                "has_next_page": (search or {}).get("hasNextPage"),
            }
        )
    except Exception as error:
        result["parse_error"] = f"{type(error).__name__}: {error}"
        result["response_prefix"] = body[:512].decode(errors="replace")
    return result


def fetch(url: str, timeout: float) -> tuple[int, bytes, dict[str, str]]:
    request = urllib.request.Request(url, headers={"user-agent": "bm-graphql-rss/1"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return (
            response.status,
            response.read(),
            {key.lower(): value for key, value in response.headers.items()},
        )


def drive(args: argparse.Namespace) -> int:
    query = graphql_query(args.projection)
    variables = {
        "input": {
            "queryString": args.query,
            "limit": 4,
            "totalCount": False,
            "hasNextPage": False,
            "orderBy": [{"field": "relevance", "descending": True}],
        }
    }
    start = threading.Barrier(args.clients)
    with futures.ThreadPoolExecutor(max_workers=args.clients) as executor:
        pending = [
            executor.submit(
                request_once,
                args.url,
                query,
                variables,
                start,
                client,
                args.timeout,
            )
            for client in range(args.clients)
        ]
        responses = sorted((future.result() for future in pending), key=lambda row: row["client"])

    metrics_error = None
    metrics_samples: list[str] = []
    metrics_sha256 = None
    metrics_bytes = 0
    try:
        status, body, _ = fetch(args.metrics_url, args.timeout)
        if status != 200:
            raise RuntimeError(f"metrics returned HTTP {status}")
        metrics_bytes = len(body)
        metrics_sha256 = hashlib.sha256(body).hexdigest()
        metrics_samples = [
            line
            for line in body.decode().splitlines()
            if line.startswith("bitmagnet_search_pathsearch_")
            or line.startswith("process_resident_memory_bytes")
            or line.startswith("process_virtual_memory_bytes")
        ]
    except Exception as error:
        metrics_error = f"{type(error).__name__}: {error}"

    json_line(
        {
            "query_name": args.query,
            "projection": args.projection,
            "query_sha256": hashlib.sha256(query.encode()).hexdigest(),
            "clients": args.clients,
            "responses": responses,
            "metrics_error": metrics_error,
            "metrics_bytes": metrics_bytes,
            "metrics_sha256": metrics_sha256,
            "metrics_samples": metrics_samples,
        }
    )
    return 0


def wait_for_url(args: argparse.Namespace) -> int:
    deadline = time.monotonic() + args.timeout
    last_error = "not attempted"
    while time.monotonic() < deadline:
        try:
            status, body, _ = fetch(args.url, min(2.0, args.timeout))
            text = body.decode(errors="replace")
            if 200 <= status < 300 and (args.contains is None or args.contains in text):
                json_line({"url": args.url, "status": status, "matched": args.contains})
                return 0
            last_error = f"HTTP {status}; required text absent"
        except Exception as error:
            last_error = f"{type(error).__name__}: {error}"
        time.sleep(0.25)
    raise RuntimeError(f"timed out waiting for {args.url}: {last_error}")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    sub = root.add_subparsers(dest="command", required=True)

    seed_parser = sub.add_parser("seed")
    seed_parser.add_argument("--dsn", required=True)
    seed_parser.add_argument("--profile", choices=sorted(PROFILES), default="gate")
    seed_parser.set_defaults(func=seed)

    mock_parser = sub.add_parser("mock")
    mock_parser.add_argument("--bind", default="0.0.0.0:50053")
    mock_parser.add_argument("--barrier", type=int, default=4)
    mock_parser.add_argument("--barrier-timeout", type=float, default=60.0)
    mock_parser.add_argument("--events", required=True)
    mock_parser.set_defaults(func=mock)

    drive_parser = sub.add_parser("drive")
    drive_parser.add_argument("--url", required=True)
    drive_parser.add_argument("--metrics-url", required=True)
    drive_parser.add_argument("--query", choices=("accepted", "adversarial"), required=True)
    drive_parser.add_argument("--projection", choices=("minimal", "files"), required=True)
    drive_parser.add_argument("--clients", type=int, default=4)
    drive_parser.add_argument("--timeout", type=float, default=180.0)
    drive_parser.set_defaults(func=drive)

    wait_parser = sub.add_parser("wait")
    wait_parser.add_argument("--url", required=True)
    wait_parser.add_argument("--contains")
    wait_parser.add_argument("--timeout", type=float, default=60.0)
    wait_parser.set_defaults(func=wait_for_url)
    return root


def main() -> int:
    args = parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        json_line({"fatal": f"{type(error).__name__}: {error}"})
        raise
