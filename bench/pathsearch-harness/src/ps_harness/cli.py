"""CLI entry point for ps-harness.

Subcommands:
  gen        generate gRPC stubs from the vendored protos (also auto-run lazily)
  health     call HealthCheck and print the index snapshot
  query      one-shot PathCandidates for a single query (debugging)
  latency    D6 gate 5 — per-query/per-group p50/p95/p99 over N reps
  recall     D6 gate 6 — candidate recall vs a ground-truth info_hash set

Connection: BITMAGNET_PATHSEARCH_ADDR (default 127.0.0.1:50053), plaintext.
All modes can write JSON via --json-out and print a readable summary to stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

DEFAULT_ADDR = os.environ.get("BITMAGNET_PATHSEARCH_ADDR", "127.0.0.1:50053")
DEFAULT_QUERIES = (
    Path(__file__).resolve().parent.parent.parent / "queries" / "ps_prefix_sweep.tsv"
)


def _add_conn(sp: argparse.ArgumentParser) -> None:
    sp.add_argument("--addr", default=DEFAULT_ADDR, help="HOST:PORT (plaintext)")
    sp.add_argument("--timeout", type=float, default=30.0, help="per-RPC timeout (s)")
    sp.add_argument("--json-out", help="write full machine-readable JSON here")


def _emit(result: dict, text: str, json_out: str | None) -> None:
    print(text)
    if json_out:
        Path(json_out).write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"\n[json written to {json_out}]", file=sys.stderr)


def _health_dict(h) -> dict:
    return {
        "status": h.status,
        "doc_count": h.doc_count,
        "index_bytes": h.index_bytes,
        "index_gib": round(h.index_bytes / 1024**3, 3),
        "watermark_epoch": h.watermark_epoch,
        "writable": h.writable,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ps-harness",
        description="Latency + recall harness for the L3 bitmagnet-pathsearch sidecar.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("gen", help="(re)generate gRPC stubs from vendored protos")
    g.add_argument("--force", action="store_true")

    h = sub.add_parser("health", help="HealthCheck snapshot")
    _add_conn(h)

    q = sub.add_parser("query", help="one-shot PathCandidates (debug)")
    _add_conn(q)
    q.add_argument("text", help="query string")
    q.add_argument("--limit", type=int, default=50)
    q.add_argument("--oversample", type=int, default=200)
    q.add_argument("--sort", action="append", default=[], help="field[:asc|:desc]")
    q.add_argument("--show", type=int, default=10, help="how many hashes to print")

    lat = sub.add_parser("latency", help="D6 gate 5 latency benchmark")
    _add_conn(lat)
    lat.add_argument("--queries-file", default=str(DEFAULT_QUERIES))
    lat.add_argument("--reps", type=int, default=30, help="timed reps per query")
    lat.add_argument("--warm-reps", type=int, default=5, help="untimed priming reps")
    lat.add_argument("--limit", type=int, default=50)
    lat.add_argument("--oversample", type=int, default=200)
    lat.add_argument("--sort", action="append", default=[])
    lat.add_argument("--no-cold-first", action="store_true")

    rec = sub.add_parser("recall", help="D6 gate 6 candidate-recall check")
    _add_conn(rec)
    rec.add_argument("--truth-file", required=True)
    rec.add_argument(
        "--limit", type=int, default=None,
        help="page size (default = truth meta.l3_request.limit, else 5000; clamped at 5000)",
    )
    rec.add_argument("--oversample", type=int, default=None)
    rec.add_argument("--sort", action="append", default=[])
    rec.add_argument(
        "--write-truth",
        help="write the truth file with _runtime fields filled to this path",
    )

    pop = sub.add_parser(
        "populate",
        help="fill truth_info_hashes from PostgreSQL (§5 rev2; read-only; LEAD-GATED H4)",
    )
    pop.add_argument("--truth-file", required=True, help="canonical truth file (in/out)")
    pop.add_argument(
        "--pg",
        default="postgresql://postgres@127.0.0.1:5432/bitmagnet",
        help="DSN WITHOUT password; PGPASSWORD is read from the environment",
    )
    pop.add_argument("--out", help="output truth file (default: overwrite --truth-file)")
    pop.add_argument(
        "--statement-timeout-ms", type=int, default=15000,
        help="statement_timeout (protects the live sidecar; MAX_CONNECTIONS=2)",
    )
    pop.add_argument("--sample-pct", type=float, default=2.0, help="TABLESAMPLE SYSTEM pct")
    pop.add_argument("--sample-seed", type=int, default=4242, help="TABLESAMPLE REPEATABLE seed")
    pop.add_argument("--limit", type=int, default=None, help="per-query truth LIMIT (default meta=500)")
    wm = pop.add_mutually_exclusive_group()
    wm.add_argument(
        "--watermark-bound-epoch", type=int, default=None,
        help="freshness bound $2 = (L3 watermark_epoch - margin); recorded in meta",
    )
    wm.add_argument(
        "--grpc", default=None,
        help="read watermark_epoch from L3 HealthCheck at this addr and subtract the margin",
    )
    pop.add_argument(
        "--no-freshness", action="store_true",
        help="skip the updated_at freshness filter (NOT recommended)",
    )
    pop.add_argument("--timeout", type=float, default=10.0, help="gRPC timeout for --grpc")

    args = parser.parse_args(argv)

    if args.cmd == "gen":
        from .protos import generate

        generate(force=args.force)
        print("stubs generated")
        return 0

    if args.cmd == "populate":
        return _run_populate(args)

    # Modes that need a live connection.
    import grpc

    try:
        return _run_live(args)
    except grpc.FutureTimeoutError:
        print(
            f"error: could not connect to {args.addr} within the readiness timeout; "
            "is the sidecar reachable (port-forward/tailscale up)?",
            file=sys.stderr,
        )
        return 3
    except grpc.RpcError as e:
        code = e.code() if hasattr(e, "code") else "?"
        print(f"error: RPC failed ({code}): {e.details() if hasattr(e, 'details') else e}",
              file=sys.stderr)
        return 4


def _run_live(args) -> int:
    from .client import PathSearchClient
    from .core import load_queries, load_truth, parse_sort

    sort = parse_sort(args.sort) if getattr(args, "sort", None) else None

    if args.cmd == "health":
        with PathSearchClient(args.addr, args.timeout) as c:
            c.wait_ready()
            h = c.health()
        d = _health_dict(h)
        text = (
            f"HealthCheck @ {args.addr}\n"
            f"  status={d['status']} doc_count={d['doc_count']:,} "
            f"index={d['index_gib']} GiB writable={d['writable']} "
            f"watermark_epoch={d['watermark_epoch']}"
        )
        _emit(d, text, args.json_out)
        return 0

    if args.cmd == "query":
        with PathSearchClient(args.addr, args.timeout) as c:
            c.wait_ready()
            res = c.path_candidates(args.text, args.limit, args.oversample, sort)
        d = {
            "query": args.text,
            "candidate_total": res.candidate_total,
            "returned": len(res.candidates_hex),
            "estimated": res.estimated,
            "elapsed_ms": res.elapsed_ms,
            "candidates": res.candidates_hex[: args.show],
        }
        text = (
            f"query={args.text!r} total={res.candidate_total} "
            f"returned={len(res.candidates_hex)} estimated={res.estimated} "
            f"elapsed={res.elapsed_ms:.2f}ms\n  "
            + "\n  ".join(res.candidates_hex[: args.show])
        )
        _emit(d, text, args.json_out)
        return 0

    if args.cmd == "latency":
        from .latency import format_latency, run_latency

        queries = load_queries(args.queries_file)
        with PathSearchClient(args.addr, args.timeout) as c:
            c.wait_ready()
            health = _health_dict(c.health())
            result = run_latency(
                c,
                queries,
                reps=args.reps,
                warm_reps=args.warm_reps,
                limit=args.limit,
                oversample=args.oversample,
                sort=sort,
                cold_first=not args.no_cold_first,
            )
        result["addr"] = args.addr
        result["health"] = health
        result["queries_file"] = args.queries_file
        result["notes"] = (
            "Single-client, sequential. If --addr points at a kubectl/port-forward, "
            "each RPC includes an extra API-server hop that inflates sub-50ms numbers "
            "(L3 in-cluster p50 ~25ms); treat absolute latency as an upper bound and "
            "compare relative per-group/charset deltas. Recall is latency-insensitive."
        )
        _emit(result, format_latency(result), args.json_out)
        return 0

    if args.cmd == "recall":
        from .recall import format_recall, run_recall

        truth = load_truth(args.truth_file)
        with PathSearchClient(args.addr, args.timeout) as c:
            c.wait_ready()
            health = _health_dict(c.health())
            # Freshness: record the L3 follow watermark so real-miss triage can
            # tell "not-yet-indexed (created_at > watermark)" from a true miss.
            result = run_recall(
                c,
                truth,
                limit=args.limit,
                oversample=args.oversample,
                sort=sort,
                watermark_epoch=health["watermark_epoch"],
            )
        result["addr"] = args.addr
        result["health"] = health
        result["truth_file"] = args.truth_file
        _emit(result, format_recall(result), args.json_out)
        if args.write_truth:
            Path(args.write_truth).write_text(
                json.dumps(result["populated_truth"], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"[truth+runtime written to {args.write_truth}]", file=sys.stderr)
        # Non-zero exit if the gate failed, so a CI/gated run can branch on it.
        return 0 if result["overall"]["gate6_pass"] else 5

    print(f"error: unknown command {args.cmd}", file=sys.stderr)
    return 2


def _run_populate(args) -> int:
    from .core import load_truth
    from .populate import populate

    truth = load_truth(args.truth_file)

    # Resolve the freshness bound: explicit epoch, or read it from L3 HealthCheck.
    wm_bound = args.watermark_bound_epoch
    if wm_bound is None and args.grpc:
        from .client import PathSearchClient

        margin = truth.watermark_margin_secs
        try:
            with PathSearchClient(args.grpc, args.timeout) as c:
                c.wait_ready()
                w = c.health().watermark_epoch
            wm_bound = w - margin
            print(
                f"[freshness] L3 watermark_epoch={w} - margin={margin}s "
                f"-> watermark_bound_epoch={wm_bound}",
                file=sys.stderr,
            )
        except Exception as e:  # noqa: BLE001
            print(f"error: could not read watermark from {args.grpc}: {e}", file=sys.stderr)
            return 6

    try:
        report = populate(
            truth,
            dsn=args.pg,
            watermark_bound_epoch=wm_bound,
            statement_timeout_ms=args.statement_timeout_ms,
            sample_pct=args.sample_pct,
            sample_seed=args.sample_seed,
            limit=args.limit,
            no_freshness=args.no_freshness,
        )
    except Exception as e:  # noqa: BLE001 — surface PG/connection errors concisely
        print(f"error: populate failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 6

    out = args.out or args.truth_file
    Path(out).write_text(
        json.dumps(truth.raw, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    n_ok = len(report["filled"])
    n_err = len(report["errors"])
    total_hashes = sum(n for _, n in report["filled"])
    print(
        f"populated {n_ok} truth sets ({total_hashes} hashes), {n_err} errors -> {out}"
    )
    if report["errors"]:
        print(f"  errors: {report['errors']}", file=sys.stderr)
    return 0 if n_err == 0 else 7


if __name__ == "__main__":
    raise SystemExit(main())
