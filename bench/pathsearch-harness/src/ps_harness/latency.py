"""Latency mode (D6 gate 5).

Single-client, sequential. For each query: ``warm_reps`` untimed priming calls,
then ``reps`` timed PathCandidates RPCs. Reports per-query and per-group
p50/p95/p99/min/max/mean, plus candidate_total / returned / estimated. Mirrors
the methodology in ``docs/dev/pathsearch-microbench-spec.md`` (cold-first +
warm-rep), adapted to the RPC boundary (the RPC internally does Count + TopDocs,
so we measure the production end-to-end candidate latency rather than the two
Tantivy collectors separately).
"""

from __future__ import annotations

from .client import PathSearchClient
from .core import MAX_CANDIDATES, Query, summarize


def run_latency(
    client: PathSearchClient,
    queries: list[Query],
    *,
    reps: int,
    warm_reps: int,
    limit: int,
    oversample: int,
    sort=None,
    cold_first: bool = True,
) -> dict:
    per_query: list[dict] = []
    by_group: dict[str, list[float]] = {}

    for q in queries:
        cold_ms = None
        # Cold-first single timed call BEFORE priming (best-effort analog of the
        # spec's post-drop_caches cold read; over RPC we cannot drop_caches, so
        # this captures the first-touch cost only).
        if cold_first:
            cold_ms = client.path_candidates(
                q.query, limit, oversample, sort
            ).elapsed_ms

        # Warmup (untimed).
        last = None
        for _ in range(warm_reps):
            last = client.path_candidates(q.query, limit, oversample, sort)

        # Timed reps.
        samples: list[float] = []
        for _ in range(reps):
            res = client.path_candidates(q.query, limit, oversample, sort)
            samples.append(res.elapsed_ms)
            last = res

        stats = summarize(samples)
        by_group.setdefault(q.group, []).extend(samples)
        per_query.append(
            {
                "group": q.group,
                "query": q.query,
                "candidate_total": last.candidate_total if last else None,
                "returned": len(last.candidates_hex) if last else None,
                "estimated": last.estimated if last else None,
                "truncated": (
                    last.candidate_total > len(last.candidates_hex)
                    if last
                    else None
                ),
                "cap_hit": (
                    len(last.candidates_hex) >= MAX_CANDIDATES if last else None
                ),
                "cold_ms": cold_ms,
                **stats,
            }
        )

    group_summary = [
        {"group": g, **summarize(v)} for g, v in sorted(by_group.items())
    ]
    all_samples = [s for v in by_group.values() for s in v]
    return {
        "mode": "latency",
        "params": {
            "reps": reps,
            "warm_reps": warm_reps,
            "limit": limit,
            "oversample": oversample,
            "cold_first": cold_first,
            "percentile_method": "nearest-rank (round-half-away), mirrors bench-file-index pct()",
        },
        "per_query": per_query,
        "by_group": group_summary,
        "overall": summarize(all_samples),
    }


def format_latency(result: dict) -> str:
    lines: list[str] = []
    p = result["params"]
    lines.append(
        f"LATENCY  reps={p['reps']} warm={p['warm_reps']} "
        f"limit={p['limit']} oversample={p['oversample']}"
    )
    lines.append("")
    lines.append("per-query (ms):")
    hdr = f"  {'group':<8} {'query':<14} {'tot':>9} {'ret':>6} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8}"
    lines.append(hdr)
    for r in result["per_query"]:
        flag = ""
        if r.get("truncated"):
            flag += " *trunc"
        if r.get("cap_hit"):
            flag += " *cap"
        lines.append(
            f"  {r['group']:<8} {r['query'][:14]:<14} {str(r['candidate_total']):>9} "
            f"{str(r['returned']):>6} {r['p50']:>8.2f} {r['p95']:>8.2f} "
            f"{r['p99']:>8.2f} {r['max']:>8.2f}{flag}"
        )
    lines.append("")
    lines.append("by-group (ms):")
    lines.append(
        f"  {'group':<8} {'n':>6} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8}"
    )
    for r in result["by_group"]:
        lines.append(
            f"  {r['group']:<8} {r['n']:>6} {r['p50']:>8.2f} {r['p95']:>8.2f} "
            f"{r['p99']:>8.2f} {r['max']:>8.2f}"
        )
    o = result["overall"]
    lines.append("")
    lines.append(
        f"overall: n={o['n']} p50={o['p50']:.2f} p95={o['p95']:.2f} "
        f"p99={o['p99']:.2f} max={o['max']:.2f} ms"
    )
    return "\n".join(lines)
