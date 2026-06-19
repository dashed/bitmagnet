"""Recall mode (D6 gate 6) — canonical rev2 gate from
``docs/dev/l3-recall-gate-query-set-and-truth.md``.

SINGLE method: sample membership, gated on the 5000-cap.

Per query the harness requests L3 with ``limit = meta.l3_request.limit`` (5000 →
``returned = min(candidate_total, 5000)``) and records ``candidate_total`` +
``returned_size``. info_hashes from L3 are raw 20 bytes → hex-encoded LOWERCASE.

* ``candidate_total <= 5000`` → ``membership_valid = true`` → L3 returned its FULL
  match-set, so the (page-sampled, freshness-filtered) truth must be wholly
  contained → ``recall = |truth ∩ returned| / |truth|`` **must = 1.0**; any single
  real miss FAILS the gate → §4c triage.
* ``candidate_total > 5000`` → ``membership_valid = false`` → EXCLUDED from the
  recall metric (a latency query; truth hashes "absent" here are below the cap,
  not misses). This auto-drop is expected for the broad/over-cap candidates.

Freshness: the truth SQL already filters ``updated_at <= watermark_bound_epoch``
(= the run-start L3 ``watermark_epoch`` − margin), so a miss is never staleness.
The harness records ``watermark_bound_epoch`` in ``meta`` for the lead's ``$2``.
"""

from __future__ import annotations

from .client import PathSearchClient
from .core import MAX_CANDIDATES, MIN_QUERY_CHARS, TruthFile, summarize

MISS_SAMPLE = 25


def _eval_query(client, tq, *, limit, oversample, sort) -> dict:
    q = tq.q
    too_short = len(q.strip()) < MIN_QUERY_CHARS

    res = client.path_candidates(q, limit, oversample, sort)
    returned = set(res.candidates_hex)
    returned_size = len(res.candidates_hex)
    candidate_total = res.candidate_total
    truth_n = len(tq.truth)
    membership_valid = candidate_total <= MAX_CANDIDATES

    row: dict = {
        "id": tq.id,
        "q": q,
        "lang": tq.lang,
        "expected": tq.expected,
        "candidate_total": candidate_total,
        "returned_size": returned_size,
        "truth_count": truth_n,
        "truth_sample_count": tq.truth_sample_count,
        "membership_valid": membership_valid,
        "elapsed_ms": res.elapsed_ms,
        "recall": None,
        "intersect": None,
        "miss_total": 0,
        "miss_real": 0,
        "real_miss_sample": [],
        "flags": [],
    }
    if too_short:
        row["flags"].append("query_below_floor(<2)")
    if tq.malformed:
        row["flags"].append(f"malformed_truth={tq.malformed}")
    # Sanity: when membership is valid, L3 returns the full set, so returned_size
    # should equal candidate_total.
    if membership_valid and returned_size != candidate_total:
        row["flags"].append(
            f"returned_size!=candidate_total ({returned_size}!={candidate_total})"
        )

    if not membership_valid:
        # Over-cap → auto-dropped from the recall metric (latency-only).
        row["gate_status"] = "dropped_overcap"
        row["flags"].append("auto_dropped(candidate_total>5000)")
        return row

    if truth_n == 0:
        row["gate_status"] = "untested_no_truth"
        row["flags"].append("empty_truth_sample")
        return row

    intersect = tq.truth & returned
    inter_n = len(intersect)
    recall = inter_n / truth_n
    row["intersect"] = inter_n
    row["recall"] = recall

    misses = tq.truth - returned  # membership valid ⇒ every miss is a REAL miss
    row["miss_total"] = len(misses)
    row["miss_real"] = len(misses)
    row["real_miss_sample"] = sorted(misses)[:MISS_SAMPLE]
    row["gate_status"] = "pass" if recall >= 1.0 else "FAIL"
    return row


def run_recall(
    client: PathSearchClient,
    truth: TruthFile,
    *,
    limit: int | None = None,
    oversample: int | None = None,
    sort=None,
    watermark_epoch: int | None = None,
) -> dict:
    limit = truth.l3_limit if limit is None else limit
    oversample = truth.l3_oversample if oversample is None else oversample

    # Record the freshness contract value so the lead's truth SQL ($2) and L3 agree.
    margin = truth.watermark_margin_secs
    watermark_bound_epoch = (
        watermark_epoch - margin if watermark_epoch is not None else None
    )
    if watermark_bound_epoch is not None:
        truth.meta["watermark_bound_epoch"] = watermark_bound_epoch
        if isinstance(truth.raw, dict):
            truth.raw.setdefault("meta", {})["watermark_bound_epoch"] = watermark_bound_epoch

    rows = []
    for tq in truth.queries:
        r = _eval_query(client, tq, limit=limit, oversample=oversample, sort=sort)
        rt = tq.raw.setdefault("_runtime", {})
        rt["candidate_total"] = r["candidate_total"]
        rt["returned_size"] = r["returned_size"]
        rt["recall"] = r["recall"]
        rt["membership_valid"] = r["membership_valid"]
        rows.append(r)

    fails = [r for r in rows if r["gate_status"] == "FAIL"]
    tested = [r for r in rows if r["gate_status"] in ("pass", "FAIL")]
    dropped = [r for r in rows if r["gate_status"] == "dropped_overcap"]
    untested = [r for r in rows if r["gate_status"] == "untested_no_truth"]
    valid_recalls = [r["recall"] for r in tested]
    real_miss_total = sum(r.get("miss_real", 0) for r in rows)
    latencies = [r["elapsed_ms"] for r in rows]

    gate_pass = len(fails) == 0 and len(tested) > 0
    return {
        "mode": "recall",
        "params": {
            "limit": limit,
            "oversample": oversample,
            "max_candidates_cap": MAX_CANDIDATES,
            "watermark_margin_secs": margin,
        },
        "watermark_epoch": watermark_epoch,
        "watermark_bound_epoch": watermark_bound_epoch,
        "truth_meta": truth.meta,
        "per_query": rows,
        "overall": {
            "queries": len(rows),
            "tested": len(tested),
            "dropped_overcap": [r["id"] for r in dropped],
            "untested_no_truth": [r["id"] for r in untested],
            "gate6_pass": gate_pass,
            "fails": [r["id"] for r in fails],
            "min_tested_recall": min(valid_recalls) if valid_recalls else None,
            "real_miss_total": real_miss_total,
            "total_truth_hashes": sum(r["truth_count"] for r in tested),
            "latency": summarize(latencies),
        },
        "populated_truth": truth.raw,
    }


def format_recall(result: dict) -> str:
    lines: list[str] = []
    p = result["params"]
    lines.append(
        f"RECALL (gate 6)  limit={p['limit']} cap={p['max_candidates_cap']}  "
        f"watermark_epoch={result.get('watermark_epoch')} "
        f"watermark_bound_epoch={result.get('watermark_bound_epoch')} "
        f"(margin={p['watermark_margin_secs']}s)"
    )
    lines.append("")
    lines.append(
        f"  {'id':<24} {'q':<18} {'tot':>9} {'ret':>6} {'truth':>6} "
        f"{'recall':>7} {'status':>18}"
    )
    for r in result["per_query"]:
        rec = r.get("recall")
        rec_s = "-" if rec is None else f"{rec:.4f}"
        lines.append(
            f"  {r['id'][:24]:<24} {r['q'][:18]:<18} {r['candidate_total']:>9} "
            f"{r['returned_size']:>6} {r['truth_count']:>6} {rec_s:>7} "
            f"{r['gate_status']:>18}"
        )
        if r.get("flags"):
            lines.append(f"      flags: {', '.join(r['flags'])}")
    o = result["overall"]
    lines.append("")
    verdict = "PASS ✅" if o["gate6_pass"] else "FAIL ❌"
    lines.append(f"GATE 6: {verdict}")
    lines.append(
        f"  tested={o['tested']} (truth hashes={o['total_truth_hashes']}) "
        f"min_recall={o['min_tested_recall']} real_misses={o['real_miss_total']} "
        f"| dropped_overcap={len(o['dropped_overcap'])} untested={len(o['untested_no_truth'])}"
    )
    if o["dropped_overcap"]:
        lines.append(
            f"  auto-dropped (over-cap, latency-only): {', '.join(o['dropped_overcap'])}"
        )
    if o["untested_no_truth"]:
        lines.append(
            "  untested (empty truth sample — NOT pass/fail; re-run hotter with "
            f"higher --sample-pct off-peak): {', '.join(o['untested_no_truth'])}"
        )
    if o["fails"]:
        lines.append(f"  FAILED: {', '.join(o['fails'])}")
    lat = o["latency"]
    lines.append(f"  recall-call latency: p50={lat['p50']:.2f} p95={lat['p95']:.2f} ms")
    miss_rows = [r for r in result["per_query"] if r.get("real_miss_sample")]
    if miss_rows:
        lines.append("")
        lines.append("real-miss samples (gate-failing; triage per §4c):")
        for r in miss_rows:
            lines.append(
                f"  {r['id']} ({r['q']}): {', '.join(r['real_miss_sample'][:5])}"
                + (" ..." if r["miss_real"] > 5 else "")
            )
    return "\n".join(lines)
