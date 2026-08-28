"""End-to-end smoke test against an in-process mock — no production contact.

Validates: stub generation, channel/RPC plumbing, latency stats shape, the rev2
single-method recall gate (membership valid iff candidate_total<=5000), the
over-cap auto-drop, real-miss detection, freshness-bound recording, and the
percentile mirror.
"""

from __future__ import annotations

import pytest

from ps_harness.client import PathSearchClient
from ps_harness.core import Query, TruthFile, TruthQuery, load_truth, pct
from ps_harness.latency import run_latency
from ps_harness.mockserver import MockPathSearch
from ps_harness.recall import run_recall

H1 = "11" * 20
H2 = "22" * 20
H3 = "33" * 20  # truth-only torrent the mock does NOT contain -> real miss
DOCS = {
    H1: ["Show.S01E01.1080p.x264.mkv"],
    H2: ["Movie.2020.2160p.x265.mkv"],
}


def _tq(id, q, truth, **kw):
    raw = {"id": id, "q": q, "class": "recall", "lang": "ascii",
           "truth_info_hashes": list(truth), "_runtime": {}}
    return TruthQuery(id=id, q=q, lang="ascii", truth={h.lower() for h in truth}, raw=raw)


def _tf(queries, meta=None):
    return TruthFile(meta=meta or {"l3_request": {"limit": 5000, "oversample": 0},
                                   "watermark_margin_secs": 60},
                     queries=queries, raw={"queries": [q.raw for q in queries]})


@pytest.fixture()
def live_addr():
    mock = MockPathSearch(DOCS)
    server, addr = mock.serve()
    yield addr
    server.stop(0)


def test_percentile_nearest_rank():
    v = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert pct(v, 50.0) == 3.0
    assert pct(v, 95.0) == 5.0
    assert pct([], 50.0) == 0.0


def test_latency_runs(live_addr):
    with PathSearchClient(live_addr, timeout=5) as c:
        c.wait_ready()
        res = run_latency(
            c,
            [Query("ascii4", "x264"), Query("ascii3", "ab")],
            reps=5, warm_reps=2, limit=50, oversample=200,
        )
    by_q = {r["query"]: r for r in res["per_query"]}
    assert by_q["x264"]["candidate_total"] == 1
    assert by_q["ab"]["candidate_total"] == 0
    assert res["overall"]["n"] == 10


def test_membership_pass_and_real_miss(live_addr):
    truth = _tf([
        _tq("ascii_x264", "x264", {H1}),           # full recall -> pass
        _tq("ascii_x264_miss", "x264", {H1, H3}),  # H3 absent -> real miss -> FAIL
    ])
    with PathSearchClient(live_addr, timeout=5) as c:
        c.wait_ready()
        res = run_recall(c, truth, watermark_epoch=1_700_000_000)
    by_id = {r["id"]: r for r in res["per_query"]}

    ok = by_id["ascii_x264"]
    assert ok["membership_valid"] is True
    assert ok["recall"] == 1.0
    assert ok["gate_status"] == "pass"

    miss = by_id["ascii_x264_miss"]
    assert miss["recall"] == 0.5
    assert miss["miss_real"] == 1
    assert H3 in miss["real_miss_sample"]
    assert miss["gate_status"] == "FAIL"

    assert res["overall"]["gate6_pass"] is False
    assert "ascii_x264_miss" in res["overall"]["fails"]
    # freshness bound recorded = watermark - margin(60)
    assert res["watermark_bound_epoch"] == 1_700_000_000 - 60
    assert res["truth_meta"]["watermark_bound_epoch"] == 1_700_000_000 - 60


def test_overcap_autodrops():
    docs = {f"{i:040x}": ["file.mkv"] for i in range(1, 5050)}  # >5000 match "mkv"
    mock = MockPathSearch(docs)
    server, addr = mock.serve()
    try:
        late = sorted(docs.keys())[-1]
        truth = _tf([
            _tq("sel_big", "mkv", {late}),                 # over-cap -> dropped
            _tq("sel_ok", "file.mkv", {sorted(docs)[0]}),  # also matches all -> over-cap
        ])
        with PathSearchClient(addr, timeout=5) as c:
            c.wait_ready()
            res = run_recall(c, truth, watermark_epoch=1_700_000_000)
        r = res["per_query"][0]
        assert r["candidate_total"] == len(docs)
        assert r["returned_size"] == 5000
        assert r["membership_valid"] is False
        assert r["gate_status"] == "dropped_overcap"
        # All queries over-cap -> nothing tested -> gate cannot pass.
        assert res["overall"]["tested"] == 0
        assert res["overall"]["gate6_pass"] is False
        assert "sel_big" in res["overall"]["dropped_overcap"]
    finally:
        server.stop(0)


def test_untested_empty_truth(live_addr):
    truth = _tf([_tq("ascii_x264", "x264", set())])  # selective but empty truth sample
    with PathSearchClient(live_addr, timeout=5) as c:
        c.wait_ready()
        res = run_recall(c, truth, watermark_epoch=1_700_000_000)
    r = res["per_query"][0]
    assert r["membership_valid"] is True
    assert r["gate_status"] == "untested_no_truth"
    assert res["overall"]["gate6_pass"] is False  # nothing actually tested


def test_loads_canonical_truth_file():
    # The real rev2 truth file must load, preserve CJK, and expose the margin.
    t = load_truth("/Users/me/aaa/github/bitmagnet/docs/dev/l3-recall-truth.json")
    assert t.l3_limit == 5000
    assert t.watermark_margin_secs == 60
    cjk = [q for q in t.queries if q.lang == "cjk"]
    assert any("蓝光" in q.q for q in cjk)
    assert all(q.expected is not None for q in t.queries)  # rev2 has the hint


def test_populate_rejects_password_in_dsn():
    from ps_harness.populate import populate

    truth = TruthFile(meta={}, queries=[], raw={"queries": []})
    with pytest.raises(ValueError):
        populate(truth, dsn="postgresql://postgres:secret@h/db",
                 watermark_bound_epoch=1_700_000_000)


def test_populate_requires_watermark():
    from ps_harness.populate import populate

    truth = TruthFile(meta={}, queries=[], raw={"queries": []})
    with pytest.raises(ValueError):
        populate(truth, dsn="postgresql://postgres@h/db", watermark_bound_epoch=None)


def test_populate_rejects_bad_sample_pct():
    from ps_harness.populate import populate

    truth = TruthFile(meta={}, queries=[], raw={"queries": []})
    for bad in (0.0, -1.0, 100.1):
        with pytest.raises(ValueError):
            populate(truth, dsn="postgresql://postgres@h/db",
                     watermark_bound_epoch=1_700_000_000, sample_pct=bad)


def test_untested_does_not_count_toward_pass(live_addr):
    # A real pass + an untested (empty-truth) query: gate passes on the tested
    # one, and the untested one is reported, never silently counted as a pass.
    truth = _tf([
        _tq("ascii_x264", "x264", {H1}),     # tested -> pass
        _tq("untested_q", "x264", set()),    # empty truth -> untested
    ])
    with PathSearchClient(live_addr, timeout=5) as c:
        c.wait_ready()
        res = run_recall(c, truth, watermark_epoch=1_700_000_000)
    o = res["overall"]
    assert o["tested"] == 1
    assert o["gate6_pass"] is True
    assert "untested_q" in o["untested_no_truth"]


def test_truth_sql_has_freshness_and_sampling():
    # Regression guard (reviewer DoD): the default truth SQL MUST page-sample,
    # JOIN torrents, and freshness-filter on updated_at via the watermark bind —
    # a future refactor must not silently drop the bounding/freshness.
    from ps_harness.populate import TRUTH_SQL_TMPL

    sql = TRUTH_SQL_TMPL.format(pct=2.0, seed=4242)
    assert "TABLESAMPLE SYSTEM (2.0) REPEATABLE (4242)" in sql
    assert "JOIN torrents" in sql
    assert "t.updated_at <= to_timestamp(%(wm)s)" in sql
    assert "position(lower(%(q)s) IN lower(s.path)) > 0" in sql
    assert "LIMIT %(limit)s" in sql
    # No PK info_hash-range slice (rev2 dropped it — it didn't bound I/O).
    assert "info_hash >=" not in sql and "info_hash <" not in sql
