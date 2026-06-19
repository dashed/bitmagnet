"""Shared helpers: percentiles, query/truth parsing, hashing, gRPC client.

Server semantics mirrored here (verified against
``bitmagnet-rs/crates/bitmagnet-search/src/pathsearch/`` at deploy time):

* Query guard (``query.rs``): a trimmed query of < 2 chars matches nothing.
* Tokenizer (``schema.rs``): ``NgramTokenizer(2, 3)`` + ``LowerCaser`` — query and
  index are both lowercased, so truth must be case-INSENSITIVE substring.
* ``candidate_total`` is the exact (uncapped) match count; the returned candidate
  list is clamped to ``limit + oversample`` and a hard ``MAX_CANDIDATES = 5000``.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path

# Server constants (from query.rs). Kept here so the harness can flag when a
# request will hit them rather than silently under-reporting recall.
MIN_QUERY_CHARS = 2
MAX_CANDIDATES = 5_000

_HEX40 = re.compile(r"^[0-9a-f]{40}$")


# --------------------------------------------------------------------------- #
# Percentiles — mirror bench-file-index `pct()` exactly (nearest-rank, with
# round-half-away-from-zero like Rust's f64::round) so numbers are comparable.
# --------------------------------------------------------------------------- #
def _round_half_away(x: float) -> int:
    return int(math.floor(x + 0.5)) if x >= 0 else int(math.ceil(x - 0.5))


def pct(sorted_vals: list[float], p: float) -> float:
    """Nearest-rank p-th percentile of an already-sorted list."""
    if not sorted_vals:
        return 0.0
    idx = _round_half_away((p / 100.0) * (len(sorted_vals) - 1))
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


def summarize(samples_ms: list[float]) -> dict[str, float]:
    """p50/p95/p99 + min/max/mean over a list of millisecond timings."""
    s = sorted(samples_ms)
    n = len(s)
    return {
        "n": n,
        "p50": pct(s, 50.0),
        "p95": pct(s, 95.0),
        "p99": pct(s, 99.0),
        "min": s[0] if n else 0.0,
        "max": s[-1] if n else 0.0,
        "mean": (sum(s) / n) if n else 0.0,
    }


# --------------------------------------------------------------------------- #
# Query set (TSV: `group<TAB>query`, `#` comments / blank lines ignored).
# --------------------------------------------------------------------------- #
@dataclass
class Query:
    group: str
    query: str


def load_queries(path: str | Path) -> list[Query]:
    out: list[Query] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.rstrip("\n")
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if "\t" in line:
            group, query = line.split("\t", 1)
        else:
            group, query = "default", line
        # Preserve the query verbatim except trailing newline; do NOT strip
        # internal/trailing spaces — they are valid path substrings.
        out.append(Query(group=group.strip(), query=query))
    return out


# --------------------------------------------------------------------------- #
# info_hash normalization (proto field is 20 raw bytes -> 40-char lower hex).
# --------------------------------------------------------------------------- #
def norm_hash(h: str) -> str | None:
    """Normalize a hex info_hash to 40-char lowercase, or None if malformed."""
    s = h.strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    return s if _HEX40.match(s) else None


def bytes_to_hex(b: bytes) -> str:
    return b.hex()


# --------------------------------------------------------------------------- #
# Truth file (recall ground truth). Canonical rev2 format authored by
# recall-engineer (docs/dev/l3-recall-gate-query-set-and-truth.md §6): one JSON
# object with `meta` + `queries[]`, each query carrying `id`, `q`, `class`
# (="recall"), `lang`, `expected` (informational selectivity hint),
# `truth_info_hashes`, `truth_sample_count`, and a `_runtime` block the harness
# fills. A flat {query: [hex,...]} dict is also accepted (ad-hoc / tests).
# --------------------------------------------------------------------------- #
@dataclass
class TruthQuery:
    id: str
    q: str
    lang: str
    truth: set[str]  # normalized 40-hex set from truth_info_hashes
    expected: str | None = None  # informational hint (selective/uncertain/overcap…)
    truth_sample_count: int | None = None
    malformed: int = 0
    raw: dict = field(default_factory=dict)  # original dict for round-trip writeback


@dataclass
class TruthFile:
    meta: dict
    queries: list[TruthQuery]
    raw: dict = field(default_factory=dict)

    @property
    def l3_limit(self) -> int:
        return int(self.meta.get("l3_request", {}).get("limit", MAX_CANDIDATES))

    @property
    def l3_oversample(self) -> int:
        return int(self.meta.get("l3_request", {}).get("oversample", 0))

    @property
    def watermark_margin_secs(self) -> int:
        return int(self.meta.get("watermark_margin_secs", 60))


def _coerce_hashes(raw_list) -> tuple[set[str], int]:
    truth: set[str] = set()
    malformed = 0
    for h in raw_list or []:
        n = norm_hash(str(h))
        if n is None:
            malformed += 1
        else:
            truth.add(n)
    return truth, malformed


def load_truth(path: str | Path) -> TruthFile:
    """Load the canonical recall truth file (rev2): one JSON object with `meta`
    + `queries[]`, each query class `recall` and gated on the 5000-cap. A flat
    {query: [hex,...]} dict is also accepted for ad-hoc use / tests."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))

    # Flat {query: [hex,...]} convenience form.
    if isinstance(data, dict) and "queries" not in data:
        queries = []
        for q, lst in data.items():
            truth, malformed = _coerce_hashes(list(lst))
            raw = {
                "id": q, "q": q, "class": "recall", "lang": "ascii",
                "truth_info_hashes": list(lst), "_runtime": {},
            }
            queries.append(
                TruthQuery(id=q, q=q, lang="ascii", truth=truth, malformed=malformed, raw=raw)
            )
        return TruthFile(meta={}, queries=queries, raw={"queries": [x.raw for x in queries]})

    if not (isinstance(data, dict) and isinstance(data.get("queries"), list)):
        raise ValueError(
            "unrecognized truth JSON: expected canonical {meta, queries:[...]} "
            "or a flat {query: [hex,...]} mapping"
        )

    meta = data.get("meta", {})
    queries: list[TruthQuery] = []
    for item in data["queries"]:
        truth, malformed = _coerce_hashes(item.get("truth_info_hashes", []))
        item.setdefault("_runtime", {})
        queries.append(
            TruthQuery(
                id=item.get("id", item.get("q", "?")),
                q=item["q"],
                lang=item.get("lang", "ascii"),
                truth=truth,
                expected=item.get("expected"),
                truth_sample_count=item.get("truth_sample_count"),
                malformed=malformed,
                raw=item,
            )
        )
    return TruthFile(meta=meta, queries=queries, raw=data)


# --------------------------------------------------------------------------- #
# Sort spec parsing: "field" or "field:desc" / "field:asc".
# --------------------------------------------------------------------------- #
_SORT_FIELDS = {"seeders", "size", "files_count", "published_at"}


def parse_sort(specs: list[str]):
    """Parse ``--sort field[:asc|:desc]`` into proto SortBy messages.

    Returns the list lazily-built against ``search_pb2`` to avoid importing
    generated code at module load.
    """
    from .protos import load  # noqa: PLC0415

    _ps_pb2, _grpc, search_pb2 = load()
    out = []
    for spec in specs:
        field_name, _, order = spec.partition(":")
        field_name = field_name.strip()
        if field_name not in _SORT_FIELDS:
            raise ValueError(
                f"unknown sort field {field_name!r}; valid: {sorted(_SORT_FIELDS)}"
            )
        descending = order.strip().lower() != "asc"  # default desc
        out.append(search_pb2.SortBy(field=field_name, descending=descending))
    return out
