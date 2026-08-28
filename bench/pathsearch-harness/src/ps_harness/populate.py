"""Populate the truth file's ``truth_info_hashes`` from PostgreSQL — the §5 rev2
ground-truth SQL (recall-engineer). LEAD-GATED (H4), read-only.

For each query, runs the page-sampled + freshness-filtered truth query:

    SELECT DISTINCT encode(s.info_hash,'hex')
    FROM torrent_files TABLESAMPLE SYSTEM (p) REPEATABLE (seed) s
    JOIN torrents t ON t.info_hash = s.info_hash
    WHERE position(lower($1) IN lower(s.path)) > 0
      AND t.updated_at <= to_timestamp($2)         -- $2 = watermark_bound_epoch
    LIMIT 500;

``TABLESAMPLE SYSTEM (p) REPEATABLE (seed)`` reads ~p% of physical PAGES (genuine
bounded I/O — a PK info_hash-range slice does NOT bound I/O for SHA-1 keys), with
the same sampled pages across all queries. The freshness filter uses the L3
follow watermark (``watermark_bound_epoch`` = run-start ``watermark_epoch`` −
margin) so truth only contains torrents L3 has already indexed → a miss is never
staleness.

Safety (access-engineer): ONE serial connection; ``statement_timeout`` set right
after connect (default 15s — the sidecar has MAX_CONNECTIONS=2, do not contend);
the password comes from ``PGPASSWORD`` only (never the DSN/argv).
"""

from __future__ import annotations

import sys

from .core import TruthFile

# p (sample %) and seed are TABLESAMPLE literals (not bindable) → validated as
# numbers and formatted in; $1 (query) and $2 (epoch) are bound parameters.
TRUTH_SQL_TMPL = """
SELECT DISTINCT encode(s.info_hash, 'hex') AS info_hash_hex
FROM torrent_files TABLESAMPLE SYSTEM ({pct}) REPEATABLE ({seed}) s
JOIN torrents t ON t.info_hash = s.info_hash
WHERE position(lower(%(q)s) IN lower(s.path)) > 0
  AND t.updated_at <= to_timestamp(%(wm)s)
LIMIT %(limit)s
"""

# Variant without the freshness JOIN (only when --no-freshness is given).
TRUTH_SQL_NOFRESH_TMPL = """
SELECT DISTINCT encode(s.info_hash, 'hex') AS info_hash_hex
FROM torrent_files TABLESAMPLE SYSTEM ({pct}) REPEATABLE ({seed}) s
WHERE position(lower(%(q)s) IN lower(s.path)) > 0
LIMIT %(limit)s
"""


def _dsn_has_password(dsn: str) -> bool:
    """True if the DSN embeds a password (keyword ``password=`` or URL userinfo
    ``user:pass@``). The password MUST come from PGPASSWORD only."""
    from urllib.parse import urlsplit

    if "password=" in dsn.lower():
        return True
    try:
        parts = urlsplit(dsn)
        if parts.scheme and parts.netloc:
            userinfo = parts.netloc.rsplit("@", 1)[0] if "@" in parts.netloc else ""
            if ":" in userinfo:
                return True
    except ValueError:
        pass
    return False


def populate(
    truth: TruthFile,
    *,
    dsn: str,
    watermark_bound_epoch: int | None,
    statement_timeout_ms: int = 15_000,
    sample_pct: float = 2.0,
    sample_seed: int = 4242,
    limit: int | None = None,
    no_freshness: bool = False,
) -> dict:
    import psycopg  # lazy: only needed for populate

    if _dsn_has_password(dsn):
        raise ValueError(
            "refusing to run: the DSN appears to contain a password. "
            "Pass --pg without a password and set PGPASSWORD in the environment."
        )
    if not no_freshness and watermark_bound_epoch is None:
        raise ValueError(
            "watermark_bound_epoch is required for the freshness filter. "
            "Pass --watermark-bound-epoch, or --grpc ADDR to read it from "
            "HealthCheck, or --no-freshness to skip (NOT recommended — risks "
            "false misses from not-yet-indexed torrents)."
        )

    pct = float(sample_pct)
    if not (0.0 < pct <= 100.0):
        raise ValueError(f"--sample-pct must be in (0, 100], got {pct}")
    seed = int(sample_seed)
    lim = int(limit or truth.meta.get("limit_per_query", 500))
    tmpl = TRUTH_SQL_NOFRESH_TMPL if no_freshness else TRUTH_SQL_TMPL
    sql = tmpl.format(pct=pct, seed=seed)

    if watermark_bound_epoch is not None:
        truth.meta["watermark_bound_epoch"] = watermark_bound_epoch
        if isinstance(truth.raw, dict):
            truth.raw.setdefault("meta", {})["watermark_bound_epoch"] = watermark_bound_epoch

    report = {"filled": [], "errors": []}
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # SET is a utility statement — PostgreSQL does NOT allow bind params
            # here ("syntax error at or near $1"), so format an int literal in.
            # int() makes it injection-safe (units = milliseconds).
            cur.execute(f"SET statement_timeout = {int(statement_timeout_ms)}")
            for tq in truth.queries:
                params = {"q": tq.q, "limit": lim}
                if not no_freshness:
                    params["wm"] = watermark_bound_epoch
                try:
                    cur.execute(sql, params)
                    hashes = [r[0].lower() for r in cur.fetchall()]
                    tq.raw["truth_info_hashes"] = hashes
                    tq.raw["truth_sample_count"] = len(hashes)
                    tq.truth = set(hashes)
                    tq.truth_sample_count = len(hashes)
                    report["filled"].append((tq.id, len(hashes)))
                    print(f"[truth] {tq.id} q={tq.q!r}: {len(hashes)} hashes", file=sys.stderr)
                except psycopg.errors.QueryCanceled:
                    report["errors"].append((tq.id, "timeout"))
                    print(
                        f"[truth] {tq.id}: TIMEOUT (>{statement_timeout_ms}ms) — "
                        "lower --sample-pct or raise --statement-timeout-ms off-peak",
                        file=sys.stderr,
                    )
    return report
