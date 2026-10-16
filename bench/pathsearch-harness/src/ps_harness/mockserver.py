"""In-process mock PathSearchService for offline harness validation.

Reproduces the server's relevant semantics so the harness can be exercised
end-to-end with no production contact:

* lowercases query + indexed paths, ngram(2,3) conjunction substring match
  (here simplified to a faithful case-insensitive substring test — for queries
  >= 2 chars an ngram(2,3) conjunction matches exactly the contiguous-substring
  set on a per-value path-bag, so substring is the correct ground truth);
* query < 2 chars => empty (the guard);
* candidate_total = full match count (uncapped);
* returned candidates clamped to limit+oversample then MAX_CANDIDATES=5000.

NOT for production use — a test double only.
"""

from __future__ import annotations

from concurrent import futures

import grpc

from .core import MAX_CANDIDATES, MIN_QUERY_CHARS
from .protos import load


def _clamp(limit: int, oversample: int) -> int:
    limit = limit if limit else 50
    oversample = oversample if oversample else 200
    return min(limit + oversample, MAX_CANDIDATES)


class MockPathSearch:
    """Build a servicer from {info_hash_hex: [paths]}."""

    def __init__(self, docs: dict[str, list[str]]):
        # store lowercased paths per doc
        self.docs = {h.lower(): [p.lower() for p in paths] for h, paths in docs.items()}
        ps_pb2, ps_grpc, _search = load()
        self._ps_pb2 = ps_pb2
        self._base = ps_grpc.PathSearchServiceServicer
        self._add = ps_grpc.add_PathSearchServiceServicer_to_server

    def _match(self, query: str) -> list[str]:
        q = query.strip().lower()
        if len(q) < MIN_QUERY_CHARS:
            return []
        hits = []
        for h, paths in self.docs.items():
            if any(q in p for p in paths):
                hits.append(h)
        return sorted(hits)  # deterministic order

    def servicer(self):
        outer = self

        class _Svc(outer._base):
            def PathCandidates(self, request, context):
                hits = outer._match(request.query)
                total = len(hits)
                n = _clamp(request.limit, request.oversample)
                page = hits[:n]
                return outer._ps_pb2.PathCandidatesResponse(
                    candidates=[
                        outer._ps_pb2.PathCandidate(
                            info_hash=bytes.fromhex(h), score=1.0, sort_value=0
                        )
                        for h in page
                    ],
                    candidate_total=total,
                    estimated=True,
                )

            def HealthCheck(self, request, context):
                return outer._ps_pb2.PathSearchHealth(
                    status=1,
                    doc_count=len(outer.docs),
                    index_bytes=12345,
                    watermark_epoch=1_700_000_000,
                    writable=True,
                )

        return _Svc()

    def serve(self, addr: str = "127.0.0.1:0") -> tuple[grpc.Server, str]:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        self._add(self.servicer(), server)
        port = server.add_insecure_port(addr)
        server.start()
        host = addr.rsplit(":", 1)[0]
        return server, f"{host}:{port}"
