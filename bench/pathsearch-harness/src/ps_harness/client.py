"""Thin gRPC client for the L3 PathSearchService (plaintext)."""

from __future__ import annotations

import time
from dataclasses import dataclass

import grpc

from .protos import load


@dataclass
class CandidatesResult:
    candidates_hex: list[str]  # returned info_hashes, lowercase hex, in server order
    candidate_total: int  # exact (uncapped) match count
    estimated: bool
    elapsed_ms: float  # wall-clock of the unary RPC


@dataclass
class HealthResult:
    status: int
    doc_count: int
    index_bytes: int
    watermark_epoch: int
    writable: bool


class PathSearchClient:
    """Single, reusable, plaintext channel to ``bitmagnet-pathsearch``."""

    def __init__(self, addr: str, timeout: float = 30.0):
        self.addr = addr
        self.timeout = timeout
        self._ps_pb2, ps_grpc, self._search_pb2 = load()
        # Reasonable headroom though responses are tiny (<=5000 * ~36 bytes).
        opts = [
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ("grpc.max_send_message_length", 16 * 1024 * 1024),
        ]
        self._channel = grpc.insecure_channel(addr, options=opts)
        self._stub = ps_grpc.PathSearchServiceStub(self._channel)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> "PathSearchClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def wait_ready(self, timeout: float = 10.0) -> None:
        grpc.channel_ready_future(self._channel).result(timeout=timeout)

    def path_candidates(
        self,
        query: str,
        limit: int,
        oversample: int,
        sort=None,
    ) -> CandidatesResult:
        req = self._ps_pb2.PathCandidatesRequest(
            query=query,
            limit=limit,
            oversample=oversample,
            sort=list(sort) if sort else [],
        )
        t0 = time.perf_counter_ns()
        resp = self._stub.PathCandidates(req, timeout=self.timeout)
        elapsed_ms = (time.perf_counter_ns() - t0) / 1e6
        return CandidatesResult(
            candidates_hex=[c.info_hash.hex() for c in resp.candidates],
            candidate_total=int(resp.candidate_total),
            estimated=bool(resp.estimated),
            elapsed_ms=elapsed_ms,
        )

    def health(self) -> HealthResult:
        resp = self._stub.HealthCheck(
            self._search_pb2.HealthCheckRequest(), timeout=self.timeout
        )
        return HealthResult(
            status=int(resp.status),
            doc_count=int(resp.doc_count),
            index_bytes=int(resp.index_bytes),
            watermark_epoch=int(resp.watermark_epoch),
            writable=bool(resp.writable),
        )
