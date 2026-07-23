# Lane R — release-name parsing

Owns release-name parsing: the frozen enum vocabularies and the parse output
surface (the two distinct images — do not conflate them), guarding the
alias-precedence determinism trap that is the fidelity risk here. Feeds Lane C
(release-parse output → classifier).

Contract: [`docs/dev/rust-rewrite/phase3-contracts.md`](../../../docs/dev/rust-rewrite/phase3-contracts.md) §3 (Release-parse output shape).
