# Lane C — content classifier

Owns the classifier: the frozen classification corpus (the oracle), the
purity precondition (Phase-0 C1), the result/outcome types, the CEL engine +
action/condition vocabulary, and the validation harness. Consumes Lane R's
release-parse output.

Contract: [`docs/dev/rust-rewrite/phase3-contracts.md`](../../../docs/dev/rust-rewrite/phase3-contracts.md) §2 (Classifier corpus contract).
