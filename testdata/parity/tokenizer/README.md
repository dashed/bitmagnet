# Tokenizer parity corpus (differential-harness format)

`corpus.jsonl` is the tokenizer subsystem re-expressed in the **Phase-0
differential-harness JSONL schema** so that the Go driver (`internal/parity`)
and the Rust crate (`bitmagnet-rs/crates/bitmagnet-diff`) consume the *same*
file and must agree. It is the "one proven example pair" for Lane C's C2 task.

## Schema (one JSON object per line, LF endings, single trailing newline)

```json
{"id":"<string>","subsystem":"<string>","input":<json>,"expected":<json>}
```

For `subsystem == "tokenizer"`:
- `input`:   `{"text": "<arbitrary string>"}`
- `expected`: `{"tokens": ["<token>", ...]}`  (empty is `[]`, never `null`)

## Provenance

Each line is a 1:1 reshape of
`bitmagnet-rs/crates/bitmagnet-search/tests/fixtures/tokenizer_fixtures.json`
(4223 adversarial cases whose `tokens` were produced by the **real Go**
`internal/database/fts.TokenizeFlat`; see that file's sibling README for the
Go 1.23.6 / Unicode 15.0.0 generation details). `id` is `tok-NNNN` by original
index, so ordering is deterministic. Regenerate with:

```
python3 - <<'PY'
import json
cases = json.load(open("bitmagnet-rs/crates/bitmagnet-search/tests/fixtures/tokenizer_fixtures.json"))
w = len(str(len(cases)-1))
with open("testdata/parity/tokenizer/corpus.jsonl","w",newline="\n") as f:
    for i,c in enumerate(cases):
        rec={"id":f"tok-{i:0{w}d}","subsystem":"tokenizer","input":{"text":c["input"]},"expected":{"tokens":c.get("tokens") or []}}
        f.write(json.dumps(rec,ensure_ascii=False,separators=(",",":"))+"\n")
PY
```

Both `go test ./internal/parity/...` and `cargo test -p bitmagnet-diff` load this
file through their respective harness loaders and assert zero diffs.
