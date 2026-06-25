# kit4go-verify

End-to-end verification harness for [`github.com/v8fg/kit4go`](https://github.com/v8fg/kit4go)
`log4go` package. It validates the **latest, unreleased** code via a local
`replace` directive, so changes in `../kit4go/log4go` are exercised before
they ship.

## What it checks

A single run exercises every advertised log4go capability and prints a
PASS/FAIL summary:

| # | Capability | What's verified |
|---|------------|-----------------|
| 1 | ConsoleWriter | colored INFO/WARN/ERROR + per-level `Metrics` |
| 2 | FileWriter | daily-rotate file creation + content |
| 3 | KafKaWriter | 100 records to Kafka; `Metrics.Sent/Errored`; console-consumer sample |
| 4 | Overflow | tiny buffer + 50k burst → spill ring/file; no OOM; `Metrics` sane |
| 5 | ShardLogger | 4-shard concurrent fan-out |
| 6 | No-caller | `WithCaller(false)` fast path |
| 7 | Webhook alert | mock httptest server receives overflow-alert POSTs |

Full results and findings: [VERIFY.md](VERIFY.md).

## Quick start

```bash
# full e2e with a throwaway Kafka (Confluent cp-kafka, KRaft, :9092)
make verify

# without Kafka (sections 1,2,4,5,6,7 still run)
make run-no-kafka
```

Requirements: Go 1.26+, Docker (for the Kafka section only).

## Layout

- `main.go` — the verifier.
- `docker-compose.yml` — single-node Kafka (KRaft) on :9092.
- `Makefile` — `kafka-up/down/ready`, `run`, `run-no-kafka`, `verify`, `clean`.

The `go.mod` uses `replace github.com/v8fg/kit4go => ../kit4go`, so clone
`v8fg/kit4go` as a sibling directory for the harness to resolve.
