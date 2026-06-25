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

## Large-scale stress (1M / 10M / 100M)

`stress/` drives log4go at 1M / 10M / 100M record volumes against a per-tier
async FileWriter and records wall time / QPS, log4go metrics (Written / Dropped
/ Spilled / Errored), process memory (sampled every 100k records), and file /
disk usage. The report is written to `STRESS.md`.

```bash
make stress          # 1M + 10M + 100M file tiers (no Kafka)
make stress-kafka    # also run the small (10k) Kafka tier
go run ./stress -tiers 1M,10M   # subset
```

Stress results and the log4go sharp-edges it surfaced: [STRESS.md](STRESS.md).

## Quick start

```bash
# full e2e with a throwaway Kafka (Confluent cp-kafka, KRaft, :9092)
make verify

# without Kafka (sections 1,2,4,5,6,7 still run)
make run-no-kafka
```

Requirements: Go 1.26+, Docker (for the Kafka section only). The stress run
needs ~8 GB transient free disk for the 100M tier (temp files are deleted per
tier).

## Layout

- `main.go` — the verifier.
- `stress/` — the large-scale stress harness (writes `STRESS.md`).
- `docker-compose.yml` — single-node Kafka (KRaft) on :9092.
- `Makefile` — `kafka-up/down/ready`, `run`, `run-no-kafka`, `verify`, `stress`, `clean`.

The `go.mod` uses `replace github.com/v8fg/kit4go => ../kit4go`, so clone
`v8fg/kit4go` as a sibling directory for the harness to resolve.
