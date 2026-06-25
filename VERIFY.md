# kit4go log4go — End-to-End Verification Report

Verification of `github.com/v8fg/kit4go/log4go` (dev branch, local `replace`)
against every advertised capability. The verify program is a single `main.go`
that exercises console/file/kafka writers, the overflow pipeline, the
ShardLogger, the no-caller fast path, and the webhook alert sink, then prints a
PASS/FAIL summary.

- **Date:** 2026-06-25
- **Go:** 1.26.0 (darwin/arm64, Apple M5 10-core)
- **kit4go ref:** dev branch, local `replace github.com/v8fg/kit4go => ../kit4go`
- **Kafka:** Confluent `cp-kafka:7.7.0` single-node KRaft on `localhost:9092`
  (bitnami/kafka tags were unavailable from the registry at run time; switched
  per the fallback policy in the task brief).
- **Result:** **15/15 checks PASS** (run exit code 0).

## How to reproduce

```bash
make verify     # kafka-up + wait ready + run (all sections)
# or, without Kafka:
make run-no-kafka
```

`make verify` leaves Kafka running so messages can be inspected with
`docker exec kit4go-verify-kafka kafka-console-consumer ...`; run
`make kafka-down` to clean up.

## Verified capabilities

### 1. ConsoleWriter (colored INFO/WARN/ERROR) — PASS
- Colored output confirmed (ANSI codes for INFO=blue, WARN=yellow, ERROR=red).
- `Metrics()` per-level counters: INFO=1, WARN=1, ERROR=1.

### 2. FileWriter (daily rotate) — PASS
- File `/tmp/kit4go-verify-20260625.log` created (4292 bytes).
- Content check: 50/50 marker lines present (`KIT4GO_VERIFY_FILE_MARKER`).
- Pattern: `kit4go-verify-%Y%M%D.log` with `Rotate=true, Daily=true`.

### 3. KafKaWriter (100 records → topic `verify-log`) — PASS
- `Metrics.Sent=100` (all 100 records handed to the producer).
- `Metrics.Errored=0` (no producer errors).
- `kafka-console-consumer` inside the container consumed 10/10 sampled messages.
- Sample message (JSON payload built by `KafKaWriter.buildPayload`):
  ```json
  {"es_index":"kit4go-verify","file":"main.go:291","level":"INFO",
   "message":"kafka verify msg 000 from kit4go-verify","now":1782349419,
   "server_ip":"127.0.0.1","timestamp":"2026-06-25T09:03:39.170+0800"}
  ```

### 4. Overflow (spill ring → file → drop, no OOM) — PASS
Burst of 50000 records into an async FileWriter with `AsyncBufferSize=8`,
`OverflowPolicy=spill`, `SpillType=""` (chain: ring → file):
- No OOM / no crash: processed 50000 records.
- `Metrics`: `written=206 dropped=0 spilled=49795 errored=0 queued=0 spillLen=0`.
- Spill engaged (`spilled=49795`); the bounded ring/file store absorbed the
  burst. `errored=0` confirms the async write path stayed healthy.
- Bounded: `written+dropped` within `burst+slack`.

### 5. ShardLogger (multi-core) — PASS
- 4 shards × 1000 msgs/worker (4 concurrent goroutines) = 4000 records
  distributed round-robin, no goroutine leak, clean parallel `Close()`.

### 6. No-caller mode (WithCaller(false)) — PASS
- `WithCaller(false)` accepted; records delivered with an empty `file:line`
  field (the throughput path that skips `runtime.Caller`).

### 7. Webhook alert (httptest mock) — PASS
- Burst of 20000 records into an async FileWriter with `AsyncBufferSize=4`,
  `OverflowPolicy=drop`, `SetAlertSink(WebhookAlertSink)`.
- Overflow drops fired alerts; the mock httptest server received **9 POSTs**
  (Lark text formatter payload). The sink is async, bounded, and non-blocking —
  it did not stall the log path.

## Findings / sharp edges (reported upstream, not blockers)

These are real-world usability issues uncovered by the e2e run. They are
worked around in `main.go` and should be addressed in log4go itself (tracked
in the kit4go dev branch work).

1. **KafKaWriter.ProducerTimeout defaults to 0 → sarama rejects the config.**
   `NewKafKaWriter` passes `cfg.Producer.Timeout = k.options.ProducerTimeout`
   unchanged; a zero `ProducerTimeout` makes `sarama.NewAsyncProducer` fail
   with `Producer.Timeout must be > 0`. There should be a sane default
   (e.g. 5s) when the user omits it. *(kit4go fix pending in the usability pass.)*

2. **async FileWriter never opens the file when the filename has no `%` pattern.**
   In async mode `Rotate()` is a no-op, so the file is opened lazily by the
   daemon's first `writeOne → rotateImpl`. But `rotateImpl` only opens a file
   when `rotate==true`, and with no `%` pattern the actions slice is empty so
   `rotate` stays false → every record hits the `fileBufWriter == nil` error
   branch (`Metrics.Errored` climbs, nothing is written). A bare filename like
   `app.log` silently drops everything in async mode. Workaround: use a dated
   pattern (`app-%Y%M%D.log`). *(kit4go fix pending.)*

3. **No way to detach a writer from a live Logger / KafKaWriter.Stop() races the
   bootstrap goroutine.** Calling `KafKaWriter.Stop()` while the writer is still
   registered on a Logger closes the producer's input channel; any subsequent
   `log.Info(...)` on that Logger panics with `send on closed channel`. There is
   no `Unregister(w Writer)` and `Logger.Close()` does not stop async writers
   (KafKaWriter is not a `Flusher`). For a long-lived process this is usually
   fine (writers live as long as the logger), but it makes a clean per-writer
   shutdown impossible. Documented here so callers know to keep writers
   registered for the logger's lifetime.

## File layout

- `main.go` — the verifier (single file, ~500 lines, heavily commented).
- `docker-compose.yml` — single-node Kafka (KRaft) on :9092.
- `Makefile` — `kafka-up/down/ready`, `run`, `run-no-kafka`, `verify`, `clean`.
- `go.mod` — `replace github.com/v8fg/kit4go => ../kit4go` (validates the
  latest local code, never published).
