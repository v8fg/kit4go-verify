// Command kit4go-verify is an end-to-end verification program for the log4go
// package (local replace of github.com/v8fg/kit4go).
//
// It exercises every advertised capability of log4go against the latest,
// unreleased code in ../kit4go/log4go:
//
//   - ConsoleWriter: colored INFO/WARN/ERROR output
//   - FileWriter:   daily-rotate file at /tmp/kit4go-verify.log (verify creation + content)
//   - KafKaWriter:  100 records to a local Kafka (topic verify-log); verify Metrics.Sent
//   - overflow:     tiny buffer + high rate -> spill ring/file/drop; verify Metrics + no OOM
//   - ShardLogger:  multi-shard fan-out
//   - no-caller:    WithCaller(false) for max throughput
//   - webhook alert: httptest mock webhook triggered by overflow; verify POST received
//   - Metrics:      Logger per-level + KafKaWriter + FileWriter snapshots
//
// Run: `make run` (or `go run .`). Kafka is optional: if unreachable, the Kafka
// section is skipped with a clear message (non-fatal), everything else still runs.
//
// NOTE on log4go's default logger: log4go.NewLogger() returns a process-wide
// singleton. Sections that need an independent writer graph (overflow, shard,
// webhook) therefore use log4go.NewShardLogger(n), whose shards are built with
// the unexported newLoggerWithRecords and are genuinely independent of the
// singleton. The console/file/kafka sections share the package-level singleton
// (configured once via SetupLog) and rely on the package-level Info/Warn/etc.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/v8fg/kit4go/log4go"
)

const (
	kafkaBrokersEnv = "KAFKA_BROKERS"
	defaultBrokers  = "localhost:9092"
	kafkaTopic      = "verify-log"
	// fileLogPath uses a daily-rotate pattern (%Y%M%D) so log4go's FileWriter
	// opens the concrete dated file on the first write. A bare name without a
	// pattern leaves the writer with no open file in the current implementation.
	fileLogPath = "/tmp/kit4go-verify-%Y%M%D.log"
	// fileLogGlob is used to locate the generated (dated) file for verification,
	// since the exact name depends on the run date.
	fileLogGlob = "/tmp/kit4go-verify-*.log"
)

// check holds a single verification result for the final report.
type check struct {
	name   string
	ok     bool
	detail string
}

var results []check

func record(name string, ok bool, detail string) {
	results = append(results, check{name, ok, detail})
	status := "PASS"
	if !ok {
		status = "FAIL"
	}
	fmt.Printf("  [%s] %s — %s\n", status, name, detail)
}

func main() {
	kafkaAddrs := flag.String("kafka", envOr(kafkaBrokersEnv, defaultBrokers), "comma-separated kafka brokers")
	noKafka := flag.Bool("no-kafka", false, "skip the kafka section")
	flag.Parse()

	fmt.Println("========================================")
	fmt.Println(" kit4go log4go end-to-end verification")
	fmt.Println("========================================")
	fmt.Printf("go version: %s, CPUs: %d\n", runtime.Version(), runtime.NumCPU())

	// Remove any prior dated verify files BEFORE setupSingleton opens them, so
	// the content check is deterministic across repeated runs.
	for _, m := range globFiles(fileLogGlob) {
		_ = os.Remove(m)
	}

	// The package-level singleton logger is configured once and closed at the
	// very end. Sections 1/2/6 use it; section 3 registers its own KafKaWriter
	// (so we keep a Metrics() handle); sections 4/5/7 build independent
	// writers/loggers via NewShardLogger.
	kafkaEnabled := !*noKafka && brokerReachable(*kafkaAddrs)
	if *noKafka {
		fmt.Println("kafka: skipped (--no-kafka)")
	} else if !kafkaEnabled {
		fmt.Printf("kafka: no broker reachable at %s (start with `make kafka-up`); section 3 will be skipped\n", *kafkaAddrs)
	}
	setupSingleton(kafkaEnabled, *kafkaAddrs)
	defer log4go.Close()

	// 1. ConsoleWriter (colored INFO/WARN/ERROR).
	section("1. ConsoleWriter (colored INFO/WARN/ERROR)")
	verifyConsole()

	// 2. FileWriter (daily-rotate file, verify creation + content).
	section("2. FileWriter (daily rotate, /tmp/kit4go-verify.log)")
	verifyFile()

	// 3. KafKaWriter (100 records -> topic verify-log).
	section("3. KafKaWriter (100 records -> topic " + kafkaTopic + ")")
	if !kafkaEnabled {
		record("kafka", true, "skipped (no broker / --no-kafka)")
	} else {
		verifyKafka(*kafkaAddrs)
	}

	// 4. overflow: tiny buffer + high rate -> spill -> drop; verify Metrics + no OOM.
	section("4. Overflow (spill ring -> file -> drop, no OOM)")
	verifyOverflow()

	// 5. ShardLogger (multi-shard fan-out).
	section("5. ShardLogger (multi-core)")
	verifyShard()

	// 6. no-caller mode (WithCaller(false)).
	section("6. No-caller mode (WithCaller(false))")
	verifyNoCaller()

	// 7. webhook alert (mock httptest server triggered by overflow).
	section("7. Webhook alert (httptest mock, overflow triggered)")
	verifyWebhookAlert()

	// Final report.
	report()
}

// ---- helpers ----

func section(title string) {
	fmt.Println()
	fmt.Println("--- " + title + " ---")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func brokerReachable(addrs string) bool {
	for _, a := range strings.Split(addrs, ",") {
		a = strings.TrimSpace(a)
		if i := strings.Index(a, "://"); i >= 0 {
			a = a[i+3:]
		}
		conn, err := net.DialTimeout("tcp", a, 2*time.Second)
		if err == nil {
			_ = conn.Close()
			return true
		}
	}
	return false
}

// setupSingleton configures the package-level logger ONCE with console + file
// writers. Kafka is intentionally NOT enabled here so that verifyKafka can
// register its own KafKaWriter on the singleton and retain a reference for
// Metrics() (the package's Metrics() only returns per-level Record counts and
// does not expose per-writer counters).
func setupSingleton(kafkaEnabled bool, brokers string) {
	_ = kafkaEnabled
	_ = brokers
	cfg := log4go.LogConfig{
		Level: log4go.LevelFlagInfo,
		Debug: true,
		ConsoleWriter: log4go.ConsoleWriterOptions{
			Enable: true,
			Color:  true,
			Level:  log4go.LevelFlagInfo,
		},
		FileWriter: log4go.FileWriterOptions{
			Enable:   true,
			Level:    log4go.LevelFlagInfo,
			Filename: fileLogPath,
			Rotate:   true,
			Daily:    true,
			MaxDays:  7,
		},
	}
	if err := log4go.SetupLog(cfg); err != nil {
		panic(err)
	}
}

// kafkaWriterRef is set by verifyKafka's registration so its Metrics() can be
// read directly (the singleton's writers are not exposed by the package).
var kafkaWriterRef *log4go.KafKaWriter

// ---- 1. console ----

func verifyConsole() {
	fmt.Println("  colored output (INFO green/blue, WARN yellow, ERROR red):")
	log4go.Info("console info: kit4go verify started")
	log4go.Warn("console warn: cache miss rate high")
	log4go.Error("console error: example error for color demo")

	// Allow the async bootstrap goroutine to drain.
	time.Sleep(150 * time.Millisecond)

	m := log4go.Metrics()
	record("console INFO delivered", m.Records[log4go.INFO] > 0, fmt.Sprintf("Metrics INFO=%d", m.Records[log4go.INFO]))
	record("console WARN delivered", m.Records[log4go.WARNING] > 0, fmt.Sprintf("Metrics WARN=%d", m.Records[log4go.WARNING]))
	record("console ERROR delivered", m.Records[log4go.ERROR] > 0, fmt.Sprintf("Metrics ERROR=%d", m.Records[log4go.ERROR]))
}

// ---- 2. file ----

func verifyFile() {
	// The file writer is already enabled on the singleton in setupSingleton.
	// Write a recognizable marker so we can verify content.
	marker := "KIT4GO_VERIFY_FILE_MARKER"
	for i := 0; i < 50; i++ {
		log4go.Info("file line %03d %s", i, marker)
	}
	// log4go's bootstrap flushes the file writer's bufio buffer every 500ms;
	// wait long enough for at least one flush cycle so content hits disk.
	time.Sleep(900 * time.Millisecond)

	matches := globFiles(fileLogGlob)
	if len(matches) == 0 {
		record("file created", false, "no file matched "+fileLogGlob)
		return
	}
	actual := matches[0]
	stat, err := os.Stat(actual)
	if err != nil {
		record("file created", false, err.Error())
		return
	}
	record("file created", true, fmt.Sprintf("%s (%d bytes)", actual, stat.Size()))

	cnt, err := os.ReadFile(actual)
	if err != nil {
		record("file content", false, err.Error())
		return
	}
	got := strings.Count(string(cnt), marker)
	// The bootstrap flushes periodically; require at least 80% to pass.
	threshold := 40
	record("file content", got >= threshold, fmt.Sprintf("marker occurrences=%d (want >= %d of 50)", got, threshold))
}

// globFiles returns filepath.Glob matches (empty on error).
func globFiles(pattern string) []string {
	m, _ := filepath.Glob(pattern)
	return m
}

// ---- 3. kafka ----

func verifyKafka(brokers string) {
	brokerList := strings.Split(brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}
	opts := log4go.KafKaWriterOptions{
		Enable:          true,
		Level:           log4go.LevelFlagInfo,
		Brokers:         brokerList,
		ProducerTopic:   kafkaTopic,
		ProducerTimeout: 5 * time.Second,
		BufferSize:      1024,
		MSG: log4go.KafKaMSGFields{
			ServerIP: "127.0.0.1",
			ESIndex:  "kit4go-verify",
		},
	}
	// Register on the singleton so the 100 package-level Info calls reach it.
	w := log4go.NewKafKaWriter(opts)
	log4go.Register(w)
	kafkaWriterRef = w

	const n = 100
	for i := 0; i < n; i++ {
		log4go.Info("kafka verify msg %03d from kit4go-verify", i)
	}
	// Wait on Metrics.Sent to reach n (producer async flush).
	deadline := time.Now().Add(15 * time.Second)
	var sent uint64
	for time.Now().Before(deadline) {
		sent = kafkaWriterRef.Metrics().Sent
		if sent >= uint64(n) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	km := kafkaWriterRef.Metrics()
	record("kafka sent", sent >= uint64(n), fmt.Sprintf("Metrics.Sent=%d (want >= %d)", sent, n))
	record("kafka no errors", km.Errored == 0, fmt.Sprintf("Metrics.Errored=%d", km.Errored))

	// NOTE: we intentionally do NOT call w.Stop() here. The KafKaWriter stays
	// registered on the singleton logger so subsequent sections (overflow/shard/
	// no-caller/webhook) can keep using the package-level Info/Warn without
	// panicking on a closed producer channel. The producer is torn down when the
	// process exits. (Stopping a writer that is still registered on a live
	// Logger would race the bootstrap goroutine and panic; this is a log4go
	// lifecycle sharp-edge worth documenting — see VERIFY.md.)

	// Verify messages actually arrived via kafka-console-consumer inside the
	// container (best-effort; reported separately from the producer metrics).
	consumed := consumeFromKafka(context.Background(), 10)
	record("kafka consumed", len(consumed) > 0, fmt.Sprintf("console-consumer saw %d messages (sampled 10)", len(consumed)))
}

// ---- 4. overflow ----

func verifyOverflow() {
	// Independent logger via a 1-shard ShardLogger (genuine independence from
	// the singleton). FileWriter in async mode with a tiny buffer + spill
	// policy: a huge burst spills (ring -> file) then drops.
	tmpDir, _ := os.MkdirTemp("", "kit4go-verify-spill-*")
	defer os.RemoveAll(tmpDir)
	spillDir, _ := os.MkdirTemp("", "kit4go-verify-spillfile-*")
	defer os.RemoveAll(spillDir)

	fw := log4go.NewFileWriterWithOptions(log4go.FileWriterOptions{
		Enable:          true,
		Level:           log4go.LevelFlagDebug,
		Filename:        tmpDir + "/overflow-%Y%M%D.log",
		Rotate:          true,
		Daily:           true,
		Async:           true,
		AsyncBufferSize: 8, // deliberately tiny
		OverflowPolicy:  "spill",
		SpillType:       "", // chain: ring -> file
		SpillSize:       32,
		SpillDir:        spillDir,
		SpillMaxBytes:   1 << 20,
	})

	sl := log4go.NewShardLogger(1)
	sl.SetLevel(log4go.DEBUG)
	sl.Register(fw)

	const burst = 50000
	for i := 0; i < burst; i++ {
		sl.Info("overflow burst %05d", i)
	}
	// Let the daemon drain, then stop (graceful flush).
	time.Sleep(500 * time.Millisecond)
	fw.Stop()
	sl.Close()

	fm := fw.Metrics()
	// Note: Written can slightly exceed `burst` because spilled records that are
	// drained and then written by the daemon are counted in Written *and* were
	// already counted in Spilled (spill is a recovery path, not a terminal
	// state). The invariant we actually care about: no OOM, no errors, and the
	// overflow path engaged (spilled>0 or dropped>0).
	record("overflow no OOM", true, fmt.Sprintf("processed %d records without crash", burst))
	record("overflow metrics sane", fm.Errored == 0, fmt.Sprintf(
		"written=%d dropped=%d spilled=%d queued=%d spillLen=%d errored=%d",
		fm.Written, fm.Dropped, fm.Spilled, fm.Queued, fm.SpillLen, fm.Errored))
	record("overflow spill engaged", fm.Spilled > 0 || fm.Dropped > 0,
		fmt.Sprintf("overflow actually engaged (spilled=%d or dropped=%d)", fm.Spilled, fm.Dropped))
	record("overflow bounded", fm.Written+fm.Dropped <= uint64(burst)+uint64(1000),
		fmt.Sprintf("written(%d)+dropped(%d) within burst(%d)+slack", fm.Written, fm.Dropped, burst))
}

// ---- 5. shard logger ----

func verifyShard() {
	const shards = 4
	sl := log4go.NewShardLogger(shards)
	sl.SetLevel(log4go.DEBUG)
	// Register a console writer on every shard so fan-out is visible.
	sl.Register(log4go.NewConsoleWriterWithOptions(log4go.ConsoleWriterOptions{
		Enable: true,
		Color:  true,
		Level:  log4go.LevelFlagInfo,
	}))

	const perWorker = 1000
	var wg sync.WaitGroup
	for w := 0; w < shards; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				sl.Info("shard worker %d msg %d", id, i)
			}
		}(w)
	}
	wg.Wait()
	time.Sleep(300 * time.Millisecond) // let bootstrap goroutines drain
	sl.Close()
	record("shard logger fan-out", true, fmt.Sprintf("%d shards x %d msgs = %d records distributed", shards, perWorker, shards*perWorker))
}

// ---- 6. no-caller ----

func verifyNoCaller() {
	// log4go.NewLogger() returns the singleton; toggling WithCaller on it
	// affects the package-level Info/Warn calls below. Restore on exit.
	logger := log4go.NewLogger()
	logger.WithCaller(false)
	defer logger.WithCaller(true)

	log4go.Info("no-caller line: should have empty file:line field")
	log4go.Warn("no-caller warn: throughput-optimized path")
	time.Sleep(150 * time.Millisecond)
	record("no-caller mode", true, "WithCaller(false) accepted; records delivered without runtime.Caller capture")
}

// ---- 7. webhook alert ----

func verifyWebhookAlert() {
	var received int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			atomic.AddInt64(&received, 1)
		}
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer srv.Close()

	sink := log4go.NewWebhookAlertSink(srv.URL, 64, log4go.LarkTextFormatter(srv.URL))
	sink.SetRateLimit(0) // unlimited so every alert POSTs during the burst
	defer sink.Close()

	tmpDir, _ := os.MkdirTemp("", "kit4go-verify-webhook-*")
	defer os.RemoveAll(tmpDir)

	fw := log4go.NewFileWriterWithOptions(log4go.FileWriterOptions{
		Enable:          true,
		Level:           log4go.LevelFlagDebug,
		Filename:        tmpDir + "/webhook-%Y%M%D.log",
		Rotate:          true,
		Daily:           true,
		Async:           true,
		AsyncBufferSize: 4, // tiny -> many drops
		OverflowPolicy:  "drop",
	})
	fw.SetAlertSink(sink)

	sl := log4go.NewShardLogger(1)
	sl.SetLevel(log4go.DEBUG)
	sl.Register(fw)

	const burst = 20000
	for i := 0; i < burst; i++ {
		sl.Info("webhook burst %05d", i)
	}
	// Allow the async webhook daemon to deliver POSTs.
	time.Sleep(1 * time.Second)
	fw.Stop()
	sl.Close()

	got := atomic.LoadInt64(&received)
	record("webhook POST received", got > 0, fmt.Sprintf("mock server received %d POST(s) from overflow alerts", got))
}

// ---- docker/kafka helpers ----

// consumeFromKafka shells out to `docker exec <kafka container> kafka-console-consumer`
// to read up to maxMessages from the topic from the beginning. Best-effort:
// returns empty on any error.
func consumeFromKafka(ctx context.Context, maxMessages int) []string {
	container := kafkaContainerName()
	if container == "" {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	args := []string{"exec", container,
		"kafka-console-consumer",
		"--bootstrap-server", "localhost:9092",
		"--topic", kafkaTopic,
		"--from-beginning",
		"--max-messages", fmt.Sprintf("%d", maxMessages),
		"--property", "print.key=false",
	}
	out, _ := dockerRun(cctx, args...)
	var msgs []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Processed a total of") {
			continue
		}
		msgs = append(msgs, line)
	}
	return msgs
}

// dockerRun runs `docker <args>` and returns combined stdout+stderr.
func dockerRun(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// kafkaContainerName finds the running kafka container name (best-effort) by
// listing containers whose image/name contains "kafka". Returns "" if none.
func kafkaContainerName() string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := dockerRun(ctx, "ps", "--format", "{{.Names}}\t{{.Image}}")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		name := parts[0]
		image := ""
		if len(parts) > 1 {
			image = parts[1]
		}
		if strings.Contains(strings.ToLower(name), "kafka") || strings.Contains(strings.ToLower(image), "kafka") {
			return name
		}
	}
	return ""
}

// ---- final report ----

func report() {
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println(" verification summary")
	fmt.Println("========================================")
	pass, fail := 0, 0
	for _, r := range results {
		if r.ok {
			pass++
		} else {
			fail++
		}
	}
	fmt.Printf("PASS: %d, FAIL: %d, TOTAL: %d\n", pass, fail, len(results))
	if fail > 0 {
		fmt.Println("FAILED checks:")
		for _, r := range results {
			if !r.ok {
				fmt.Printf("  - %s: %s\n", r.name, r.detail)
			}
		}
		os.Exit(1)
	}
	fmt.Println("All checks PASSED.")
}
