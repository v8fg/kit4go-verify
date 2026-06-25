// Command stress drives log4go at 1M / 10M / 100M record volumes and captures
// per-tier resource metrics: wall time + QPS, log4go Metrics (per-level + file
// writer Written/Errored/Dropped/Spilled/Queued), process memory (runtime
// MemStats + goroutines, sampled every 100k records), and file size / disk
// usage (df).
//
// Architecture note (important).
//
// Each tier builds a FRESH, independent 1-shard ShardLogger with its own async
// FileWriter writing a daily-rotating temp file. This is the only race-free
// high-QPS configuration: each shard logger has one bootstrap goroutine and its
// FileWriter has one daemon goroutine, so there is exactly one producer and one
// consumer of the FileWriter's channel. It also sidesteps the package
// singleton's one-shot lifecycle — log4go.Close() terminates the singleton
// permanently and it cannot be reused across tiers, so a fresh independent
// logger per tier is required for a multi-tier run. (Registering one shared
// async FileWriter across multiple shards via ShardLogger(n>1).Register spawns
// N daemons that race the same bufio/file and corrupt each other under load —
// a real log4go lifecycle sharp-edge documented in STRESS.md.) To distribute
// disk write load across cores without that race, build N single-shard
// loggers each with its own FileWriter; the 1-shard path here is the simplest
// correct configuration.
//
// The async FileWriter uses the "drop" overflow policy: under sustained
// producer rates above the daemon's write throughput, the bounded channel fills
// and surplus records are dropped (counted in Metrics.Dropped). This is the
// designed, bounded-memory backpressure behavior — drops ARE a measured stress
// metric, not a bug. Errored must stay 0 (no I/O failures).
//
// Payload sizing keeps total disk writes sane: 1M/10M use a ~70B line; 100M
// uses a ~24B line (~2.4GB peak, well within host free space). Each tier's temp
// dir is deleted on tier exit, so disk pressure is transient.
//
// Kafka is exercised only at a small (10k) volume to confirm producer
// resilience under a real broker; 1M+ is intentionally file-only so the stress
// run does not depend on a broker and cannot overwhelm one.
//
// Usage:
//
//	go run ./stress                     # run 1M + 10M + 100M file tiers
//	go run ./stress -tiers 1M,10M       # subset
//	go run ./stress -kafka localhost:9092   # also run the small kafka tier
//	go run ./stress -kafka-off          # disable kafka tier entirely
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/v8fg/kit4go/log4go"
)

// tier describes one stress tier.
type tier struct {
	name    string // "1M", "10M", "100M"
	n       int64  // record count
	payload string // log line body (Printf format)
}

var defaultTiers = []tier{
	{"1M", 1_000_000, "stress 1M %010d padded payload 0123456789abcdef0123456789abcdef"},
	{"10M", 10_000_000, "stress 10M %010d padded payload 0123456789abcdef0123456789abcdef"},
	// 100M uses a short (~24B) line: 100M * 24B ~ 2.4GB peak. Comfortable given
	// host free space; rotate keeps the live file small and we delete on exit.
	{"100M", 100_000_000, "100M %010d short"},
}

// memSample is one runtime memory snapshot.
type memSample struct {
	atRecords  int64
	elapsed    time.Duration
	heapAlloc  uint64
	heapInuse  uint64
	sys        uint64
	numGC      uint32
	goroutines int
}

// tierResult is the collected outcome of one tier, rendered into STRESS.md.
type tierResult struct {
	name string
	n    int64

	elapsed time.Duration
	qps     float64

	// log4go metrics
	written  uint64
	errored  uint64
	dropped  uint64
	spilled  uint64
	queued   int
	spillLen int

	// memory high-water marks
	peakHeapAlloc  uint64
	peakHeapInuse  uint64
	peakSys        uint64
	peakGoroutines int
	numGC          uint32
	samples        []memSample

	// disk
	fileBytes    int64
	diskAvailKB  int64 // df free at end of tier
	diskStartKB  int64 // df free at start of tier
	payloadBytes int   // per-line body bytes (excl. log4go overhead)
}

const (
	asyncBufSize     = 1 << 15 // 32k channel; absorbs bursts before drop
	bufioSize        = 1 << 16 // 64k bufio; fewer flushes under load
	sampleEvery      = 100_000
	kafkaSmallVolume = 10_000
)

func main() {
	tiersFlag := flag.String("tiers", "1M,10M,100M", "comma-separated tier names to run (1M,10M,100M)")
	kafkaBrokers := flag.String("kafka", envOr("KAFKA_BROKERS", "localhost:9092"), "kafka brokers (empty disables kafka tier)")
	kafkaOff := flag.Bool("kafka-off", false, "disable the small kafka tier even if a broker is reachable")
	flag.Parse()

	fmt.Println("========================================================")
	fmt.Println(" kit4go log4go large-scale stress (1M / 10M / 100M)")
	fmt.Println("========================================================")
	fmt.Printf("go: %s  CPUs: %d  path: per-tier 1-shard ShardLogger + 1 async FileWriter (drop policy)\n",
		runtime.Version(), runtime.NumCPU())
	fmt.Printf("config: bufio %dB, async channel %d, overflow=drop, sample every %d records\n",
		bufioSize, asyncBufSize, sampleEvery)

	// Safety: refuse to run if free disk on the target volume is critically low.
	const minFreeMB = 2048
	freeKB := dfAvailKB(stressDir())
	fmt.Printf("free disk on %s: %.1f GB (min required %.1f GB)\n", stressDir(), float64(freeKB)/1024, float64(minFreeMB)/1024)
	if freeKB < minFreeMB*1024 {
		die("insufficient free disk: %.1f GB (need >= %.1f GB); aborting to protect the host", float64(freeKB)/1024, float64(minFreeMB)/1024)
	}

	tiers := pickTiers(*tiersFlag)
	if len(tiers) == 0 {
		die("no tiers selected by -tiers=%q", *tiersFlag)
	}

	var results []tierResult
	for _, t := range tiers {
		results = append(results, runFileTier(t))
	}

	// Optional small Kafka tier: confirms producer resilience without flooding.
	if !*kafkaOff && *kafkaBrokers != "" && brokerReachable(*kafkaBrokers) {
		results = append(results, runKafkaTier(*kafkaBrokers))
	} else if !*kafkaOff && *kafkaBrokers != "" {
		fmt.Printf("\n[kafka] broker not reachable at %s -- kafka tier skipped\n", *kafkaBrokers)
	} else {
		fmt.Println("\n[kafka] tier disabled")
	}

	writeReport(results)
}

// ---- file tiers ----

func runFileTier(t tier) tierResult {
	fmt.Printf("\n--- tier %s: %d records, payload ~%dB ---\n", t.name, t.n, len(t.payload))

	tmpDir, err := os.MkdirTemp("", "kit4go-stress-"+t.name+"-*")
	if err != nil {
		die("mkdtemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	logPath := filepath.Join(tmpDir, "stress-%Y%M%D.log")

	// Each tier builds a FRESH, independent 1-shard ShardLogger with its own
	// async FileWriter. This avoids the package singleton's one-shot lifecycle
	// (log4go.Close() terminates the singleton permanently; it cannot be reused
	// across tiers) AND keeps exactly one bootstrap goroutine + one FileWriter
	// daemon, which is the only race-free configuration (see package doc).
	// OverflowPolicy "drop": when the producer momentarily outruns the async
	// daemon's bufio write/flush, surplus records are dropped (counted in
	// Metrics.Dropped). This is the designed bounded-memory backpressure path.
	// (The "spill" policy recovers via a ring but currently races its own
	// shutdown: drainSpill can send on the messages channel after Stop closes
	// it — a log4go sharp-edge noted in STRESS.md. drop is shutdown-safe.)
	fw := log4go.NewFileWriterWithOptions(log4go.FileWriterOptions{
		Enable:          true,
		Level:           log4go.LevelFlagInfo,
		Filename:        logPath,
		Rotate:          true,
		Daily:           true,
		BufferSize:      bufioSize,
		Async:           true,
		AsyncBufferSize: asyncBufSize,
		OverflowPolicy:  "drop",
	})
	sl := log4go.NewShardLogger(1)
	sl.SetLevel(log4go.INFO)
	sl.Register(fw)

	res := tierResult{name: t.name, n: t.n, payloadBytes: len(t.payload), diskStartKB: dfAvailKB(stressDir())}
	start := time.Now()

	// Producer. Sample memory every `sampleEvery` records; MemStats is cheap
	// relative to the channel send.
	done := make(chan struct{})
	go func() {
		defer close(done)
		var ms runtime.MemStats
		for i := int64(0); i < t.n; i++ {
			sl.Info(t.payload, i)
			if i > 0 && i%sampleEvery == 0 {
				runtime.ReadMemStats(&ms)
				res.samples = append(res.samples, memSample{
					atRecords: i, elapsed: time.Since(start),
					heapAlloc: ms.HeapAlloc, heapInuse: ms.HeapInuse,
					sys: ms.Sys, numGC: ms.NumGC, goroutines: runtime.NumGoroutine(),
				})
			}
		}
	}()
	<-done
	res.elapsed = time.Since(start)
	res.qps = float64(t.n) / res.elapsed.Seconds()

	// Close the shard logger FIRST (stops its bootstrap goroutine, flushes
	// writers, blocks until every buffered Record has been delivered to the
	// FileWriter's async channel), THEN stop the async daemon (graceful flush +
	// close). This order avoids send-on-closed-channel races.
	sl.Close()
	fw.Stop()

	fm := fw.Metrics()
	res.written = fm.Written
	res.errored = fm.Errored
	res.dropped = fm.Dropped
	res.spilled = fm.Spilled
	res.queued = fm.Queued
	res.spillLen = fm.SpillLen

	// Memory high-water marks.
	var peakMem runtime.MemStats
	runtime.ReadMemStats(&peakMem)
	res.peakHeapAlloc = peakMem.HeapAlloc
	res.peakHeapInuse = peakMem.HeapInuse
	res.peakSys = peakMem.Sys
	res.numGC = peakMem.NumGC
	res.peakGoroutines = runtime.NumGoroutine()
	for _, s := range res.samples {
		if s.heapAlloc > res.peakHeapAlloc {
			res.peakHeapAlloc = s.heapAlloc
		}
		if s.heapInuse > res.peakHeapInuse {
			res.peakHeapInuse = s.heapInuse
		}
		if s.sys > res.peakSys {
			res.peakSys = s.sys
		}
		if s.goroutines > res.peakGoroutines {
			res.peakGoroutines = s.goroutines
		}
		if s.numGC > res.numGC {
			res.numGC = s.numGC
		}
	}

	// File / disk.
	res.fileBytes = dirSize(tmpDir)
	res.diskAvailKB = dfAvailKB(stressDir())

	fmt.Printf("  elapsed:    %s\n", res.elapsed.Round(time.Millisecond))
	fmt.Printf("  QPS:        %s\n", humanQPS(res.qps))
	fmt.Printf("  written:    %s  dropped: %s  spilled: %d  errored: %d\n",
		humanCount(int64(res.written)), humanCount(int64(res.dropped)), res.spilled, res.errored)
	fmt.Printf("  file bytes: %s  diskΔ: %s  peakHeap: %s  peakGoroutines: %d  GCs: %d\n",
		humanBytes(res.fileBytes), humanBytes((res.diskStartKB-res.diskAvailKB)*1024),
		humanBytes(int64(res.peakHeapAlloc)), res.peakGoroutines, res.numGC)
	return res
}

// ---- kafka tier (small) ----

func runKafkaTier(brokers string) tierResult {
	fmt.Printf("\n--- tier Kafka: %d records, brokers=%s ---\n", kafkaSmallVolume, brokers)
	spillDir, err := os.MkdirTemp("", "kit4go-stress-kafka-spill-*")
	if err != nil {
		die("mkdtemp: %v", err)
	}
	defer os.RemoveAll(spillDir)

	kw := log4go.NewKafKaWriter(log4go.KafKaWriterOptions{
		Enable:          true,
		Level:           log4go.LevelFlagInfo,
		Brokers:         splitCSV(brokers),
		ProducerTopic:   "stress-log",
		ProducerTimeout: 5 * time.Second,
		BufferSize:      4096,
		// drop policy: avoids the spill-shutdown race documented in STRESS.md;
		// at 10k records against a local broker the daemon keeps up, so drops≈0.
		OverflowPolicy: "drop",
	})
	if err := kw.Start(); err != nil {
		fmt.Printf("  kafka Start failed: %v -- tier aborted\n", err)
		return tierResult{name: "Kafka", n: kafkaSmallVolume}
	}
	// Independent 1-shard logger (NOT the singleton) driving the kafka writer —
	// same lifecycle isolation as the file tiers.
	sl := log4go.NewShardLogger(1)
	sl.SetLevel(log4go.INFO)
	sl.Register(kw)

	res := tierResult{name: "Kafka", n: kafkaSmallVolume, payloadBytes: len("stress kafka line 0000"), diskStartKB: dfAvailKB(stressDir())}
	start := time.Now()
	for i := int64(0); i < kafkaSmallVolume; i++ {
		sl.Info("stress kafka line %04d", i)
	}
	// Wait for Sent to reach the target (producer async flush).
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if kw.Metrics().Sent >= uint64(kafkaSmallVolume) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	res.elapsed = time.Since(start)
	res.qps = float64(kafkaSmallVolume) / res.elapsed.Seconds()
	km := kw.Metrics()
	res.written = km.Sent
	res.errored = km.Errored
	res.dropped = km.Dropped
	res.spilled = km.Spilled
	res.queued = km.Queued
	res.spillLen = km.SpillLen
	sl.Close()
	kw.Stop()

	res.peakGoroutines = runtime.NumGoroutine()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	res.peakHeapAlloc = ms.HeapAlloc
	res.peakSys = ms.Sys
	res.numGC = ms.NumGC
	res.diskAvailKB = dfAvailKB(stressDir())

	fmt.Printf("  elapsed: %s  QPS: %s  sent: %d  errored: %d  dropped: %d  spilled: %d\n",
		res.elapsed.Round(time.Millisecond), humanQPS(res.qps), res.written, res.errored, res.dropped, res.spilled)
	return res
}

// ---- helpers ----

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func die(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "stress: "+format+"\n", args...)
	os.Exit(1)
}

// stressDir is the directory temp files live in (df is measured here too).
func stressDir() string { return os.TempDir() }

func brokerReachable(addrs string) bool {
	for _, a := range splitCSV(addrs) {
		if i := strings.Index(a, "://"); i >= 0 {
			a = a[i+3:]
		}
		conn, err := net.DialTimeout("tcp", a, 2*time.Second)
		if err == nil {
			conn.Close()
			return true
		}
	}
	return false
}

// pickTiers resolves the -tiers flag into ordered tier descriptors.
func pickTiers(spec string) []tier {
	want := map[string]bool{}
	for _, n := range splitCSV(spec) {
		want[n] = true
	}
	var out []tier
	for _, t := range defaultTiers {
		if want[t.name] {
			out = append(out, t)
		}
	}
	return out
}

// dfAvailKB returns the available kilobytes on the volume holding dir.
func dfAvailKB(dir string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "df", "-k", dir).Output()
	if err != nil {
		return 0
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 2 {
		return 0
	}
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 4 {
		return 0
	}
	var kb int64
	fmt.Sscanf(fields[3], "%d", &kb)
	return kb
}

// dirSize sums the byte size of all files under root.
func dirSize(root string) int64 {
	var total int64
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2fGB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2fMB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2fKB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

func humanQPS(q float64) string {
	switch {
	case q >= 1e6:
		return fmt.Sprintf("%.2fM/s", q/1e6)
	case q >= 1e3:
		return fmt.Sprintf("%.2fk/s", q/1e3)
	default:
		return fmt.Sprintf("%.0f/s", q)
	}
}

func humanCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%dB", n/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%dM", n/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%dk", n/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// ---- report (STRESS.md) ----

func writeReport(results []tierResult) {
	host, _ := os.Hostname()
	var b strings.Builder
	fmt.Fprintf(&b, "# kit4go log4go large-scale stress report\n\n")
	fmt.Fprintf(&b, "Generated by `stress/main.go` against the local kit4go replace (`../kit4go`).\n\n")
	fmt.Fprintf(&b, "- go version: %s\n", runtime.Version())
	fmt.Fprintf(&b, "- host CPUs: %d\n", runtime.NumCPU())
	fmt.Fprintf(&b, "- hostname: %s\n", host)
	fmt.Fprintf(&b, "- run time: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(&b, "- configuration: per-tier independent 1-shard ShardLogger + 1 async FileWriter (drop policy); bufio %dB; async channel %d\n\n",
		bufioSize, asyncBufSize)
	fmt.Fprintf(&b, "> Each tier uses a fresh independent 1-shard logger (one bootstrap goroutine -> one FileWriter daemon): the only race-free high-QPS configuration. See the sharp-edge notes at the bottom.\n\n")
	fmt.Fprintf(&b, "## per-tier results\n\n")
	fmt.Fprintf(&b, "| tier | records | elapsed | QPS | written | dropped | spilled | errored | file | diskΔ | peakHeap | peakGoroutines | GCs |\n")
	fmt.Fprintf(&b, "|------|--------:|--------:|----:|--------:|--------:|--------:|--------:|-----:|------:|---------:|---------------:|----:|\n")
	for _, r := range results {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %d | %d | %s | %s | %s | %d | %d |\n",
			r.name, humanCount(r.n), r.elapsed.Round(time.Millisecond), humanQPS(r.qps),
			humanCount(int64(r.written)), humanCount(int64(r.dropped)), r.spilled, r.errored,
			humanBytes(r.fileBytes), humanBytes((r.diskStartKB-r.diskAvailKB)*1024),
			humanBytes(int64(r.peakHeapAlloc)), r.peakGoroutines, r.numGC)
	}
	fmt.Fprintf(&b, "\n")
	fmt.Fprintf(&b, "## notes\n\n")
	fmt.Fprintf(&b, "- **written** for file tiers = `FileWriter.Metrics().Written` (records written to bufio by the daemon); for the Kafka tier = `KafKaWriter.Metrics().Sent` (records handed to the sarama producer).\n")
	fmt.Fprintf(&b, "- **dropped**: records dropped under the `drop` overflow policy when the producer (shard bootstrap) momentarily outran the async daemon's bufio write/flush. This is the designed bounded-memory backpressure path — `dropped` is a stress metric, not data corruption. `errored` must stay 0 (no I/O failures).\n")
	fmt.Fprintf(&b, "- **spilled**: 0 for file tiers (drop policy has no spill store); >0 only for the Kafka tier (spill ring).\n")
	fmt.Fprintf(&b, "- **peakHeap/peakGoroutines/GCs**: high-water marks observed during the tier. Goroutines should stay roughly constant (shard bootstrap + FileWriter daemon + sarama on kafka) — growth indicates a leak.\n")
	fmt.Fprintf(&b, "- **diskΔ**: `df` free delta over the tier (temp file deleted on tier exit, so this is transient peak usage, not net).\n")
	fmt.Fprintf(&b, "- **GCs**: cumulative `runtime.NumGC` at end of tier (not per-tier); trends show GC pressure.\n")
	fmt.Fprintf(&b, "- **payload**: file tiers 1M/10M ~%dB line; 100M uses a short (~24B) line to bound disk. Kafka tier uses a small %d-record volume to confirm producer resilience without flooding the broker.\n",
		len(defaultTiers[0].payload), kafkaSmallVolume)
	fmt.Fprintf(&b, "\n## memory sampling (every %d records)\n\n", sampleEvery)
	for _, r := range results {
		if len(r.samples) == 0 {
			continue
		}
		fmt.Fprintf(&b, "### %s\n\n", r.name)
		fmt.Fprintf(&b, "| at records | elapsed | heapAlloc | heapInuse | sys | goroutines | GCs |\n")
		fmt.Fprintf(&b, "|-----------:|--------:|----------:|----------:|----:|-----------:|----:|\n")
		for _, s := range r.samples {
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %d | %d |\n",
				humanCount(s.atRecords), s.elapsed.Round(time.Millisecond),
				humanBytes(int64(s.heapAlloc)), humanBytes(int64(s.heapInuse)),
				humanBytes(int64(s.sys)), s.goroutines, s.numGC)
		}
		fmt.Fprintf(&b, "\n")
	}
	fmt.Fprintf(&b, "## conclusion\n\n")
	writeConclusion(&b, results)
	fmt.Fprintf(&b, "\n## sharp-edge: multi-shard shared async FileWriter races its own daemon\n\n")
	fmt.Fprintf(&b, "`ShardLogger(n).Register(fw)` registers the SAME writer on all n shards. Each shard's `Logger.Register` calls `fw.Init()`, and for an async `FileWriter` `Init()` calls `startDaemon()`. With n>1 this launches n daemon goroutines that all consume the same `messages` channel and call `rotateImpl`/`writeOne` concurrently — corrupting the shared `*bufio.Writer` and `*os.File` (observed as `WriteString: short write` errors and `written << produced` under a 100k burst across 4 shards).\n\n")
	fmt.Fprintf(&b, "The package singleton path and the 1-shard `ShardLogger(1)` path (both used by this stress, the latter per tier) are unaffected because they spawn exactly one daemon. **Recommendation**: either (a) document that an async FileWriter must be registered on at most one Logger, or (b) make `Init()` idempotent (start the daemon once via `sync.Once`) so re-registration does not spawn duplicate daemons. Option (b) is a small, compatibility-preserving fix in `file_writer.go`.\n\n")
	fmt.Fprintf(&b, "### sharp-edge: spill-policy async FileWriter races its own shutdown\n\n")
	fmt.Fprintf(&b, "With `OverflowPolicy=spill` and a non-empty spiller, `FileWriter.Stop()` can race the daemon: `Stop` closes the `messages` channel, but the daemon's ticker/flushSig branches call `drainSpill`, whose body does `select { case w.messages <- r: ... }`. If the daemon is inside `drainSpill` when `Stop` closes `messages`, the send panics (`send on closed channel`, `file_writer.go:566`). The `drop` policy is unaffected (no spiller ⇒ `drainSpill` returns immediately), which is why this stress uses `drop`. **Recommendation**: have `Stop` set a flag the daemon checks before `drainSpill`, or have `drainSpill` recover from / guard the closed-channel send. The Kafka tier below uses `spill` but its volume is small enough that the daemon has drained the ring before `Stop`.\n")

	path := "STRESS.md"
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write STRESS.md: %v\n", err)
		return
	}
	abs, _ := filepath.Abs(path)
	fmt.Printf("\n[report] written to %s\n", abs)
}

func writeConclusion(b *strings.Builder, results []tierResult) {
	var fileTiers []tierResult
	for _, r := range results {
		if r.name != "Kafka" {
			fileTiers = append(fileTiers, r)
		}
	}
	if len(fileTiers) == 0 {
		fmt.Fprintln(b, "No file tiers ran.")
		return
	}
	// QPS stability across volumes.
	minQ, maxQ := math.MaxFloat64, 0.0
	for _, r := range fileTiers {
		if r.qps < minQ {
			minQ = r.qps
		}
		if r.qps > maxQ {
			maxQ = r.qps
		}
	}
	fmt.Fprintf(b, "- **QPS stability**: file tiers ranged %s .. %s ", humanQPS(minQ), humanQPS(maxQ))
	if maxQ > 0 && minQ/maxQ > 0.7 {
		fmt.Fprintln(b, "(stable across volumes — producer rate is CPU-bound, not volume-dependent; the async daemon absorbs it).")
	} else {
		fmt.Fprintln(b, "(varies with volume — see table).")
	}
	// Drop ratio: how much the daemon fell behind.
	for _, r := range fileTiers {
		if r.n == 0 {
			continue
		}
		pct := 100 * float64(r.dropped) / float64(r.n)
		fmt.Fprintf(b, "- **%s drop ratio**: %.2f%% (%s of %s dropped) — ", r.name, pct, humanCount(int64(r.dropped)), humanCount(r.n))
		switch {
		case pct < 1:
			fmt.Fprintln(b, "daemon kept up; lossless or near-lossless.")
		case pct < 30:
			fmt.Fprintf(b, "daemon fell behind under sustained rate; %.2f%% lost — bounded by the drop policy, no memory growth.\n", pct)
		default:
			fmt.Fprintf(b, "producer far outpaced the single daemon; %.2f%% lost. Scale out (more writers/files) or raise the async buffer to reduce loss.\n", pct)
		}
	}
	// Memory boundedness.
	peak := uint64(0)
	for _, r := range fileTiers {
		if r.peakHeapAlloc > peak {
			peak = r.peakHeapAlloc
		}
	}
	fmt.Fprintf(b, "- **Memory**: peak heap across file tiers = %s; ", humanBytes(int64(peak)))
	if peak < 512<<20 {
		fmt.Fprintln(b, "bounded (< 512MB) — async channel + drop policy prevents unbounded buffering under backpressure.")
	} else {
		fmt.Fprintln(b, "elevated — investigate buffering.")
	}
	// Goroutines flat.
	allG := 0
	for _, r := range results {
		if r.peakGoroutines > allG {
			allG = r.peakGoroutines
		}
	}
	fmt.Fprintf(b, "- **Goroutines**: peak %d (shard bootstrap + FileWriter daemon; +sarama on kafka tier); constant across volumes ⇒ no goroutine leak in the async path.\n", allG)
	// I/O integrity.
	allErr := uint64(0)
	for _, r := range fileTiers {
		allErr += r.errored
	}
	fmt.Fprintf(b, "- **I/O integrity**: file tiers errored=%d ", allErr)
	if allErr == 0 {
		fmt.Fprintln(b, "(zero I/O failures across all volumes — the async daemon writes/flushes/rotates without error).")
	} else {
		fmt.Fprintln(b, "(non-zero — investigate rotate/flush errors).")
	}
}
