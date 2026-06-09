package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var (
	argWindow    int
	argZScore    float64
	argWorkers   int
	argSkipHead  bool
	argFile      string
	totalLines   int64
	anomalyCount int64
	outputMu     sync.Mutex
)

func init() {
	flag.IntVar(&argWindow, "window", 100, "滑动窗口长度（数据点数）")
	flag.Float64Var(&argZScore, "z", 3.0, "Z-Score 阈值（标准差倍数）")
	flag.IntVar(&argWorkers, "workers", 0, "消费者 Goroutine 数量（0=CPU核心数）")
	flag.BoolVar(&argSkipHead, "skip-header", true, "跳过 CSV 首行表头")
	flag.StringVar(&argFile, "file", "", "CSV 传感器日志文件路径")
}

const (
	scannerBufSize = 256 * 1024
	chanBufSize    = 65536
)

type SlidingWindow struct {
	mu    sync.Mutex
	buf   []float64
	head  int
	count int
	sum   float64
	sumSq float64
}

func NewSlidingWindow(size int) *SlidingWindow {
	return &SlidingWindow{
		buf: make([]float64, size),
	}
}

func (w *SlidingWindow) CheckAndPush(value float64) (mean, stddev, zscore float64, anomaly bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.count >= 2 {
		n := float64(w.count)
		mean = w.sum / n
		variance := w.sumSq/n - mean*mean
		if variance < 0 {
			variance = 0
		}
		stddev = math.Sqrt(variance)
		if stddev > 0 {
			zscore = math.Abs(value-mean) / stddev
			anomaly = zscore > argZScore
		}
	}

	if w.count == len(w.buf) {
		outgoing := w.buf[w.head]
		w.sum -= outgoing
		w.sumSq -= outgoing * outgoing
	} else {
		w.count++
	}

	w.buf[w.head] = value
	w.sum += value
	w.sumSq += value * value
	w.head = (w.head + 1) % len(w.buf)

	return
}

type SpindleTracker struct {
	mu      sync.Mutex
	windows map[string]*SlidingWindow
}

func NewSpindleTracker() *SpindleTracker {
	return &SpindleTracker{
		windows: make(map[string]*SlidingWindow),
	}
}

func (t *SpindleTracker) GetOrCreate(spindleID string) *SlidingWindow {
	t.mu.Lock()
	defer t.mu.Unlock()
	if w, exists := t.windows[spindleID]; exists {
		return w
	}
	w := NewSlidingWindow(argWindow)
	t.windows[spindleID] = w
	return w
}

func (t *SpindleTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.windows)
}

type SensorRecord struct {
	Timestamp string
	SpindleID string
	Tension   float64
	LineNo    int64
}

const (
	ansiRed    = "\033[31m"
	ansiBold   = "\033[1m"
	ansiReset  = "\033[0m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
	ansiDim    = "\033[2m"
	ansiGreen  = "\033[32m"
)

func enableWindowsVT() {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	getMode := kernel32.NewProc("GetConsoleMode")
	setMode := kernel32.NewProc("SetConsoleMode")
	var mode uint32
	getMode.Call(uintptr(syscall.Stdout), uintptr(unsafe.Pointer(&mode)))
	setMode.Call(uintptr(syscall.Stdout), uintptr(mode|0x0004))
	setMode.Call(uintptr(syscall.Stderr), uintptr(mode|0x0004))
}

func printAlert(rec SensorRecord, mean, stddev, zscore float64) {
	atomic.AddInt64(&anomalyCount, 1)
	msg := fmt.Sprintf("%s%s⚠  经纱张力突发性滑脱  ⚠%s\n"+
		"%s  ┌──────────────────────────────────────────────┐%s\n"+
		"%s  │ 时间戳      │ %s%s\n"+
		"%s  │ 锭子编号    │ %s%s\n"+
		"%s  │ 实时张力    │ %.2f cN%s\n"+
		"%s  │ 窗口均值    │ %.2f cN%s\n"+
		"%s  │ 窗口标准差  │ %.2f cN%s\n"+
		"%s  │ Z-Score     │ %.2f  (> %.1f σ)%s\n"+
		"%s  │ 日志行号    │ %d%s\n"+
		"%s  └──────────────────────────────────────────────┘%s\n\n",
		ansiRed, ansiBold, ansiReset,
		ansiRed, ansiReset,
		ansiRed, rec.Timestamp, ansiReset,
		ansiRed, rec.SpindleID, ansiReset,
		ansiRed, rec.Tension, ansiReset,
		ansiRed, mean, ansiReset,
		ansiRed, stddev, ansiReset,
		ansiRed, zscore, argZScore, ansiReset,
		ansiRed, rec.LineNo, ansiReset,
		ansiRed, ansiReset,
	)
	outputMu.Lock()
	fmt.Print(msg)
	outputMu.Unlock()
}

func parseLine(line string, lineNo int64) (SensorRecord, bool) {
	parts := strings.SplitN(line, ",", 4)
	if len(parts) < 3 {
		return SensorRecord{}, false
	}
	tension, err := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
	if err != nil {
		return SensorRecord{}, false
	}
	return SensorRecord{
		Timestamp: strings.TrimSpace(parts[0]),
		SpindleID: strings.TrimSpace(parts[1]),
		Tension:   tension,
		LineNo:    lineNo,
	}, true
}

func streamCSV(path string, ch chan<- SensorRecord) {
	defer close(ch)

	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s错误: 无法打开文件 %s: %v%s\n", ansiRed, path, err, ansiReset)
		os.Exit(1)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, scannerBufSize), scannerBufSize)

	var lineNo int64

	if argSkipHead && scanner.Scan() {
		lineNo++
	}

	for scanner.Scan() {
		lineNo++
		atomic.StoreInt64(&totalLines, lineNo)

		line := scanner.Text()
		if line == "" {
			continue
		}

		rec, ok := parseLine(line, lineNo)
		if !ok {
			continue
		}

		ch <- rec
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "%s错误: 读取文件中断 - %v%s\n", ansiRed, err, ansiReset)
	}
}

func worker(tracker *SpindleTracker, ch <-chan SensorRecord, wg *sync.WaitGroup) {
	defer wg.Done()
	for rec := range ch {
		window := tracker.GetOrCreate(rec.SpindleID)
		mean, stddev, zscore, anomaly := window.CheckAndPush(rec.Tension)
		if anomaly {
			printAlert(rec, mean, stddev, zscore)
		}
	}
}

func main() {
	flag.Parse()

	if argFile == "" {
		fmt.Fprintf(os.Stderr, "%s错误: 请使用 -file 指定 CSV 传感器日志文件路径%s\n", ansiRed, ansiReset)
		flag.Usage()
		os.Exit(1)
	}

	enableWindowsVT()

	if argWorkers <= 0 {
		argWorkers = runtime.NumCPU()
	}

	fmt.Printf("%s%s╔═════════════════════════════════════════════════════════╗%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s%s║       织布车间 · 经纱张力异常排查终端  v1.0            ║%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s%s╚═════════════════════════════════════════════════════════╝%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s  数据源       : %s%s\n", ansiYellow, argFile, ansiReset)
	fmt.Printf("%s  滑动窗口     : %d 个数据点%s\n", ansiYellow, argWindow, ansiReset)
	fmt.Printf("%s  Z-Score 阈值 : %.1f σ%s\n", ansiYellow, argZScore, ansiReset)
	fmt.Printf("%s  消费者协程   : %d 个 Goroutine%s\n", ansiYellow, argWorkers, ansiReset)
	fmt.Printf("%s  跳过表头     : %v%s\n", ansiYellow, argSkipHead, ansiReset)
	fmt.Println()
	fmt.Printf("%s▶ 开始流式扫描...%s\n\n", ansiGreen+ansiBold, ansiReset)

	tracker := NewSpindleTracker()
	ch := make(chan SensorRecord, chanBufSize)

	var wg sync.WaitGroup
	for i := 0; i < argWorkers; i++ {
		wg.Add(1)
		go worker(tracker, ch, &wg)
	}

	start := time.Now()
	go streamCSV(argFile, ch)

	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				lines := atomic.LoadInt64(&totalLines)
				anomalies := atomic.LoadInt64(&anomalyCount)
				elapsed := time.Since(start).Round(time.Millisecond)
				msg := fmt.Sprintf("%s[进度] 已处理 %d 行 | 预警 %d 条 | 耗时 %v%s\n",
					ansiDim, lines, anomalies, elapsed, ansiReset)
				outputMu.Lock()
				fmt.Print(msg)
				outputMu.Unlock()
			case <-progressDone:
				return
			}
		}
	}()

	wg.Wait()
	close(progressDone)

	elapsed := time.Since(start).Round(time.Millisecond)
	lines := atomic.LoadInt64(&totalLines)
	anomalies := atomic.LoadInt64(&anomalyCount)
	spindleCount := tracker.Len()

	fmt.Println()
	fmt.Printf("%s%s╔═════════════════════════════════════════════════════════╗%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s%s║                    排 查 报 告                         ║%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s%s╚═════════════════════════════════════════════════════════╝%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("  总处理行数   : %s%d%s\n", ansiBold, lines, ansiReset)
	fmt.Printf("  活跃锭子数   : %s%d%s\n", ansiBold, spindleCount, ansiReset)
	fmt.Printf("  异常预警数   : %s%s%d%s%s\n", ansiRed, ansiBold, anomalies, ansiReset, ansiReset)
	fmt.Printf("  总耗时       : %v\n", elapsed)
	if lines > 0 && elapsed.Seconds() > 0 {
		rate := float64(lines) / elapsed.Seconds()
		fmt.Printf("  处理速率     : %s%.0f 行/秒%s\n", ansiGreen, rate, ansiReset)
	}
	fmt.Println()

	if anomalies > 0 {
		fmt.Printf("%s%s⚠  检测到 %d 条经纱张力突发性滑脱预警，请立即排查相关锭子！%s\n",
			ansiRed, ansiBold, anomalies, ansiReset)
	} else {
		fmt.Printf("%s✓ 未检测到经纱张力突发性滑脱异常%s\n", ansiGreen, ansiReset)
	}
	fmt.Println()
}
