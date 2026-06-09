package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var (
	argWindow     int
	argZScore     float64
	argWorkers    int
	argSkipHead   bool
	argFile       string
	totalLines    int64
	anomalyCount  int64
	malformedCnt  int64
	panicCount    int64
	outputMu      sync.Mutex
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

func (w *SlidingWindow) CheckAndPush(value float64) (mean, stddev, zscore float64, anomaly bool, rejected bool) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		rejected = true
		return
	}

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

type MalformedRow struct {
	LineNo int64
	Raw    string
	Cause  string
}

const (
	ansiRed    = "\033[31m"
	ansiBold   = "\033[1m"
	ansiReset  = "\033[0m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
	ansiDim    = "\033[2m"
	ansiGreen  = "\033[32m"
	ansiMagenta = "\033[35m"
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

func logMalformed(row MalformedRow) {
	total := atomic.AddInt64(&malformedCnt, 1)
	if total <= 10 || total%1000 == 0 {
		truncated := row.Raw
		if len(truncated) > 80 {
			truncated = truncated[:80] + "..."
		}
		msg := fmt.Sprintf("%s%s[畸形行] 行号 %d | 原因: %s | 内容: %s%s\n",
			ansiMagenta, ansiBold, row.LineNo, row.Cause, truncated, ansiReset)
		outputMu.Lock()
		fmt.Fprint(os.Stderr, msg)
		outputMu.Unlock()
	}
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
	if len(line) == 0 {
		logMalformed(MalformedRow{LineNo: lineNo, Raw: line, Cause: "空行"})
		return SensorRecord{}, false
	}

	parts := strings.SplitN(line, ",", 4)
	if len(parts) < 3 {
		logMalformed(MalformedRow{LineNo: lineNo, Raw: line, Cause: fmt.Sprintf("字段不足(期望≥3,实际%d)", len(parts))})
		return SensorRecord{}, false
	}

	timestamp := strings.TrimSpace(parts[0])
	if timestamp == "" {
		logMalformed(MalformedRow{LineNo: lineNo, Raw: line, Cause: "时间戳为空"})
		return SensorRecord{}, false
	}

	spindleID := strings.TrimSpace(parts[1])
	if spindleID == "" {
		logMalformed(MalformedRow{LineNo: lineNo, Raw: line, Cause: "锭子编号为空"})
		return SensorRecord{}, false
	}

	tensionStr := strings.TrimSpace(parts[2])
	if tensionStr == "" {
		logMalformed(MalformedRow{LineNo: lineNo, Raw: line, Cause: "张力值为空"})
		return SensorRecord{}, false
	}

	tension, err := strconv.ParseFloat(tensionStr, 64)
	if err != nil {
		logMalformed(MalformedRow{LineNo: lineNo, Raw: line, Cause: fmt.Sprintf("张力解析失败(%q): %v", tensionStr, err)})
		return SensorRecord{}, false
	}

	if math.IsNaN(tension) || math.IsInf(tension, 0) {
		logMalformed(MalformedRow{LineNo: lineNo, Raw: line, Cause: fmt.Sprintf("张力值非有限数: %v", tension)})
		return SensorRecord{}, false
	}

	if tension < 0 {
		logMalformed(MalformedRow{LineNo: lineNo, Raw: line, Cause: fmt.Sprintf("张力值为负数: %.2f", tension)})
		return SensorRecord{}, false
	}

	return SensorRecord{
		Timestamp: timestamp,
		SpindleID: spindleID,
		Tension:   tension,
		LineNo:    lineNo,
	}, true
}

func streamCSV(path string, ch chan<- SensorRecord) {
	defer func() {
		if r := recover(); r != nil {
			atomic.AddInt64(&panicCount, 1)
			outputMu.Lock()
			fmt.Fprintf(os.Stderr, "%s%s[致命] streamCSV 发生 panic: %v%s\n", ansiRed, ansiBold, r, ansiReset)
			fmt.Fprintf(os.Stderr, "%s堆栈:\n%s%s\n", ansiRed, debug.Stack(), ansiReset)
			outputMu.Unlock()
		}
		close(ch)
	}()

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

func worker(id int, tracker *SpindleTracker, ch <-chan SensorRecord, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			atomic.AddInt64(&panicCount, 1)
			outputMu.Lock()
			fmt.Fprintf(os.Stderr, "%s%s[致命] Worker-%d panic: %v%s\n", ansiRed, ansiBold, id, r, ansiReset)
			fmt.Fprintf(os.Stderr, "%s堆栈:\n%s%s\n", ansiRed, debug.Stack(), ansiReset)
			outputMu.Unlock()
		}
		wg.Done()
	}()

	for rec := range ch {
		func() {
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt64(&panicCount, 1)
					outputMu.Lock()
					fmt.Fprintf(os.Stderr, "%s%s[致命] Worker-%d 处理行 %d 时 panic: %v%s\n",
						ansiRed, ansiBold, id, rec.LineNo, r, ansiReset)
					fmt.Fprintf(os.Stderr, "%s堆栈:\n%s%s\n", ansiRed, debug.Stack(), ansiReset)
					outputMu.Unlock()
				}
			}()

			window := tracker.GetOrCreate(rec.SpindleID)
			mean, stddev, zscore, anomaly, rejected := window.CheckAndPush(rec.Tension)
			if rejected {
				logMalformed(MalformedRow{
					LineNo: rec.LineNo,
					Raw:    fmt.Sprintf("%s,%s,%.4f", rec.Timestamp, rec.SpindleID, rec.Tension),
					Cause:  fmt.Sprintf("滑动窗口拒绝(张力=%.4f: NaN/Inf/负值)", rec.Tension),
				})
				return
			}
			if anomaly {
				printAlert(rec, mean, stddev, zscore)
			}
		}()
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
	fmt.Printf("%s%s║       织布车间 · 经纱张力异常排查终端  v2.0            ║%s\n", ansiCyan, ansiBold, ansiReset)
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
		go worker(i, tracker, ch, &wg)
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
				malformed := atomic.LoadInt64(&malformedCnt)
				panics := atomic.LoadInt64(&panicCount)
				elapsed := time.Since(start).Round(time.Millisecond)
				msg := fmt.Sprintf("%s[进度] 已处理 %d 行 | 预警 %d 条 | 畸形 %d 行 | panic %d 次 | 耗时 %v%s\n",
					ansiDim, lines, anomalies, malformed, panics, elapsed, ansiReset)
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
	malformed := atomic.LoadInt64(&malformedCnt)
	panics := atomic.LoadInt64(&panicCount)
	spindleCount := tracker.Len()

	fmt.Println()
	fmt.Printf("%s%s╔═════════════════════════════════════════════════════════╗%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s%s║                    排 查 报 告                         ║%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s%s╚═════════════════════════════════════════════════════════╝%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("  总处理行数   : %s%d%s\n", ansiBold, lines, ansiReset)
	fmt.Printf("  活跃锭子数   : %s%d%s\n", ansiBold, spindleCount, ansiReset)
	fmt.Printf("  异常预警数   : %s%s%d%s%s\n", ansiRed, ansiBold, anomalies, ansiReset, ansiReset)
	if malformed > 0 {
		fmt.Printf("  畸形行数     : %s%s%d%s%s\n", ansiMagenta, ansiBold, malformed, ansiReset, ansiReset)
	}
	if panics > 0 {
		fmt.Printf("  Panic 捕获   : %s%s%d%s%s (已恢复，未死锁)\n", ansiRed, ansiBold, panics, ansiReset, ansiReset)
	}
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
