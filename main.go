package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"math/cmplx"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var (
	argWindow       int
	argZScore       float64
	argWorkers      int
	argSkipHead     bool
	argFile         string
	argSampleRate   float64
	argMotorFreq    float64
	argFreqDrift    float64
	argFFTInterval  int
	argSpecSNR      float64
	argMinPeak      float64
	totalLines      int64
	anomalyCount    int64
	malformedCnt    int64
	panicCount      int64
	bearingAlertCnt  int64
	bearingRepeatCnt int64
	outputMu         sync.Mutex
	bearingSet       sync.Map
)

func init() {
	flag.IntVar(&argWindow, "window", 256, "滑动窗口长度（数据点数，建议2的幂）")
	flag.Float64Var(&argZScore, "z", 3.0, "Z-Score 阈值（标准差倍数）")
	flag.IntVar(&argWorkers, "workers", 0, "消费者 Goroutine 数量（0=CPU核心数）")
	flag.BoolVar(&argSkipHead, "skip-header", true, "跳过 CSV 首行表头")
	flag.StringVar(&argFile, "file", "", "CSV 传感器日志文件路径")
	flag.Float64Var(&argSampleRate, "sample-rate", 10.0, "传感器采样率（Hz），用于频率轴换算")
	flag.Float64Var(&argMotorFreq, "motor-freq", 2.0, "电机主轴标准转动频率（Hz）")
	flag.Float64Var(&argFreqDrift, "freq-drift", 0.5, "主导频率允许漂移量（Hz），超过即判定轴承磨损")
	flag.IntVar(&argFFTInterval, "fft-interval", 5, "每推进 N 个数据点执行一次 FFT 分析")
	flag.Float64Var(&argSpecSNR, "spec-snr", 3.0, "频谱信噪比阈值（主峰幅值/噪声底均值），低于此值视为噪声不触发轴承报告")
	flag.Float64Var(&argMinPeak, "min-peak", 0.15, "主导频率最小峰值幅值（cN），低于此值视为噪声不触发轴承报告")
}

const (
	scannerBufSize = 256 * 1024
	chanBufSize    = 65536
)

type SlidingWindow struct {
	mu           sync.Mutex
	buf          []float64
	head         int
	count        int
	maxSize      int
	sum          float64
	sumSq        float64
	pushSinceFFT int
}

func NewSlidingWindow(size int) *SlidingWindow {
	return &SlidingWindow{
		buf:     make([]float64, size),
		maxSize: size,
	}
}

func (w *SlidingWindow) IsFull() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count == w.maxSize
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

	if w.count == w.maxSize {
		outgoing := w.buf[w.head]
		w.sum -= outgoing
		w.sumSq -= outgoing * outgoing
	} else {
		w.count++
	}

	w.buf[w.head] = value
	w.head = (w.head + 1) % w.maxSize
	w.sum += value
	w.sumSq += value * value
	w.pushSinceFFT++

	return
}

func (w *SlidingWindow) Snapshot() []float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]float64, w.count)
	tail := (w.head - w.count + w.maxSize) % w.maxSize
	for i := 0; i < w.count; i++ {
		out[i] = w.buf[(tail+i)%w.maxSize]
	}
	return out
}

func (w *SlidingWindow) ShouldRunFFT() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.count < w.maxSize {
		return false
	}
	return w.pushSinceFFT >= argFFTInterval
}

func (w *SlidingWindow) ResetFFTClock() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pushSinceFFT = 0
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

func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func fft(x []complex128) []complex128 {
	n := len(x)
	if n == 1 {
		return x
	}
	if n%2 != 0 {
		return dft(x)
	}
	even := make([]complex128, n/2)
	odd := make([]complex128, n/2)
	for i := 0; i < n/2; i++ {
		even[i] = x[2*i]
		odd[i] = x[2*i+1]
	}
	E := fft(even)
	O := fft(odd)
	out := make([]complex128, n)
	for k := 0; k < n/2; k++ {
		angle := -2.0 * math.Pi * float64(k) / float64(n)
		w := cmplx.Exp(complex(0, angle))
		out[k] = E[k] + w*O[k]
		out[k+n/2] = E[k] - w*O[k]
	}
	return out
}

func dft(x []complex128) []complex128 {
	n := len(x)
	out := make([]complex128, n)
	for k := 0; k < n; k++ {
		var sum complex128
		for j := 0; j < n; j++ {
			angle := -2.0 * math.Pi * float64(k) * float64(j) / float64(n)
			sum += x[j] * cmplx.Exp(complex(0, angle))
		}
		out[k] = sum
	}
	return out
}

type FreqPeak struct {
	Freq      float64
	Magnitude float64
}

type FreqAnalysisResult struct {
	DominantFreq    float64
	Peaks           []FreqPeak
	SpectralCentroid float64
	SpectralSNR     float64
	DriftHz         float64
	IsDrifted       bool
}

func analyzeSpectrum(samples []float64, sampleRate float64) FreqAnalysisResult {
	n := len(samples)
	mean := 0.0
	for _, v := range samples {
		mean += v
	}
	mean /= float64(n)

	detrended := make([]float64, n)
	for i, v := range samples {
		detrended[i] = v - mean
	}

	hann := make([]float64, n)
	for i := 0; i < n; i++ {
		hann[i] = 0.5 * (1.0 - math.Cos(2.0*math.Pi*float64(i)/float64(n-1)))
	}

	windowed := make([]float64, n)
	for i := 0; i < n; i++ {
		windowed[i] = detrended[i] * hann[i]
	}

	nFFT := nextPow2(n)
	x := make([]complex128, nFFT)
	for i := 0; i < n; i++ {
		x[i] = complex(windowed[i], 0)
	}

	X := fft(x)

	halfN := nFFT / 2
	magnitudes := make([]float64, halfN)
	freqs := make([]float64, halfN)
	for k := 0; k < halfN; k++ {
		freqs[k] = float64(k) * sampleRate / float64(nFFT)
		magnitudes[k] = cmplx.Abs(X[k]) / float64(n)
		if k > 0 {
			magnitudes[k] *= 2.0
		}
	}

	minFreqBin := 1
	maxFreqBin := halfN - 1
	if maxFreqBin <= minFreqBin {
		return FreqAnalysisResult{}
	}

	type binInfo struct {
		idx  int
		freq float64
		mag  float64
	}

	bins := make([]binInfo, 0, maxFreqBin-minFreqBin+1)
	for k := minFreqBin; k <= maxFreqBin; k++ {
		bins = append(bins, binInfo{k, freqs[k], magnitudes[k]})
	}
	sort.Slice(bins, func(i, j int) bool {
		return bins[i].mag > bins[j].mag
	})

	peakCount := 5
	if len(bins) < peakCount {
		peakCount = len(bins)
	}
	peaks := make([]FreqPeak, 0, peakCount)
	for i := 0; i < peakCount; i++ {
		peaks = append(peaks, FreqPeak{
			Freq:      bins[i].freq,
			Magnitude: bins[i].mag,
		})
	}

	dominantFreq := 0.0
	if len(peaks) > 0 {
		dominantFreq = peaks[0].Freq
	}

	var weightedSum, weightTotal float64
	for k := minFreqBin; k <= maxFreqBin; k++ {
		weightedSum += freqs[k] * magnitudes[k]
		weightTotal += magnitudes[k]
	}
	spectralCentroid := 0.0
	if weightTotal > 0 {
		spectralCentroid = weightedSum / weightTotal
	}

	drift := math.Abs(dominantFreq - argMotorFreq)

	var noiseSum float64
	noiseCount := 0
	topPeakSet := make(map[float64]bool)
	for _, p := range peaks {
		topPeakSet[p.Freq] = true
	}
	for _, b := range bins {
		if !topPeakSet[b.freq] {
			noiseSum += b.mag
			noiseCount++
		}
	}
	noiseFloor := 0.0
	if noiseCount > 0 {
		noiseFloor = noiseSum / float64(noiseCount)
	}
	spectralSNR := 0.0
	if noiseFloor > 0 && len(peaks) > 0 {
		spectralSNR = peaks[0].Magnitude / noiseFloor
	}

	isDrifted := drift > argFreqDrift && dominantFreq > 0 && spectralSNR >= argSpecSNR && len(peaks) > 0 && peaks[0].Magnitude >= argMinPeak

	return FreqAnalysisResult{
		DominantFreq:    dominantFreq,
		Peaks:           peaks,
		SpectralCentroid: spectralCentroid,
		SpectralSNR:     spectralSNR,
		DriftHz:         drift,
		IsDrifted:       isDrifted,
	}
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
	ansiRed     = "\033[31m"
	ansiBold    = "\033[1m"
	ansiReset   = "\033[0m"
	ansiYellow  = "\033[33m"
	ansiCyan    = "\033[36m"
	ansiDim     = "\033[2m"
	ansiGreen   = "\033[32m"
	ansiMagenta = "\033[35m"
	ansiBlue    = "\033[34m"
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

func spindleHash(id string) uint32 {
	h := uint32(0)
	for _, c := range id {
		h = h*31 + uint32(c)
	}
	return h
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

func printBearingReport(spindleID string, rec SensorRecord, result FreqAnalysisResult) {
	if _, loaded := bearingSet.LoadOrStore(spindleID, true); loaded {
		cnt := atomic.AddInt64(&bearingRepeatCnt, 1)
		if cnt%100 == 0 {
			msg := fmt.Sprintf("%s  🔩 %s 轴承疲劳持续恶化 (第 %d 次检出) | 主导 %.3f Hz | 行 %d%s\n",
				ansiDim, spindleID, cnt, result.DominantFreq, rec.LineNo, ansiReset)
			outputMu.Lock()
			fmt.Print(msg)
			outputMu.Unlock()
		}
		return
	}
	atomic.AddInt64(&bearingAlertCnt, 1)

	peakLines := ""
	for i, p := range result.Peaks {
		if i >= 3 {
			break
		}
		peakLines += fmt.Sprintf("%s  │  峰值 #%d     │ %.2f Hz  |  幅值 %.3f cN%s\n",
			ansiBlue, i+1, p.Freq, p.Magnitude, ansiReset)
	}

	msg := fmt.Sprintf("%s%s🔩  主轴轴承疲劳报告  🔩%s\n"+
		"%s  ┌──────────────────────────────────────────────────┐%s\n"+
		"%s  │ 锭子编号      │ %s%s\n"+
		"%s  │ 检测时间戳    │ %s%s\n"+
		"%s  │ 触发行号      │ %d%s\n"+
		"%s  │                                              %s\n"+
		"%s  │ ─── 频域分析 ───                             %s\n"+
		"%s  │ 主导频率      │ %.3f Hz%s\n"+
		"%s  │ 标准电机频率  │ %.3f Hz%s\n"+
		"%s  │ 频率漂移量    │ %.3f Hz (阈值 %.1f Hz)%s\n"+
		"%s  │ 频谱质心      │ %.3f Hz%s\n"+
		"%s  │ 频谱信噪比    │ %.1f× (阈值 %.1f×)%s\n"+
		"%s  │                                              %s\n"+
		"%s  │ ─── 主峰频谱 ───                             %s\n"+
		"%s"+
		"%s  │                                              %s\n"+
		"%s  │ ⚠ 判定: 主导频率偏离电机主轴标准频率         %s\n"+
		"%s  │    疑似轴承磨损导致机械共振偏移              %s\n"+
		"%s  └──────────────────────────────────────────────────┘%s\n\n",
		ansiBlue, ansiBold, ansiReset,
		ansiBlue, ansiReset,
		ansiBlue, spindleID, ansiReset,
		ansiBlue, rec.Timestamp, ansiReset,
		ansiBlue, rec.LineNo, ansiReset,
		ansiBlue, ansiReset,
		ansiBlue, ansiReset,
		ansiBlue, result.DominantFreq, ansiReset,
		ansiBlue, argMotorFreq, ansiReset,
		ansiBlue, result.DriftHz, argFreqDrift, ansiReset,
		ansiBlue, result.SpectralCentroid, ansiReset,
		ansiBlue, result.SpectralSNR, argSpecSNR, ansiReset,
		ansiBlue, ansiReset,
		ansiBlue, ansiReset,
		peakLines,
		ansiBlue, ansiReset,
		ansiRed+ansiBold, ansiReset,
		ansiRed+ansiBold, ansiReset,
		ansiBlue, ansiReset,
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

func streamCSV(path string, workerChs []chan SensorRecord) {
	nWorkers := len(workerChs)

	defer func() {
		if r := recover(); r != nil {
			atomic.AddInt64(&panicCount, 1)
			outputMu.Lock()
			fmt.Fprintf(os.Stderr, "%s%s[致命] streamCSV 发生 panic: %v%s\n", ansiRed, ansiBold, r, ansiReset)
			fmt.Fprintf(os.Stderr, "%s堆栈:\n%s%s\n", ansiRed, debug.Stack(), ansiReset)
			outputMu.Unlock()
		}
		for _, ch := range workerChs {
			close(ch)
		}
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

		idx := spindleHash(rec.SpindleID) % uint32(nWorkers)
		workerChs[idx] <- rec
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

			if window.ShouldRunFFT() {
				snapshot := window.Snapshot()
				if len(snapshot) >= 16 {
					result := analyzeSpectrum(snapshot, argSampleRate)
					if result.IsDrifted {
						printBearingReport(rec.SpindleID, rec, result)
					}
				}
				window.ResetFFTClock()
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
	fmt.Printf("%s%s║     织布车间 · 经纱张力异常排查终端  v3.2              ║%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s%s╚═════════════════════════════════════════════════════════╝%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s  数据源         : %s%s\n", ansiYellow, argFile, ansiReset)
	fmt.Printf("%s  滑动窗口       : %d 个数据点%s\n", ansiYellow, argWindow, ansiReset)
	fmt.Printf("%s  Z-Score 阈值   : %.1f σ%s\n", ansiYellow, argZScore, ansiReset)
	fmt.Printf("%s  采样率         : %.1f Hz%s\n", ansiYellow, argSampleRate, ansiReset)
	fmt.Printf("%s  电机标准频率   : %.2f Hz%s\n", ansiYellow, argMotorFreq, ansiReset)
	fmt.Printf("%s  频率漂移阈值   : %.2f Hz%s\n", ansiYellow, argFreqDrift, ansiReset)
	fmt.Printf("%s  频谱信噪比阈值 : %.1f×%s\n", ansiYellow, argSpecSNR, ansiReset)
	fmt.Printf("%s  最小峰值幅值   : %.1f cN%s\n", ansiYellow, argMinPeak, ansiReset)
	fmt.Printf("%s  FFT 分析间隔   : 每 %d 个数据点%s\n", ansiYellow, argFFTInterval, ansiReset)
	fmt.Printf("%s  消费者协程     : %d 个 Goroutine (锭子哈希路由)%s\n", ansiYellow, argWorkers, ansiReset)
	fmt.Printf("%s  跳过表头       : %v%s\n", ansiYellow, argSkipHead, ansiReset)
	fmt.Println()
	fmt.Printf("%s▶ 开始流式扫描 (时域 Z-Score + 频域 FFT 双通道)...%s\n\n", ansiGreen+ansiBold, ansiReset)

	tracker := NewSpindleTracker()

	workerChs := make([]chan SensorRecord, argWorkers)
	perWorkerBuf := chanBufSize/argWorkers + 1
	for i := 0; i < argWorkers; i++ {
		workerChs[i] = make(chan SensorRecord, perWorkerBuf)
	}

	var wg sync.WaitGroup
	for i := 0; i < argWorkers; i++ {
		wg.Add(1)
		go worker(i, tracker, workerChs[i], &wg)
	}

	start := time.Now()
	go streamCSV(argFile, workerChs)

	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				lines := atomic.LoadInt64(&totalLines)
				anomalies := atomic.LoadInt64(&anomalyCount)
				bearings := atomic.LoadInt64(&bearingAlertCnt)
				malformed := atomic.LoadInt64(&malformedCnt)
				panics := atomic.LoadInt64(&panicCount)
				elapsed := time.Since(start).Round(time.Millisecond)
				msg := fmt.Sprintf("%s[进度] %d 行 | 滑脱 %d | 轴承首次 %d | 持续恶化 %d | 畸形 %d | panic %d | %v%s\n",
					ansiDim, lines, anomalies, bearings, atomic.LoadInt64(&bearingRepeatCnt), malformed, panics, elapsed, ansiReset)
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
	bearings := atomic.LoadInt64(&bearingAlertCnt)
	repeats := atomic.LoadInt64(&bearingRepeatCnt)
	malformed := atomic.LoadInt64(&malformedCnt)
	panics := atomic.LoadInt64(&panicCount)
	spindleCount := tracker.Len()

	fmt.Println()
	fmt.Printf("%s%s╔═════════════════════════════════════════════════════════╗%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s%s║                    排 查 报 告                         ║%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("%s%s╚═════════════════════════════════════════════════════════╝%s\n", ansiCyan, ansiBold, ansiReset)
	fmt.Printf("  总处理行数     : %s%d%s\n", ansiBold, lines, ansiReset)
	fmt.Printf("  活跃锭子数     : %s%d%s\n", ansiBold, spindleCount, ansiReset)
	fmt.Printf("  张力滑脱预警   : %s%s%d%s%s\n", ansiRed, ansiBold, anomalies, ansiReset, ansiReset)
	fmt.Printf("  轴承疲劳预警   : %s%s%d%s 个锭子首次报告\n", ansiBlue, ansiBold, bearings, ansiReset)
	if repeats > 0 {
		fmt.Printf("  频域持续检出   : %s%s%d%s 次 (已去重，仅首次完整报告)%s\n", ansiDim, ansiBold, repeats, ansiDim, ansiReset)
	}
	if malformed > 0 {
		fmt.Printf("  畸形行数       : %s%s%d%s%s\n", ansiMagenta, ansiBold, malformed, ansiReset, ansiReset)
	}
	if panics > 0 {
		fmt.Printf("  Panic 捕获     : %s%s%d%s%s (已恢复，未死锁)\n", ansiRed, ansiBold, panics, ansiReset, ansiReset)
	}
	fmt.Printf("  总耗时         : %v\n", elapsed)
	if lines > 0 && elapsed.Seconds() > 0 {
		rate := float64(lines) / elapsed.Seconds()
		fmt.Printf("  处理速率       : %s%.0f 行/秒%s\n", ansiGreen, rate, ansiReset)
	}
	fmt.Println()

	if anomalies > 0 {
		fmt.Printf("%s%s⚠  检测到 %d 条经纱张力突发性滑脱预警，请立即排查相关锭子！%s\n",
			ansiRed, ansiBold, anomalies, ansiReset)
	}
	if bearings > 0 {
		var bearingSpindles []string
		bearingSet.Range(func(key, _ interface{}) bool {
			bearingSpindles = append(bearingSpindles, key.(string))
			return true
		})
		sort.Strings(bearingSpindles)
		fmt.Printf("%s%s🔩  检测到 %d 个锭子存在轴承疲劳共振，请安排维护！%s\n",
			ansiBlue, ansiBold, len(bearingSpindles), ansiReset)
		fmt.Printf("%s  轴承疲劳锭子列表: %v%s\n", ansiBlue, bearingSpindles, ansiReset)
	}
	if anomalies == 0 && bearings == 0 {
		fmt.Printf("%s✓ 未检测到经纱张力突发性滑脱异常及轴承疲劳共振%s\n", ansiGreen, ansiReset)
	}
	fmt.Println()
}
