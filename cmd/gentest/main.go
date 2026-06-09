package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: go run main.go <output.csv>")
		os.Exit(1)
	}

	f, err := os.Create(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "无法创建文件: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	spindleCount := 20
	pointsPerSpindle := 1024
	sampleRate := 10.0
	motorFreq := 2.0
	motorAmplitude := 3.5
	anomalySpindles := []int{3, 7, 15}
	anomalyPositions := []int{500, 600, 800}
	bearingWearSpindles := []int{1, 6, 12}
	bearingWearFreqs := []float64{3.5, 4.2, 0.8}
	bearingWearAmplitudes := []float64{8.0, 7.5, 7.0}

	fmt.Fprintln(f, "timestamp,spindle_id,tension_cN")

	rng := rand.New(rand.NewSource(42))
	baseTime := time.Date(2026, 6, 9, 6, 0, 0, 0, time.Local)

	malformedLines := 0

	for s := 0; s < spindleCount; s++ {
		spindleID := fmt.Sprintf("SP-%03d", s+1)
		baseTension := 45.0 + rng.Float64()*10.0
		noise := 1.5 + rng.Float64()*1.0

		isAnomalySpindle := false
		anomalyPos := -1
		for i, as := range anomalySpindles {
			if s == as {
				isAnomalySpindle = true
				anomalyPos = anomalyPositions[i]
				break
			}
		}

		isBearingWear := false
		bearingFreq := 0.0
		bearingAmp := 0.0
		for i, bs := range bearingWearSpindles {
			if s == bs {
				isBearingWear = true
				bearingFreq = bearingWearFreqs[i]
				bearingAmp = bearingWearAmplitudes[i]
				break
			}
		}

		t := baseTime
		for p := 0; p < pointsPerSpindle; p++ {
			tension := baseTension + rng.NormFloat64()*noise

			motorPhase := 2.0 * math.Pi * motorFreq * float64(p) / sampleRate
			tension += motorAmplitude * math.Sin(motorPhase)

			if isAnomalySpindle && p == anomalyPos {
				drop := baseTension * 0.35
				tension = baseTension - drop + rng.NormFloat64()*0.5
			}

			if isBearingWear {
				phase := 2.0 * math.Pi * bearingFreq * float64(p) / sampleRate
				envelope := 1.0
				progress := float64(p) / float64(pointsPerSpindle)
				envelope = 0.3 + 0.7*progress
				tension += bearingAmp * envelope * math.Sin(phase)
			}

			if tension < 0 {
				tension = 0.1
			}

			if s == 2 && p == 100 {
				fmt.Fprint(f, "2026-06-09T06:00:10.000,SP-003,\n")
				malformedLines++
				continue
			}
			if s == 5 && p == 200 {
				fmt.Fprint(f, "2026-06-09T06:00:20.000,,48.50\n")
				malformedLines++
				continue
			}
			if s == 8 && p == 150 {
				fmt.Fprint(f, ",SP-009,47.30\n")
				malformedLines++
				continue
			}
			if s == 11 && p == 300 {
				fmt.Fprint(f, "2026-06-09T06:00:30.000,SP-012,NAN\n")
				malformedLines++
				continue
			}
			if s == 13 && p == 50 {
				fmt.Fprint(f, "2026-06-09T06:00:05.000,SP-014,-12.50\n")
				malformedLines++
				continue
			}
			if s == 17 && p == 400 {
				fmt.Fprint(f, "2026-06-09T06:00:40.000,SP-018,Inf\n")
				malformedLines++
				continue
			}
			if s == 19 && p == 250 {
				fmt.Fprint(f, "~~GARBAGE_NOISE_!@#$%%^&*()~~\n")
				malformedLines++
				continue
			}
			if s == 4 && p == 350 {
				fmt.Fprint(f, "2026-06-09T06:00:35.000,SP-005\n")
				malformedLines++
				continue
			}
			if s == 9 && p == 80 {
				fmt.Fprint(f, "\n")
				malformedLines++
				continue
			}
			if s == 14 && p == 420 {
				fmt.Fprint(f, "2026-06-09T06:00:42.000,SP-015,4\x008.5\xFF2\n")
				malformedLines++
				continue
			}

			fmt.Fprintf(f, "%s,%s,%.2f\n",
				t.Format("2006-01-02T15:04:05.000"),
				spindleID,
				tension,
			)
			t = t.Add(100 * time.Millisecond)
		}
	}

	totalNormal := spindleCount*pointsPerSpindle - malformedLines
	fmt.Fprintf(os.Stderr, "生成完成: %d 正常行 + %d 畸形行 = %d 行\n",
		totalNormal, malformedLines, totalNormal+malformedLines)
	fmt.Fprintf(os.Stderr, "张力突降锭子: SP-004, SP-008, SP-016\n")
	fmt.Fprintf(os.Stderr, "轴承磨损锭子:\n")
	for i, bs := range bearingWearSpindles {
		fmt.Fprintf(os.Stderr, "  SP-%03d: 磨损共振频率 %.1f Hz (标准 2.0 Hz), 幅值 %.1f cN, 逐渐增强\n",
			bs+1, bearingWearFreqs[i], bearingWearAmplitudes[i])
	}
	fmt.Fprintf(os.Stderr, "采样率: %.1f Hz | 电机标准频率: %.1f Hz | 电机信号幅值: %.1f cN | 漂移阈值: 0.5 Hz\n", sampleRate, motorFreq, motorAmplitude)
}
