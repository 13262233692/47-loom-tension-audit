package main

import (
	"fmt"
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
	pointsPerSpindle := 500
	anomalySpindles := []int{3, 7, 15}
	anomalyPositions := []int{250, 300, 400}

	fmt.Fprintln(f, "timestamp,spindle_id,tension_cN")

	rng := rand.New(rand.NewSource(42))
	baseTime := time.Date(2026, 6, 9, 6, 0, 0, 0, time.Local)

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

		t := baseTime
		for p := 0; p < pointsPerSpindle; p++ {
			tension := baseTension + rng.NormFloat64()*noise

			if isAnomalySpindle && p == anomalyPos {
				drop := baseTension * 0.35
				tension = baseTension - drop + rng.NormFloat64()*0.5
			}

			if tension < 0 {
				tension = 0.1
			}

			fmt.Fprintf(f, "%s,%s,%.2f\n",
				t.Format("2006-01-02T15:04:05.000"),
				spindleID,
				tension,
			)
			t = t.Add(100 * time.Millisecond)
		}
	}

	fmt.Fprintf(os.Stderr, "生成完成: %d 个锭子 x %d 数据点 = %d 行\n",
		spindleCount, pointsPerSpindle, spindleCount*pointsPerSpindle)
	fmt.Fprintf(os.Stderr, "异常锭子: SP-004, SP-008, SP-016\n")
	fmt.Fprintf(os.Stderr, "异常位置: 第 250, 300, 400 个数据点 (张力突降约35%%)\n")
}
