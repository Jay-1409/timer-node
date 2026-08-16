package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jay-1409/atimer/internals/timer"
)

type BenchmarkConfig struct {
	Scenario        string
	TotalRequests   int
	Concurrency     int
	RateLimit       int // RPS limit (0 = unlimited)
	TimerDelay      time.Duration
	Duration        time.Duration // Total duration for sustained flood / soak tests
	Waves           int           // Number of flood burst waves
	WaveInterval    time.Duration // Time between flood waves
	WaveSize        int           // Requests per flood wave
	Heaps           int
	Workers         int
	HeapsList       []int         // List of heaps to test in grid scenario
	WorkersList     []int         // List of workers per heap to test in grid scenario
	QueueSize       int
	TargetURL       string
	ReceiverPort    int
	ReceiverLatency time.Duration
	OutputFormat    string
	ReportFile      string
	PlotFile        string        // Path to output interactive HTML plot
	CSVFile         string        // Path to output grid CSV data
}

type ScheduledTaskInfo struct {
	ID     string
	FireAt time.Time
}

func getHTTPClient(concurrency int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 4,
			MaxIdleConnsPerHost: concurrency * 4,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   false,
		},
		Timeout: 15 * time.Second,
	}
}

// StartEmbeddedServer launches an in-process Timer and returns its HTTP base URL and cleanup func.
func StartEmbeddedServer(heaps, queueSize, workers int) (string, *timer.Timer, func(), error) {
	t := timer.NewTimer(heaps, queueSize, workers)
	t.Start()

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return "", nil, nil, fmt.Errorf("failed to listen on ephemeral port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	mux := http.NewServeMux()
	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := r.FormValue("id")
		fireInSecStr := r.FormValue("timer_time")
		callbackURL := r.FormValue("callback_url")

		if id == "" || fireInSecStr == "" || callbackURL == "" {
			http.Error(w, "Missing required parameters", http.StatusBadRequest)
			return
		}

		var fireInSec float64
		if _, err := fmt.Sscanf(fireInSecStr, "%f", &fireInSec); err != nil || fireInSec < 0 {
			http.Error(w, "Invalid timer_time", http.StatusBadRequest)
			return
		}

		task := &timer.TimerTask{
			ID:          id,
			FireAt:      time.Now().Add(time.Duration(fireInSec * float64(time.Second))),
			CallBackURL: callbackURL,
		}

		heapID, ok := t.AddTask(task)
		if !ok {
			http.Error(w, fmt.Sprintf("Heap %d queue capacity reached", heapID), http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "success: routed to heap %d\n", heapID)
	})

	server := &http.Server{Handler: mux}
	go func() {
		_ = server.Serve(listener)
	}()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	cleanup := func() {
		_ = server.Close()
	}

	return baseURL, t, cleanup, nil
}

func sendScheduleRequest(client *http.Client, endpoint string, id string, timerSec float64, callbackURL string) (time.Duration, bool) {
	data := url.Values{}
	data.Set("id", id)
	data.Set("timer_time", fmt.Sprintf("%.4f", timerSec))
	data.Set("callback_url", callbackURL)

	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return 0, false
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start)

	if err != nil {
		return latency, false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return latency, resp.StatusCode == http.StatusOK
}

func RunThroughputScenario(cfg BenchmarkConfig) (*BenchmarkResult, error) {
	var targetURL string
	if cfg.TargetURL != "" {
		targetURL = strings.TrimRight(cfg.TargetURL, "/") + "/api"
	} else {
		embeddedURL, _, cleanup, err := StartEmbeddedServer(cfg.Heaps, cfg.QueueSize, cfg.Workers)
		if err != nil {
			return nil, err
		}
		defer cleanup()
		targetURL = embeddedURL + "/api"
	}

	collector := NewMetricCollector()
	client := getHTTPClient(cfg.Concurrency)

	jobs := make(chan int, cfg.TotalRequests)
	for i := 0; i < cfg.TotalRequests; i++ {
		jobs <- i
	}
	close(jobs)

	var rateTicker *time.Ticker
	if cfg.RateLimit > 0 {
		rateTicker = time.NewTicker(time.Second / time.Duration(cfg.RateLimit))
		defer rateTicker.Stop()
	}

	var wg sync.WaitGroup
	collector.Start()

	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for jobID := range jobs {
				if rateTicker != nil {
					<-rateTicker.C
				}
				taskID := fmt.Sprintf("tp-%d-%d", workerID, jobID)
				// Delay in the far future so execution doesn't consume callback resources
				latency, success := sendScheduleRequest(client, targetURL, taskID, 3600, "http://127.0.0.1:9999/null")
				collector.RecordRequest(latency, success)
			}
		}(w)
	}

	wg.Wait()
	collector.Stop()

	return collector.BuildResult("Throughput (Ingestion Load)", 0, 0), nil
}

func RunAccuracyScenario(cfg BenchmarkConfig) (*BenchmarkResult, error) {
	receiver, err := NewMockCallbackReceiver(cfg.ReceiverPort, cfg.ReceiverLatency)
	if err != nil {
		return nil, fmt.Errorf("failed to start mock receiver: %w", err)
	}
	receiver.Start()
	defer receiver.Stop()

	var targetURL string
	if cfg.TargetURL != "" {
		targetURL = strings.TrimRight(cfg.TargetURL, "/") + "/api"
	} else {
		embeddedURL, _, cleanup, err := StartEmbeddedServer(cfg.Heaps, cfg.QueueSize, cfg.Workers)
		if err != nil {
			return nil, err
		}
		defer cleanup()
		targetURL = embeddedURL + "/api"
	}

	collector := NewMetricCollector()
	client := getHTTPClient(cfg.Concurrency)

	scheduledTasks := make([]ScheduledTaskInfo, cfg.TotalRequests)
	var taskMu sync.Mutex

	jobs := make(chan int, cfg.TotalRequests)
	for i := 0; i < cfg.TotalRequests; i++ {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	collector.Start()

	delaySec := cfg.TimerDelay.Seconds()
	if delaySec <= 0 {
		delaySec = 1.0
	}

	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for jobID := range jobs {
				taskID := fmt.Sprintf("acc-%d-%d", workerID, jobID)
				expectedFireAt := time.Now().Add(cfg.TimerDelay)

				latency, success := sendScheduleRequest(client, targetURL, taskID, delaySec, receiver.URL())
				collector.RecordRequest(latency, success)

				if success {
					taskMu.Lock()
					scheduledTasks[jobID] = ScheduledTaskInfo{
						ID:     taskID,
						FireAt: expectedFireAt,
					}
					taskMu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()

	// Wait for all callbacks to arrive
	maxWait := cfg.TimerDelay + 10*time.Second
	receivedTotal := receiver.WaitForCount(cfg.TotalRequests, maxWait)

	collector.Stop()

	// Compute drift for each scheduled task
	for _, task := range scheduledTasks {
		if task.ID == "" {
			continue
		}
		if receivedAt, ok := receiver.GetReceivedTime(task.ID); ok {
			drift := receivedAt.Sub(task.FireAt)
			collector.RecordDrift(drift)
		}
	}

	return collector.BuildResult("Accuracy & Drift Analysis", cfg.TotalRequests, receivedTotal), nil
}

func RunFloodScenario(cfg BenchmarkConfig) (*BenchmarkResult, error) {
	receiver, err := NewMockCallbackReceiver(cfg.ReceiverPort, cfg.ReceiverLatency)
	if err != nil {
		return nil, fmt.Errorf("failed to start mock receiver: %w", err)
	}
	receiver.Start()
	defer receiver.Stop()

	var targetURL string
	if cfg.TargetURL != "" {
		targetURL = strings.TrimRight(cfg.TargetURL, "/") + "/api"
	} else {
		embeddedURL, _, cleanup, err := StartEmbeddedServer(cfg.Heaps, cfg.QueueSize, cfg.Workers)
		if err != nil {
			return nil, err
		}
		defer cleanup()
		targetURL = embeddedURL + "/api"
	}

	collector := NewMetricCollector()
	client := getHTTPClient(cfg.Concurrency)

	// Determine wave parameters
	totalWaves := cfg.Waves
	if totalWaves <= 0 {
		totalWaves = 1
	}
	waveSize := cfg.WaveSize
	if waveSize <= 0 {
		if cfg.Waves > 1 {
			waveSize = cfg.TotalRequests / cfg.Waves
			if waveSize <= 0 {
				waveSize = 1000
			}
		} else {
			waveSize = cfg.TotalRequests
		}
	}
	waveInterval := cfg.WaveInterval
	if waveInterval <= 0 {
		waveInterval = cfg.TimerDelay + 500*time.Millisecond
	}

	// If Duration is set, calculate total waves to fill the duration
	if cfg.Duration > 0 {
		calculatedWaves := int(cfg.Duration / waveInterval)
		if calculatedWaves < 1 {
			calculatedWaves = 1
		}
		totalWaves = calculatedWaves
	}

	totalScheduledCount := totalWaves * waveSize

	fmt.Printf(" [Sustained Flood] Starting %d waves of %d tasks (Total: %d tasks, Wave Interval: %v, Timer Delay: %v)\n",
		totalWaves, waveSize, totalScheduledCount, waveInterval, cfg.TimerDelay)
	if cfg.ReceiverLatency > 0 {
		fmt.Printf(" [Sustained Flood] Mock Callback Processing Delay: %v (Simulating slow endpoints)\n", cfg.ReceiverLatency)
	}
	fmt.Println("--------------------------------------------------------------------------------")

	var allScheduledTasks []ScheduledTaskInfo
	var taskMu sync.Mutex

	collector.Start()
	benchStart := time.Now()

	for wave := 0; wave < totalWaves; wave++ {
		waveStartTime := time.Now()
		targetFireTime := waveStartTime.Add(cfg.TimerDelay)

		waveJobs := make(chan int, waveSize)
		for j := 0; j < waveSize; j++ {
			waveJobs <- j
		}
		close(waveJobs)

		var wg sync.WaitGroup
		for w := 0; w < cfg.Concurrency; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for jobID := range waveJobs {
					taskID := fmt.Sprintf("flood-w%d-%d-%d", wave, workerID, jobID)
					delay := time.Until(targetFireTime).Seconds()
					if delay < 0.01 {
						delay = 0.01
					}

					latency, success := sendScheduleRequest(client, targetURL, taskID, delay, receiver.URL())
					collector.RecordRequest(latency, success)

					if success {
						taskMu.Lock()
						allScheduledTasks = append(allScheduledTasks, ScheduledTaskInfo{
							ID:     taskID,
							FireAt: targetFireTime,
						})
						taskMu.Unlock()
					}
				}
			}(w)
		}

		wg.Wait()

		receivedSoFar := receiver.TotalReceived()
		expectedSoFar := (wave + 1) * waveSize
		fmt.Printf("   -> [Wave %2d/%2d] Scheduled %d tasks | Callbacks Drained: %d / %d (Elapsed: %v)\n",
			wave+1, totalWaves, waveSize, receivedSoFar, expectedSoFar, time.Since(benchStart).Round(time.Millisecond))

		// Wait until next wave if more waves remain
		if wave < totalWaves-1 {
			timeElapsed := time.Since(waveStartTime)
			if timeElapsed < waveInterval {
				time.Sleep(waveInterval - timeElapsed)
			}
		}
	}

	// Wait for final wave drain
	fmt.Println("   -> Waiting for final callback wave to drain...")
	maxDrainWait := cfg.TimerDelay + 30*time.Second
	receivedTotal := receiver.WaitForCount(totalScheduledCount, maxDrainWait)

	collector.Stop()

	// Compute drift for all scheduled tasks
	for _, task := range allScheduledTasks {
		if task.ID == "" {
			continue
		}
		if receivedAt, ok := receiver.GetReceivedTime(task.ID); ok {
			drift := receivedAt.Sub(task.FireAt)
			collector.RecordDrift(drift)
		}
	}

	return collector.BuildResult("Flood & Burst Handling (Sustained)", totalScheduledCount, receivedTotal), nil
}

func RunScalingSweep(cfg BenchmarkConfig) ([]*BenchmarkResult, error) {
	heapOptions := []int{1, 2, 4, 8, 16}
	results := make([]*BenchmarkResult, 0, len(heapOptions))

	fmt.Printf("\nRunning Multi-Heap Shard Scaling Benchmark (%d requests per tier, %d concurrency)...\n", cfg.TotalRequests, cfg.Concurrency)
	fmt.Println("--------------------------------------------------------------------------------")

	for _, heaps := range heapOptions {
		tierCfg := cfg
		tierCfg.Heaps = heaps
		tierCfg.TargetURL = "" // Use embedded with specific heap count

		res, err := RunThroughputScenario(tierCfg)
		if err != nil {
			return nil, fmt.Errorf("failed running tier with %d heaps: %w", heaps, err)
		}
		res.Scenario = fmt.Sprintf("Sharded Scale: %d Heaps", heaps)
		results = append(results, res)
		fmt.Printf("   [%2d Heaps] Throughput: %8.2f req/s | P50: %8v | P99: %8v\n",
			heaps, res.RequestsPerSec, res.Latency.P50.Round(time.Microsecond), res.Latency.P99.Round(time.Microsecond))
	}
	fmt.Println("--------------------------------------------------------------------------------")

	return results, nil
}

func RunGridSweep(cfg BenchmarkConfig) (*GridResult, error) {
	heapsList := cfg.HeapsList
	if len(heapsList) == 0 {
		heapsList = []int{1, 2, 4, 8, 16, 32}
	}
	workersList := cfg.WorkersList
	if len(workersList) == 0 {
		workersList = []int{1, 2, 4, 8, 16}
	}

	totalCombinations := len(heapsList) * len(workersList)
	fmt.Printf("\n================================================================================\n")
	fmt.Printf("  RUNNING 2D PARAMETER EXPLORATION: %d Combinations (%d requests/test)\n", totalCombinations, cfg.TotalRequests)
	fmt.Printf("  Heaps:   %v\n", heapsList)
	fmt.Printf("  Workers: %v per heap\n", workersList)
	fmt.Printf("================================================================================\n\n")

	gridRes := &GridResult{
		HeapsList:   heapsList,
		WorkersList: workersList,
		Points:      make([]GridPoint, 0, totalCombinations),
		Matrix:      make([][]float64, len(heapsList)),
	}

	idx := 0
	for hIdx, h := range heapsList {
		gridRes.Matrix[hIdx] = make([]float64, len(workersList))
		for wIdx, w := range workersList {
			idx++
			cellCfg := cfg
			cellCfg.Heaps = h
			cellCfg.Workers = w
			cellCfg.TargetURL = "" // Embedded server with specific (H, W)
			cellCfg.Waves = 1
			cellCfg.Duration = 0

			fmt.Printf(" [%2d/%2d] Testing Heaps=%-2d | Workers=%-2d (Total Workers=%-3d)... ",
				idx, totalCombinations, h, w, h*w)

			floodRes, err := RunFloodScenario(cellCfg)
			if err != nil {
				return nil, fmt.Errorf("grid cell (H=%d, W=%d) failed: %w", h, w, err)
			}

			maxDrift := time.Duration(0)
			p99Drift := time.Duration(0)
			p95Drift := time.Duration(0)
			p50Drift := time.Duration(0)
			meanDrift := time.Duration(0)

			if floodRes.Drift != nil {
				maxDrift = floodRes.Drift.Max
				p99Drift = floodRes.Drift.P99
				p95Drift = floodRes.Drift.P95
				p50Drift = floodRes.Drift.P50
				meanDrift = floodRes.Drift.Mean
			}

			maxDriftMs := float64(maxDrift) / float64(time.Millisecond)
			gridRes.Matrix[hIdx][wIdx] = maxDriftMs

			point := GridPoint{
				Heaps:        h,
				Workers:      w,
				TotalWorkers: h * w,
				MaxDrift:     maxDrift,
				P99Drift:     p99Drift,
				P95Drift:     p95Drift,
				P50Drift:     p50Drift,
				MeanDrift:    meanDrift,
				Throughput:   floodRes.RequestsPerSec,
				DeliveryRate: floodRes.DeliveryRate,
			}
			gridRes.Points = append(gridRes.Points, point)

			fmt.Printf("=> Max Drift: %7.2fms | P99: %7.2fms | P50: %7.2fms\n",
				maxDriftMs, float64(p99Drift)/float64(time.Millisecond), float64(p50Drift)/float64(time.Millisecond))
		}
	}

	return gridRes, nil
}
