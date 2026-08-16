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
	Heaps           int
	Workers         int
	QueueSize       int
	TargetURL       string
	ReceiverPort    int
	ReceiverLatency time.Duration
	OutputFormat    string
	ReportFile      string
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

	// In flood scenario, all tasks are targeted to fire at the EXACT SAME target time
	targetFireTime := time.Now().Add(cfg.TimerDelay)
	scheduledTasks := make([]ScheduledTaskInfo, cfg.TotalRequests)
	var taskMu sync.Mutex

	jobs := make(chan int, cfg.TotalRequests)
	for i := 0; i < cfg.TotalRequests; i++ {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	collector.Start()

	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for jobID := range jobs {
				taskID := fmt.Sprintf("flood-%d-%d", workerID, jobID)
				delay := time.Until(targetFireTime).Seconds()
				if delay < 0.05 {
					delay = 0.05
				}

				latency, success := sendScheduleRequest(client, targetURL, taskID, delay, receiver.URL())
				collector.RecordRequest(latency, success)

				if success {
					taskMu.Lock()
					scheduledTasks[jobID] = ScheduledTaskInfo{
						ID:     taskID,
						FireAt: targetFireTime,
					}
					taskMu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()

	// Wait until flood expires and callbacks drain
	timeToWait := time.Until(targetFireTime) + 15*time.Second
	if timeToWait < 5*time.Second {
		timeToWait = 5 * time.Second
	}
	receivedTotal := receiver.WaitForCount(cfg.TotalRequests, timeToWait)

	collector.Stop()

	for _, task := range scheduledTasks {
		if task.ID == "" {
			continue
		}
		if receivedAt, ok := receiver.GetReceivedTime(task.ID); ok {
			drift := receivedAt.Sub(task.FireAt)
			collector.RecordDrift(drift)
		}
	}

	return collector.BuildResult("Flood & Burst Handling", cfg.TotalRequests, receivedTotal), nil
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
