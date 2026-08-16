package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type LatencyStats struct {
	Count       int           `json:"count"`
	Min         time.Duration `json:"min"`
	Max         time.Duration `json:"max"`
	Mean        time.Duration `json:"mean"`
	StdDev      time.Duration `json:"std_dev"`
	P50         time.Duration `json:"p50"`
	P90         time.Duration `json:"p90"`
	P95         time.Duration `json:"p95"`
	P99         time.Duration `json:"p99"`
	P999        time.Duration `json:"p999"`
}

type DriftStats struct {
	Count       int           `json:"count"`
	Min         time.Duration `json:"min"`
	Max         time.Duration `json:"max"`
	Mean        time.Duration `json:"mean"`
	StdDev      time.Duration `json:"std_dev"`
	P50         time.Duration `json:"p50"`
	P90         time.Duration `json:"p90"`
	P95         time.Duration `json:"p95"`
	P99         time.Duration `json:"p99"`
}

type BenchmarkResult struct {
	Scenario         string        `json:"scenario"`
	Duration         time.Duration `json:"duration"`
	TotalRequests    int           `json:"total_requests"`
	SuccessRequests  int           `json:"success_requests"`
	FailedRequests   int           `json:"failed_requests"`
	RequestsPerSec   float64       `json:"requests_per_sec"`
	Latency          LatencyStats  `json:"latency"`
	
	// Callback metrics (for accuracy & flood)
	ExpectedCallbacks int          `json:"expected_callbacks,omitempty"`
	ReceivedCallbacks int          `json:"received_callbacks,omitempty"`
	DeliveryRate      float64      `json:"delivery_rate,omitempty"`
	CallbacksPerSec   float64      `json:"callbacks_per_sec,omitempty"`
	Drift             *DriftStats  `json:"drift,omitempty"`
}

type MetricCollector struct {
	mu           sync.Mutex
	latencies    []time.Duration
	drifts       []time.Duration
	successCount int
	failCount    int
	startTime    time.Time
	endTime      time.Time
}

func NewMetricCollector() *MetricCollector {
	return &MetricCollector{
		latencies: make([]time.Duration, 0, 10000),
		drifts:    make([]time.Duration, 0, 10000),
	}
}

func (c *MetricCollector) Start() {
	c.startTime = time.Now()
}

func (c *MetricCollector) Stop() {
	c.endTime = time.Now()
}

func (c *MetricCollector) RecordRequest(latency time.Duration, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latencies = append(c.latencies, latency)
	if success {
		c.successCount++
	} else {
		c.failCount++
	}
}

func (c *MetricCollector) RecordDrift(drift time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drifts = append(c.drifts, drift)
}

func calculateLatencyStats(samples []time.Duration) LatencyStats {
	if len(samples) == 0 {
		return LatencyStats{}
	}

	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	n := len(sorted)
	var sum float64
	for _, d := range sorted {
		sum += float64(d)
	}
	mean := sum / float64(n)

	var varianceSum float64
	for _, d := range sorted {
		diff := float64(d) - mean
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(n))

	percentile := func(p float64) time.Duration {
		idx := int(float64(n-1) * p)
		return sorted[idx]
	}

	return LatencyStats{
		Count:  n,
		Min:    sorted[0],
		Max:    sorted[n-1],
		Mean:   time.Duration(mean),
		StdDev: time.Duration(stdDev),
		P50:    percentile(0.50),
		P90:    percentile(0.90),
		P95:    percentile(0.95),
		P99:    percentile(0.99),
		P999:   percentile(0.999),
	}
}

func calculateDriftStats(samples []time.Duration) *DriftStats {
	if len(samples) == 0 {
		return nil
	}

	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	n := len(sorted)
	var sum float64
	for _, d := range sorted {
		sum += float64(d)
	}
	mean := sum / float64(n)

	var varianceSum float64
	for _, d := range sorted {
		diff := float64(d) - mean
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(n))

	percentile := func(p float64) time.Duration {
		idx := int(float64(n-1) * p)
		return sorted[idx]
	}

	return &DriftStats{
		Count:  n,
		Min:    sorted[0],
		Max:    sorted[n-1],
		Mean:   time.Duration(mean),
		StdDev: time.Duration(stdDev),
		P50:    percentile(0.50),
		P90:    percentile(0.90),
		P95:    percentile(0.95),
		P99:    percentile(0.99),
	}
}

func (c *MetricCollector) BuildResult(scenario string, expectedCallbacks int, receivedCallbacks int) *BenchmarkResult {
	duration := c.endTime.Sub(c.startTime)
	if duration <= 0 {
		duration = time.Millisecond
	}

	totalReqs := c.successCount + c.failCount
	rps := float64(totalReqs) / duration.Seconds()

	result := &BenchmarkResult{
		Scenario:        scenario,
		Duration:        duration,
		TotalRequests:   totalReqs,
		SuccessRequests: c.successCount,
		FailedRequests:  c.failCount,
		RequestsPerSec:  rps,
		Latency:         calculateLatencyStats(c.latencies),
	}

	if expectedCallbacks > 0 || receivedCallbacks > 0 {
		result.ExpectedCallbacks = expectedCallbacks
		result.ReceivedCallbacks = receivedCallbacks
		if expectedCallbacks > 0 {
			result.DeliveryRate = (float64(receivedCallbacks) / float64(expectedCallbacks)) * 100.0
		}
		result.CallbacksPerSec = float64(receivedCallbacks) / duration.Seconds()
		result.Drift = calculateDriftStats(c.drifts)
	}

	return result
}

func PrintTable(res *BenchmarkResult) {
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Printf("                    BENCHMARK RESULTS: %s\n", strings.ToUpper(res.Scenario))
	fmt.Println("================================================================================")
	fmt.Printf(" Total Duration:       %v\n", res.Duration.Round(time.Millisecond))
	fmt.Printf(" Total Requests:       %d\n", res.TotalRequests)
	fmt.Printf(" Success Requests:     %d (%.2f%%)\n", res.SuccessRequests, (float64(res.SuccessRequests)/float64(res.TotalRequests))*100)
	fmt.Printf(" Failed Requests:      %d\n", res.FailedRequests)
	fmt.Printf(" Ingestion Throughput: %.2f req/sec\n", res.RequestsPerSec)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(" HTTP Scheduling Latency:")
	fmt.Printf("   Min:    %10v | Mean:   %10v | StdDev: %10v\n", res.Latency.Min.Round(time.Microsecond), res.Latency.Mean.Round(time.Microsecond), res.Latency.StdDev.Round(time.Microsecond))
	fmt.Printf("   P50:    %10v | P90:    %10v | P95:    %10v\n", res.Latency.P50.Round(time.Microsecond), res.Latency.P90.Round(time.Microsecond), res.Latency.P95.Round(time.Microsecond))
	fmt.Printf("   P99:    %10v | P99.9:  %10v | Max:    %10v\n", res.Latency.P99.Round(time.Microsecond), res.Latency.P999.Round(time.Microsecond), res.Latency.Max.Round(time.Microsecond))

	if res.Drift != nil {
		fmt.Println("--------------------------------------------------------------------------------")
		fmt.Println(" Timer Accuracy & Callback Drift (Actual Arrival - Scheduled Time):")
		fmt.Printf("   Callbacks Received: %d / %d (%.2f%% delivery rate)\n", res.ReceivedCallbacks, res.ExpectedCallbacks, res.DeliveryRate)
		fmt.Printf("   Min Drift:  %10v | Mean Drift: %10v | StdDev: %10v\n", res.Drift.Min.Round(time.Microsecond), res.Drift.Mean.Round(time.Microsecond), res.Drift.StdDev.Round(time.Microsecond))
		fmt.Printf("   P50 Drift:  %10v | P90 Drift:  %10v | P95 Drift: %10v\n", res.Drift.P50.Round(time.Microsecond), res.Drift.P90.Round(time.Microsecond), res.Drift.P95.Round(time.Microsecond))
		fmt.Printf("   P99 Drift:  %10v | Max Drift:  %10v\n", res.Drift.P99.Round(time.Microsecond), res.Drift.Max.Round(time.Microsecond))
	}
	fmt.Println("================================================================================")
	fmt.Println()
}

func ToMarkdown(res *BenchmarkResult) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("## Benchmark Results: `%s`\n\n", res.Scenario))
	sb.WriteString("| Metric | Value |\n")
	sb.WriteString("|---|---|\n")
	sb.WriteString(fmt.Sprintf("| **Duration** | %v |\n", res.Duration.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("| **Requests** | %d (Success: %d, Failed: %d) |\n", res.TotalRequests, res.SuccessRequests, res.FailedRequests))
	sb.WriteString(fmt.Sprintf("| **Throughput** | `%.2f req/s` |\n", res.RequestsPerSec))
	sb.WriteString(fmt.Sprintf("| **Latency P50** | `%v` |\n", res.Latency.P50.Round(time.Microsecond)))
	sb.WriteString(fmt.Sprintf("| **Latency P95** | `%v` |\n", res.Latency.P95.Round(time.Microsecond)))
	sb.WriteString(fmt.Sprintf("| **Latency P99** | `%v` |\n", res.Latency.P99.Round(time.Microsecond)))

	if res.Drift != nil {
		sb.WriteString(fmt.Sprintf("| **Delivery Rate** | `%.2f%%` (%d/%d) |\n", res.DeliveryRate, res.ReceivedCallbacks, res.ExpectedCallbacks))
		sb.WriteString(fmt.Sprintf("| **Drift Mean** | `%v` |\n", res.Drift.Mean.Round(time.Microsecond)))
		sb.WriteString(fmt.Sprintf("| **Drift P50** | `%v` |\n", res.Drift.P50.Round(time.Microsecond)))
		sb.WriteString(fmt.Sprintf("| **Drift P95** | `%v` |\n", res.Drift.P95.Round(time.Microsecond)))
		sb.WriteString(fmt.Sprintf("| **Drift P99** | `%v` |\n", res.Drift.P99.Round(time.Microsecond)))
		sb.WriteString(fmt.Sprintf("| **Drift Max** | `%v` |\n", res.Drift.Max.Round(time.Microsecond)))
	}
	sb.WriteString("\n")
	return sb.String()
}

func ToJSON(res *BenchmarkResult) string {
	b, _ := json.MarshalIndent(res, "", "  ")
	return string(b)
}
