package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	scenarioFlag := flag.String("scenario", "throughput", "Benchmark scenario: throughput | accuracy | flood | scaling | grid | all")
	requestsFlag := flag.Int("requests", 5000, "Total number of timer requests to send")
	concurrencyFlag := flag.Int("concurrency", 50, "Number of concurrent worker goroutines")
	rateLimitFlag := flag.Int("rate", 0, "Max requests per second throttle (0 for maximum speed)")
	delayFlag := flag.Duration("delay", 1*time.Second, "Timer expiration delay for accuracy/flood tests (e.g. 500ms, 1s, 2s)")
	durationFlag := flag.Duration("duration", 0, "Total duration for sustained flood / soak tests (e.g. 30s, 1m). Overrides -waves if > 0")
	wavesFlag := flag.Int("waves", 1, "Number of flood waves to blast")
	waveIntervalFlag := flag.Duration("wave-interval", 0, "Time interval between flood waves (default: delay + 500ms)")
	waveSizeFlag := flag.Int("wave-size", 0, "Number of tasks per flood wave (default: -requests)")
	heapsFlag := flag.Int("heaps", 4, "Number of heaps for embedded atimer instance")
	workersFlag := flag.Int("workers", 4, "Number of notification workers per heap")
	heapsListFlag := flag.String("heaps-list", "1,2,4,8,16,32", "Comma-separated heaps list for grid exploration (e.g. 1,2,4,8,16)")
	workersListFlag := flag.String("workers-list", "1,2,4,8,16", "Comma-separated workers per heap list for grid exploration")
	plotFileFlag := flag.String("plot-file", "benchmark_results/drift_plot.html", "Path to save interactive HTML 3D/Heatmap plot")
	csvFileFlag := flag.String("csv-file", "benchmark_results/drift_grid.csv", "Path to save grid sweep CSV results")
	queueSizeFlag := flag.Int("queue-size", 100000, "Task capacity per heap")
	targetURLFlag := flag.String("target", "", "Target URL of running atimer server (e.g. http://localhost:8080). If empty, runs embedded.")
	receiverPortFlag := flag.Int("receiver-port", 0, "Port for mock callback receiver (0 = auto ephemeral)")
	receiverLatencyFlag := flag.Duration("receiver-latency", 0, "Simulated callback response delay (simulating slow webhook endpoints)")
	outputFlag := flag.String("output", "table", "Output format: table | json | markdown")
	reportFileFlag := flag.String("report-file", "", "Optional path to write benchmark report (e.g. benchmark_results/report.md)")
	verboseFlag := flag.Bool("verbose", false, "Enable verbose server logging")
	flag.Parse()

	if !*verboseFlag {
		log.SetOutput(io.Discard)
	}

	cfg := BenchmarkConfig{
		Scenario:        strings.ToLower(*scenarioFlag),
		TotalRequests:   *requestsFlag,
		Concurrency:     *concurrencyFlag,
		RateLimit:       *rateLimitFlag,
		TimerDelay:      *delayFlag,
		Duration:        *durationFlag,
		Waves:           *wavesFlag,
		WaveInterval:    *waveIntervalFlag,
		WaveSize:        *waveSizeFlag,
		Heaps:           *heapsFlag,
		Workers:         *workersFlag,
		HeapsList:       parseIntList(*heapsListFlag),
		WorkersList:     parseIntList(*workersListFlag),
		PlotFile:        *plotFileFlag,
		CSVFile:         *csvFileFlag,
		QueueSize:       *queueSizeFlag,
		TargetURL:       *targetURLFlag,
		ReceiverPort:    *receiverPortFlag,
		ReceiverLatency: *receiverLatencyFlag,
		OutputFormat:    strings.ToLower(*outputFlag),
		ReportFile:      *reportFileFlag,
	}

	modeDesc := "Embedded Instance"
	if cfg.TargetURL != "" {
		modeDesc = fmt.Sprintf("External Server (%s)", cfg.TargetURL)
	}

	fmt.Println("================================================================================")
	fmt.Println("                       ATIMER HIGH-PERFORMANCE BENCHMARK                        ")
	fmt.Println("================================================================================")
	fmt.Printf(" Mode:        %s\n", modeDesc)
	fmt.Printf(" Scenario:    %s\n", cfg.Scenario)
	fmt.Printf(" Requests:    %d\n", cfg.TotalRequests)
	fmt.Printf(" Concurrency: %d workers\n", cfg.Concurrency)
	if cfg.RateLimit > 0 {
		fmt.Printf(" Rate Limit:  %d req/s\n", cfg.RateLimit)
	}
	if cfg.TargetURL == "" {
		fmt.Printf(" Config:      %d heaps, %d workers/heap, %d queue size/heap\n", cfg.Heaps, cfg.Workers, cfg.QueueSize)
	}
	fmt.Println("================================================================================")

	var results []*BenchmarkResult

	switch cfg.Scenario {
	case "throughput":
		res, err := RunThroughputScenario(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running throughput benchmark: %v\n", err)
			os.Exit(1)
		}
		results = append(results, res)

	case "accuracy":
		res, err := RunAccuracyScenario(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running accuracy benchmark: %v\n", err)
			os.Exit(1)
		}
		results = append(results, res)

	case "flood":
		res, err := RunFloodScenario(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running flood benchmark: %v\n", err)
			os.Exit(1)
		}
		results = append(results, res)

	case "scaling":
		sweepResults, err := RunScalingSweep(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running scaling benchmark: %v\n", err)
			os.Exit(1)
		}
		results = append(results, sweepResults...)

	case "grid":
		gridRes, err := RunGridSweep(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running grid sweep: %v\n", err)
			os.Exit(1)
		}
		PrintGridTerminalTable(gridRes)
		if cfg.CSVFile != "" {
			if err := SaveGridCSV(gridRes, cfg.CSVFile); err == nil {
				fmt.Printf(" Grid data exported to CSV: %s\n", cfg.CSVFile)
			}
		}
		if cfg.PlotFile != "" {
			if err := GenerateInteractivePlotHTML(gridRes, cfg.PlotFile); err == nil {
				fmt.Printf(" Interactive 3D / Heatmap plot generated: %s\n", cfg.PlotFile)
				fmt.Printf(" -> Open this file in your browser to view the interactive 3D Surface & Heatmap:\n    file://%s/%s\n\n", mustGetCwd(), cfg.PlotFile)
			}
		}

	case "all":
		fmt.Println("\n>>> [1/4] Running Ingestion Throughput Test...")
		if res, err := RunThroughputScenario(cfg); err == nil {
			results = append(results, res)
			PrintTable(res)
		}

		fmt.Println("\n>>> [2/4] Running Timer Accuracy & Drift Test...")
		accCfg := cfg
		accCfg.TotalRequests = min(cfg.TotalRequests, 2000)
		if res, err := RunAccuracyScenario(accCfg); err == nil {
			results = append(results, res)
			PrintTable(res)
		}

		fmt.Println("\n>>> [3/4] Running Task Flood / Burst Test...")
		floodCfg := cfg
		floodCfg.TotalRequests = min(cfg.TotalRequests, 2000)
		if res, err := RunFloodScenario(floodCfg); err == nil {
			results = append(results, res)
			PrintTable(res)
		}

		fmt.Println("\n>>> [4/4] Running Multi-Heap Scaling Sweep...")
		sweepCfg := cfg
		sweepCfg.TotalRequests = min(cfg.TotalRequests, 5000)
		if sweepResults, err := RunScalingSweep(sweepCfg); err == nil {
			results = append(results, sweepResults...)
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown scenario '%s'. Available: throughput, accuracy, flood, scaling, grid, all\n", cfg.Scenario)
		os.Exit(1)
	}

	// Output Formatting
	if cfg.Scenario != "all" && cfg.Scenario != "grid" {
		for _, res := range results {
			if cfg.OutputFormat == "json" {
				fmt.Println(ToJSON(res))
			} else if cfg.OutputFormat == "markdown" {
				fmt.Print(ToMarkdown(res))
			} else {
				PrintTable(res)
			}
		}
	}

	// Save Report if requested
	if cfg.ReportFile != "" {
		var reportContent string
		if strings.HasSuffix(cfg.ReportFile, ".json") {
			for _, r := range results {
				reportContent += ToJSON(r) + "\n"
			}
		} else {
			reportContent = "# atimer Benchmark Report\n\n"
			reportContent += fmt.Sprintf("Generated at: %s\n\n", time.Now().Format(time.RFC3339))
			for _, r := range results {
				reportContent += ToMarkdown(r)
			}
		}

		if dir := filepath.Dir(cfg.ReportFile); dir != "." && dir != "" {
			_ = os.MkdirAll(dir, 0755)
		}
		if err := os.WriteFile(cfg.ReportFile, []byte(reportContent), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write report file: %v\n", err)
		} else {
			fmt.Printf("Benchmark report saved to %s\n", cfg.ReportFile)
		}
	}
}

func parseIntList(s string) []int {
	parts := strings.Split(s, ",")
	var res []int
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var val int
		if _, err := fmt.Sscanf(p, "%d", &val); err == nil && val > 0 {
			res = append(res, val)
		}
	}
	return res
}

func mustGetCwd() string {
	cwd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return cwd
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
