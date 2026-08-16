package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type GridPoint struct {
	Heaps        int           `json:"heaps"`
	Workers      int           `json:"workers"`
	TotalWorkers int           `json:"total_workers"`
	MaxDrift     time.Duration `json:"max_drift"`
	P99Drift     time.Duration `json:"p99_drift"`
	P95Drift     time.Duration `json:"p95_drift"`
	P50Drift     time.Duration `json:"p50_drift"`
	MeanDrift    time.Duration `json:"mean_drift"`
	Throughput   float64       `json:"throughput"`
	DeliveryRate float64       `json:"delivery_rate"`
}

type GridResult struct {
	HeapsList   []int         `json:"heaps_list"`
	WorkersList []int         `json:"workers_list"`
	Points      []GridPoint   `json:"points"`
	Matrix      [][]float64   `json:"matrix_ms"` // [heaps_idx][workers_idx] -> max drift in ms
}

func PrintGridTerminalTable(res *GridResult) {
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println("       MAX DRIFT MATRIX (in milliseconds): HEAPS vs WORKERS PER HEAP            ")
	fmt.Println("================================================================================")
	fmt.Print(" Heaps \\ Workers |")
	for _, w := range res.WorkersList {
		fmt.Printf("   W=%-4d |", w)
	}
	fmt.Println()
	fmt.Println("-----------------+" + strings.Repeat("-----------+", len(res.WorkersList)))

	for hIdx, h := range res.HeapsList {
		fmt.Printf("   Heaps=%-7d |", h)
		for wIdx := range res.WorkersList {
			val := res.Matrix[hIdx][wIdx]
			fmt.Printf(" %7.2fms |", val)
		}
		fmt.Println()
	}
	fmt.Println("================================================================================")
	fmt.Println()
}

func SaveGridCSV(res *GridResult, filename string) error {
	if dir := filepath.Dir(filename); dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0755)
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	_ = w.Write([]string{
		"heaps",
		"workers_per_heap",
		"total_workers",
		"max_drift_ms",
		"p99_drift_ms",
		"p95_drift_ms",
		"p50_drift_ms",
		"mean_drift_ms",
		"throughput_rps",
		"delivery_rate_pct",
	})

	for _, p := range res.Points {
		_ = w.Write([]string{
			fmt.Sprintf("%d", p.Heaps),
			fmt.Sprintf("%d", p.Workers),
			fmt.Sprintf("%d", p.TotalWorkers),
			fmt.Sprintf("%.3f", float64(p.MaxDrift)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(p.P99Drift)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(p.P95Drift)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(p.P50Drift)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(p.MeanDrift)/float64(time.Millisecond)),
			fmt.Sprintf("%.2f", p.Throughput),
			fmt.Sprintf("%.2f", p.DeliveryRate),
		})
	}

	return nil
}

func GenerateInteractivePlotHTML(res *GridResult, filename string) error {
	dataJSON, err := json.Marshal(res)
	if err != nil {
		return err
	}

	html := fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>atimer: Heaps vs Workers vs Max Drift</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {
      --bg: #0d1117;
      --card-bg: #161b22;
      --border: #30363d;
      --text: #c9d1d9;
      --heading: #f0f6fc;
      --accent: #58a6ff;
    }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      margin: 0;
      padding: 24px;
    }
    .header {
      max-width: 1200px;
      margin: 0 auto 24px auto;
      border-bottom: 1px solid var(--border);
      padding-bottom: 16px;
    }
    h1 {
      color: var(--heading);
      margin: 0 0 8px 0;
      font-size: 26px;
    }
    p {
      margin: 0;
      color: #8b949e;
      font-size: 14px;
    }
    .container {
      max-width: 1200px;
      margin: 0 auto;
      display: grid;
      grid-template-columns: 1fr;
      gap: 24px;
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 20px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    .card-title {
      font-size: 18px;
      font-weight: 600;
      color: var(--heading);
      margin-bottom: 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .plot-box {
      width: 100%%;
      height: 520px;
    }
    table {
      width: 100%%;
      border-collapse: collapse;
      font-size: 13px;
      margin-top: 12px;
    }
    th, td {
      padding: 10px 12px;
      text-align: left;
      border-bottom: 1px solid var(--border);
    }
    th {
      background: #21262d;
      color: var(--heading);
      font-weight: 600;
    }
    tr:hover {
      background: rgba(255,255,255,0.02);
    }
    .badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 12px;
      font-size: 12px;
      font-weight: 600;
      background: rgba(88, 166, 255, 0.15);
      color: var(--accent);
    }
  </style>
</head>
<body>
  <div class="header">
    <h1>⏱️ atimer: Parameter Exploration Dashboard</h1>
    <p>Visualizing the impact of Heap Sharding and Worker Thread Count on Max Drift &amp; Latency</p>
  </div>

  <div class="container">
    <div class="card">
      <div class="card-title">
        <span>3D Surface Plot: Heaps × Workers vs Max Drift (ms)</span>
        <span class="badge">Interactive 3D (Click &amp; Drag to Rotate)</span>
      </div>
      <div id="surfacePlot" class="plot-box"></div>
    </div>

    <div class="card">
      <div class="card-title">
        <span>2D Heatmap Matrix: Max Drift (ms)</span>
        <span class="badge">Hover for Exact Milliseconds</span>
      </div>
      <div id="heatmapPlot" class="plot-box"></div>
    </div>

    <div class="card">
      <div class="card-title">
        <span>Comparative Drift Curves by Heap Count</span>
        <span class="badge">X: Workers/Heap | Y: Max Drift (ms)</span>
      </div>
      <div id="linePlot" class="plot-box"></div>
    </div>

    <div class="card">
      <div class="card-title">
        <span>Data Summary Matrix</span>
      </div>
      <div style="overflow-x:auto;">
        <table id="dataTable">
          <thead>
            <tr>
              <th>Heaps</th>
              <th>Workers / Heap</th>
              <th>Total Workers</th>
              <th>Max Drift</th>
              <th>P99 Drift</th>
              <th>P95 Drift</th>
              <th>P50 Drift</th>
              <th>Mean Drift</th>
              <th>Throughput</th>
              <th>Delivery Rate</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    const data = %s;

    const heaps = data.heaps_list.map(h => 'Heaps=' + h);
    const workers = data.workers_list.map(w => 'W=' + w);
    const zMatrix = data.matrix_ms;

    // 1. 3D Surface Plot
    const surfaceTrace = {
      z: zMatrix,
      x: data.workers_list,
      y: data.heaps_list,
      type: 'surface',
      colorscale: 'Viridis',
      colorbar: { title: 'Max Drift (ms)', tickfont: { color: '#c9d1d9' } }
    };
    const surfaceLayout = {
      paper_bgcolor: '#161b22',
      plot_bgcolor: '#161b22',
      font: { color: '#c9d1d9' },
      scene: {
        xaxis: { title: 'Workers / Heap', gridcolor: '#30363d', zerolinecolor: '#30363d' },
        yaxis: { title: 'Heaps', gridcolor: '#30363d', zerolinecolor: '#30363d' },
        zaxis: { title: 'Max Drift (ms)', gridcolor: '#30363d', zerolinecolor: '#30363d' }
      },
      margin: { l: 0, r: 0, b: 0, t: 0 }
    };
    Plotly.newPlot('surfacePlot', [surfaceTrace], surfaceLayout, { responsive: true });

    // 2. 2D Heatmap Plot
    const heatmapTrace = {
      z: zMatrix,
      x: data.workers_list.map(w => w + ' workers'),
      y: data.heaps_list.map(h => h + ' heaps'),
      type: 'heatmap',
      colorscale: 'Portland',
      hoverongaps: false,
      colorbar: { title: 'Max Drift (ms)', tickfont: { color: '#c9d1d9' } }
    };
    const heatmapLayout = {
      paper_bgcolor: '#161b22',
      plot_bgcolor: '#161b22',
      font: { color: '#c9d1d9' },
      xaxis: { title: 'Notification Workers per Heap', gridcolor: '#30363d' },
      yaxis: { title: 'Timer Heap Count', gridcolor: '#30363d' },
      margin: { l: 80, r: 40, b: 60, t: 40 }
    };
    Plotly.newPlot('heatmapPlot', [heatmapTrace], heatmapLayout, { responsive: true });

    // 3. Multi-line Curves
    const lineTraces = data.heaps_list.map((h, hIdx) => {
      return {
        x: data.workers_list,
        y: data.matrix_ms[hIdx],
        mode: 'lines+markers',
        name: h + ' Heaps',
        marker: { size: 8 }
      };
    });
    const lineLayout = {
      paper_bgcolor: '#161b22',
      plot_bgcolor: '#161b22',
      font: { color: '#c9d1d9' },
      xaxis: { title: 'Workers per Heap', gridcolor: '#30363d' },
      yaxis: { title: 'Max Drift (ms)', gridcolor: '#30363d' },
      margin: { l: 60, r: 40, b: 60, t: 40 }
    };
    Plotly.newPlot('linePlot', lineTraces, lineLayout, { responsive: true });

    // 4. Fill Data Table
    const tbody = document.querySelector('#dataTable tbody');
    data.points.forEach(p => {
      const tr = document.createElement('tr');
      const maxMs = (p.max_drift / 1000000).toFixed(2);
      const p99Ms = (p.p99_drift / 1000000).toFixed(2);
      const p95Ms = (p.p95_drift / 1000000).toFixed(2);
      const p50Ms = (p.p50_drift / 1000000).toFixed(2);
      const meanMs = (p.mean_drift / 1000000).toFixed(2);
      tr.innerHTML = ` + "`" + `
        <td><strong>${p.heaps}</strong></td>
        <td>${p.workers}</td>
        <td>${p.total_workers}</td>
        <td><strong style="color:#58a6ff">${maxMs} ms</strong></td>
        <td>${p99Ms} ms</td>
        <td>${p95Ms} ms</td>
        <td>${p50Ms} ms</td>
        <td>${meanMs} ms</td>
        <td>${p.throughput.toFixed(2)} req/s</td>
        <td>${p.delivery_rate.toFixed(1)}%%</td>
      ` + "`" + `;
      tbody.appendChild(tr);
    });
  </script>
</body>
</html>`, string(dataJSON))

	if dir := filepath.Dir(filename); dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0755)
	}
	return os.WriteFile(filename, []byte(html), 0644)
}
