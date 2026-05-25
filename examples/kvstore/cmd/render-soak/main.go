// Copyright 2026 the comlink authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// render-soak turns a comlink-soak events JSONL file into a
// single self-contained HTML report. Chart.js is loaded from
// a CDN by default (--inline-chartjs to bundle it for offline
// viewing — adds ~200 KiB).
//
// Usage:
//
//	render-soak -in soak-events.jsonl -out report.html
//	open report.html
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"os"
	"sort"
	"strings"
	"time"
)

var (
	flagIn      = flag.String("in", "", "input events JSONL file (required)")
	flagOut     = flag.String("out", "soak-report.html", "output HTML path")
	flagTitle   = flag.String("title", "comlink soak run", "report title")
	flagSubtitle = flag.String("subtitle", "", "optional subtitle / context line")
)

func main() {
	flag.Parse()
	if *flagIn == "" {
		fmt.Fprintln(os.Stderr, "render-soak: -in is required")
		os.Exit(2)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "render-soak:", err)
		os.Exit(1)
	}
}

// event is the parsed JSONL line. Both 'tick' and 'annotation'
// kinds share this shape; unused fields stay zero.
type event struct {
	TS                string  `json:"ts"`
	Kind              string  `json:"kind"`
	Tag               string  `json:"tag,omitempty"`
	Text              string  `json:"text,omitempty"`
	WritesOK          uint64  `json:"writes_ok"`
	WritesFail        uint64  `json:"writes_fail"`
	WritesFailDelta   uint64  `json:"writes_fail_delta"`
	ReadsOK           uint64  `json:"reads_ok"`
	ReadsMiss         uint64  `json:"reads_miss"`
	ReadsFail         uint64  `json:"reads_fail"`
	Restarts          uint64  `json:"restarts"`
	BytesWrite        uint64  `json:"bytes_write"`
	BytesRead         uint64  `json:"bytes_read"`
	WritesPerSec      float64 `json:"writes_per_sec"`
	ReadsPerSec       float64 `json:"reads_per_sec"`
	BytesWriteMiBs    float64 `json:"bytes_write_mib_s"`
	BytesReadMiBs     float64 `json:"bytes_read_mib_s"`
}

func run() error {
	events, err := loadEvents(*flagIn)
	if err != nil {
		return err
	}

	ticks, annotations := splitEvents(events)
	if len(ticks) == 0 {
		return fmt.Errorf("no 'tick' events found in %s", *flagIn)
	}
	sort.Slice(ticks, func(i, j int) bool { return ticks[i].t.Before(ticks[j].t) })
	sort.Slice(annotations, func(i, j int) bool { return annotations[i].t.Before(annotations[j].t) })

	chartData := buildChartData(ticks, annotations)

	tmpl, err := template.New("report").Funcs(template.FuncMap{
		"json": jsonEncode,
	}).Parse(reportTemplate)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	outF, err := os.Create(*flagOut)
	if err != nil {
		return err
	}
	defer outF.Close()

	type renderContext struct {
		Title       string
		Subtitle    string
		GeneratedAt string
		Data        chartPayload
	}
	if err := tmpl.Execute(outF, renderContext{
		Title:       *flagTitle,
		Subtitle:    *flagSubtitle,
		GeneratedAt: time.Now().Format(time.RFC1123),
		Data:        chartData,
	}); err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	fmt.Printf("wrote %s (%d ticks, %d annotations)\n", *flagOut, len(ticks), len(annotations))
	return nil
}

type tickEvent struct {
	event
	t time.Time
}

type annotEvent struct {
	event
	t time.Time
}

func loadEvents(path string) ([]event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []event
	sc := bufio.NewScanner(f)
	// JSONL lines can be large with future schemas; bump the buffer.
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var ev event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			fmt.Fprintf(os.Stderr, "warn: skipping line %d: %v\n", lineNo, err)
			continue
		}
		out = append(out, ev)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func splitEvents(in []event) ([]tickEvent, []annotEvent) {
	var ticks []tickEvent
	var annots []annotEvent
	for _, e := range in {
		t, err := time.Parse(time.RFC3339Nano, e.TS)
		if err != nil {
			// Try the seconds-precision fallback.
			t, err = time.Parse(time.RFC3339, e.TS)
			if err != nil {
				continue
			}
		}
		switch e.Kind {
		case "tick":
			ticks = append(ticks, tickEvent{event: e, t: t})
		case "annotation":
			annots = append(annots, annotEvent{event: e, t: t})
		}
	}
	return ticks, annots
}

// chartPayload is what we hand to the HTML template (and to JS).
type chartPayload struct {
	// Per-tick labels (seconds-from-run-start, e.g. "00:05").
	Labels []string `json:"labels"`
	// Throughput rates per tick.
	WritesPerSec []float64 `json:"writes_per_sec"`
	ReadsPerSec  []float64 `json:"reads_per_sec"`
	BytesWMiBs   []float64 `json:"bytes_w_mib_s"`
	BytesRMiBs   []float64 `json:"bytes_r_mib_s"`
	// Cumulative totals.
	CumWritesOK  []uint64 `json:"cum_writes_ok"`
	CumWritesFail []uint64 `json:"cum_writes_fail"`
	CumReadsOK   []uint64 `json:"cum_reads_ok"`
	CumBytesW    []uint64 `json:"cum_bytes_w"`
	CumBytesR    []uint64 `json:"cum_bytes_r"`
	// Per-tick incremental write-success ratio
	// (writes_ok_delta / (writes_ok_delta + writes_fail_delta)).
	SuccessRatePct []float64 `json:"success_rate_pct"`
	// Vertical annotation markers.
	Annotations []annotationMarker `json:"annotations"`
	// Final summary numbers for the table.
	Summary summary `json:"-"`
}

type annotationMarker struct {
	Label string `json:"label"`
	Text  string `json:"text"`
	// Index into Labels[] this annotation lines up with.
	Index int `json:"index"`
}

type summary struct {
	StartTS         string
	EndTS           string
	Duration        string
	TotalWritesOK   uint64
	TotalWritesFail uint64
	TotalReadsOK    uint64
	TotalReadsMiss  uint64
	TotalReadsFail  uint64
	BytesWritten    string
	BytesRead       string
	PeakWriteMiBs   float64
	PeakReadMiBs    float64
	WriteSuccess    string
}

func buildChartData(ticks []tickEvent, annots []annotEvent) chartPayload {
	out := chartPayload{}
	if len(ticks) == 0 {
		return out
	}
	t0 := ticks[0].t
	for i, tk := range ticks {
		secs := int(tk.t.Sub(t0).Seconds())
		out.Labels = append(out.Labels, fmt.Sprintf("%02d:%02d", secs/60, secs%60))
		out.WritesPerSec = append(out.WritesPerSec, tk.WritesPerSec)
		out.ReadsPerSec = append(out.ReadsPerSec, tk.ReadsPerSec)
		out.BytesWMiBs = append(out.BytesWMiBs, tk.BytesWriteMiBs)
		out.BytesRMiBs = append(out.BytesRMiBs, tk.BytesReadMiBs)
		out.CumWritesOK = append(out.CumWritesOK, tk.WritesOK)
		out.CumWritesFail = append(out.CumWritesFail, tk.WritesFail)
		out.CumReadsOK = append(out.CumReadsOK, tk.ReadsOK)
		out.CumBytesW = append(out.CumBytesW, tk.BytesWrite)
		out.CumBytesR = append(out.CumBytesR, tk.BytesRead)

		// Success ratio for this interval. Use the delta in
		// writes_fail and the delta in writes_ok between ticks.
		var prevOK, prevFail uint64
		if i > 0 {
			prevOK = ticks[i-1].WritesOK
			prevFail = ticks[i-1].WritesFail
		}
		dOK := tk.WritesOK - prevOK
		dFail := tk.WritesFail - prevFail
		ratio := 100.0
		if dOK+dFail > 0 {
			ratio = float64(dOK) * 100.0 / float64(dOK+dFail)
		}
		out.SuccessRatePct = append(out.SuccessRatePct, ratio)
	}
	// Annotations — find the tick they fall closest to.
	for _, a := range annots {
		// Skip annotations outside the tick window.
		if a.t.Before(ticks[0].t) || a.t.After(ticks[len(ticks)-1].t.Add(5*time.Second)) {
			continue
		}
		idx := 0
		for i, tk := range ticks {
			if tk.t.After(a.t) {
				break
			}
			idx = i
		}
		out.Annotations = append(out.Annotations, annotationMarker{
			Label: a.Tag,
			Text:  a.Text,
			Index: idx,
		})
	}

	// Summary.
	last := ticks[len(ticks)-1]
	out.Summary = summary{
		StartTS:         ticks[0].t.Format(time.RFC1123),
		EndTS:           last.t.Format(time.RFC1123),
		Duration:        last.t.Sub(ticks[0].t).Round(time.Second).String(),
		TotalWritesOK:   last.WritesOK,
		TotalWritesFail: last.WritesFail,
		TotalReadsOK:    last.ReadsOK,
		TotalReadsMiss:  last.ReadsMiss,
		TotalReadsFail:  last.ReadsFail,
		BytesWritten:    humanBytes(last.BytesWrite),
		BytesRead:       humanBytes(last.BytesRead),
	}
	for _, v := range out.BytesWMiBs {
		if v > out.Summary.PeakWriteMiBs {
			out.Summary.PeakWriteMiBs = v
		}
	}
	for _, v := range out.BytesRMiBs {
		if v > out.Summary.PeakReadMiBs {
			out.Summary.PeakReadMiBs = v
		}
	}
	if last.WritesOK+last.WritesFail > 0 {
		pct := float64(last.WritesOK) * 100.0 / float64(last.WritesOK+last.WritesFail)
		out.Summary.WriteSuccess = fmt.Sprintf("%.1f%%", pct)
	} else {
		out.Summary.WriteSuccess = "n/a"
	}
	return out
}

func humanBytes(b uint64) string {
	const (
		ki = 1024
		mi = 1024 * ki
		gi = 1024 * mi
	)
	switch {
	case b >= gi:
		return fmt.Sprintf("%.2f GiB", float64(b)/float64(gi))
	case b >= mi:
		return fmt.Sprintf("%.2f MiB", float64(b)/float64(mi))
	case b >= ki:
		return fmt.Sprintf("%.2f KiB", float64(b)/float64(ki))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// jsonEncode is a template helper that emits a JSON-encoded
// value safe to embed in a <script> tag.
func jsonEncode(v any) (template.JS, error) {
	bs, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return template.JS(bs), nil
}

// reportTemplate is the single-page HTML emitted by the
// renderer. Chart.js v4 is loaded from a CDN — for offline
// viewing the inline option could swap in a bundled copy
// later (not implemented today).
const reportTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<title>{{.Title}}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.6/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0f1419;
    --surface: #1a1f2e;
    --surface-2: #232a3d;
    --text: #e6e8eb;
    --text-dim: #9aa3b2;
    --accent: #6cc1ff;
    --good: #6dd17e;
    --bad: #ff7575;
    --warn: #ffce6d;
    --border: #2a3145;
  }
  * { box-sizing: border-box; }
  html, body {
    margin: 0; padding: 0;
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", Roboto, sans-serif;
    line-height: 1.5;
  }
  .wrap {
    max-width: 1180px;
    margin: 0 auto;
    padding: 32px 24px 64px;
  }
  header h1 {
    margin: 0 0 4px;
    font-size: 28px;
    font-weight: 600;
    letter-spacing: -0.01em;
  }
  header .subtitle {
    color: var(--text-dim);
    font-size: 14px;
    margin-bottom: 24px;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 18px 20px;
  }
  .card.kpi {
    display: flex; flex-direction: column; gap: 4px;
  }
  .kpi .label {
    color: var(--text-dim);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .kpi .value {
    font-size: 24px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
  }
  .kpi .value.good { color: var(--good); }
  .kpi .value.bad { color: var(--bad); }
  .kpi .value.warn { color: var(--warn); }
  .card.chart {
    padding: 16px 18px 12px;
  }
  .card.chart h2 {
    margin: 0 0 12px;
    font-size: 15px;
    font-weight: 600;
    color: var(--text);
  }
  .card.chart h2 .hint {
    font-weight: 400;
    color: var(--text-dim);
    font-size: 12px;
    margin-left: 8px;
  }
  .chart-wrap {
    position: relative;
    height: 240px;
  }
  .chart-wrap.tall {
    height: 320px;
  }
  table.summary {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
  }
  table.summary td {
    padding: 8px 12px;
    border-top: 1px solid var(--border);
  }
  table.summary td:first-child {
    color: var(--text-dim);
    width: 50%;
  }
  table.summary td:last-child {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }
  .narrative {
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 3px solid var(--accent);
    border-radius: 8px;
    padding: 16px 22px;
    margin: 24px 0;
  }
  .narrative h2 {
    margin: 0 0 8px;
    font-size: 16px;
  }
  .narrative p {
    margin: 0 0 8px;
    color: var(--text-dim);
    font-size: 14px;
  }
  .narrative p:last-child { margin-bottom: 0; }
  .annotations-list {
    list-style: none;
    padding: 0; margin: 0;
  }
  .annotations-list li {
    padding: 8px 0;
    border-top: 1px solid var(--border);
    font-size: 13px;
  }
  .annotations-list li:first-child { border-top: none; }
  .annotations-list .tag {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 12px;
    background: var(--surface-2);
    color: var(--accent);
    font-size: 11px;
    font-weight: 600;
    margin-right: 8px;
    font-variant-numeric: tabular-nums;
  }
  footer {
    margin-top: 32px;
    color: var(--text-dim);
    font-size: 12px;
    text-align: center;
  }
  .two-col {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
  }
  @media (max-width: 800px) {
    .two-col { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>{{.Title}}</h1>
    {{if .Subtitle}}<div class="subtitle">{{.Subtitle}}</div>{{end}}
    <div class="subtitle">Generated {{.GeneratedAt}} · Run window {{.Data.Summary.StartTS}} → {{.Data.Summary.EndTS}} ({{.Data.Summary.Duration}})</div>
  </header>

  <section class="grid">
    <div class="card kpi"><span class="label">Bytes written</span><span class="value good">{{.Data.Summary.BytesWritten}}</span></div>
    <div class="card kpi"><span class="label">Bytes read</span><span class="value">{{.Data.Summary.BytesRead}}</span></div>
    <div class="card kpi"><span class="label">Write success</span><span class="value">{{.Data.Summary.WriteSuccess}}</span></div>
    <div class="card kpi"><span class="label">Peak write throughput</span><span class="value">{{printf "%.2f" .Data.Summary.PeakWriteMiBs}} MiB/s</span></div>
    <div class="card kpi"><span class="label">Peak read throughput</span><span class="value">{{printf "%.2f" .Data.Summary.PeakReadMiBs}} MiB/s</span></div>
  </section>

  <div class="card chart" style="margin-bottom: 16px;">
    <h2>Throughput over time <span class="hint">MiB/sec, write + read</span></h2>
    <div class="chart-wrap tall"><canvas id="chartThroughput"></canvas></div>
  </div>

  <div class="two-col">
    <div class="card chart">
      <h2>Operations per second <span class="hint">writes vs. reads</span></h2>
      <div class="chart-wrap"><canvas id="chartOps"></canvas></div>
    </div>
    <div class="card chart">
      <h2>Write success rate <span class="hint">% of attempts in interval</span></h2>
      <div class="chart-wrap"><canvas id="chartSuccess"></canvas></div>
    </div>
  </div>

  <div class="card chart" style="margin-top: 16px;">
    <h2>Cumulative bytes <span class="hint">total written + read since start</span></h2>
    <div class="chart-wrap"><canvas id="chartCumBytes"></canvas></div>
  </div>

  <div class="two-col" style="margin-top: 24px;">
    <div class="card">
      <h2 style="margin-top: 0; font-size: 16px;">Run summary</h2>
      <table class="summary">
        <tr><td>Total writes OK</td><td>{{.Data.Summary.TotalWritesOK}}</td></tr>
        <tr><td>Total writes FAIL</td><td>{{.Data.Summary.TotalWritesFail}}</td></tr>
        <tr><td>Total reads OK</td><td>{{.Data.Summary.TotalReadsOK}}</td></tr>
        <tr><td>Total reads MISS (404)</td><td>{{.Data.Summary.TotalReadsMiss}}</td></tr>
        <tr><td>Total reads FAIL</td><td>{{.Data.Summary.TotalReadsFail}}</td></tr>
        <tr><td>Bytes written</td><td>{{.Data.Summary.BytesWritten}}</td></tr>
        <tr><td>Bytes read</td><td>{{.Data.Summary.BytesRead}}</td></tr>
        <tr><td>Write success rate</td><td>{{.Data.Summary.WriteSuccess}}</td></tr>
      </table>
    </div>
    <div class="card">
      <h2 style="margin-top: 0; font-size: 16px;">Timeline annotations</h2>
      {{if .Data.Annotations}}
      <ul class="annotations-list">
        {{range .Data.Annotations}}
        <li><span class="tag">{{.Label}}</span>{{.Text}}</li>
        {{end}}
      </ul>
      {{else}}
      <p style="color: var(--text-dim); margin: 0;">No annotations recorded for this run.</p>
      {{end}}
    </div>
  </div>

  <footer>
    Generated by <code>comlink/examples/kvstore/cmd/render-soak</code>.
    Charts powered by Chart.js. Source JSONL: <code>{{.Data.Summary.Duration}}</code> window, {{len .Data.Labels}} ticks.
  </footer>
</div>

<script>
const labels = {{json .Data.Labels}};
const writesPerSec = {{json .Data.WritesPerSec}};
const readsPerSec  = {{json .Data.ReadsPerSec}};
const bytesWMiBs   = {{json .Data.BytesWMiBs}};
const bytesRMiBs   = {{json .Data.BytesRMiBs}};
const cumBytesW    = {{json .Data.CumBytesW}};
const cumBytesR    = {{json .Data.CumBytesR}};
const successPct   = {{json .Data.SuccessRatePct}};
const annotations  = {{json .Data.Annotations}};

// Convert annotations into Chart.js annotation-plugin-free
// custom verticals — we draw them via afterDraw on each chart.
const annotationPlugin = {
  id: 'annotationLines',
  afterDraw(chart) {
    const {ctx, chartArea: {top, bottom}, scales: {x}} = chart;
    if (!x || !annotations || annotations.length === 0) return;
    ctx.save();
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 4]);
    annotations.forEach((a, i) => {
      const idx = Math.min(a.index, labels.length - 1);
      const xPos = x.getPixelForValue(idx);
      const color = a.label.includes('migrate') ? '#ffce6d' : '#6cc1ff';
      ctx.strokeStyle = color;
      ctx.beginPath();
      ctx.moveTo(xPos, top);
      ctx.lineTo(xPos, bottom);
      ctx.stroke();
      // Label only on the throughput chart (i.e. when chart has
      // the throughput-tag canvas id).
      if (chart.canvas.id === 'chartThroughput') {
        ctx.setLineDash([]);
        ctx.fillStyle = color;
        ctx.font = 'bold 11px sans-serif';
        const labelText = a.label;
        const textW = ctx.measureText(labelText).width;
        const boxX = xPos + 4;
        const boxY = top + 8 + (i * 18);
        ctx.fillRect(boxX, boxY, textW + 8, 16);
        ctx.fillStyle = '#0f1419';
        ctx.fillText(labelText, boxX + 4, boxY + 12);
      }
    });
    ctx.restore();
  }
};

const baseOpts = {
  responsive: true,
  maintainAspectRatio: false,
  interaction: { mode: 'index', intersect: false },
  scales: {
    x: {
      ticks: { color: '#9aa3b2', maxRotation: 0, autoSkipPadding: 24 },
      grid: { color: 'rgba(154,163,178,0.08)' },
    },
    y: {
      ticks: { color: '#9aa3b2' },
      grid: { color: 'rgba(154,163,178,0.08)' },
      beginAtZero: true,
    },
  },
  plugins: {
    legend: { labels: { color: '#e6e8eb', boxWidth: 14 } },
    tooltip: {
      backgroundColor: '#0f1419',
      borderColor: '#2a3145',
      borderWidth: 1,
      titleColor: '#e6e8eb',
      bodyColor: '#e6e8eb',
    },
  },
};

new Chart(document.getElementById('chartThroughput'), {
  type: 'line',
  data: {
    labels,
    datasets: [
      { label: 'Write MiB/s', data: bytesWMiBs, borderColor: '#6dd17e',
        backgroundColor: 'rgba(109,209,126,0.15)', fill: true, tension: 0.25, pointRadius: 0 },
      { label: 'Read MiB/s',  data: bytesRMiBs, borderColor: '#6cc1ff',
        backgroundColor: 'rgba(108,193,255,0.10)', fill: true, tension: 0.25, pointRadius: 0 },
    ],
  },
  options: baseOpts,
  plugins: [annotationPlugin],
});

new Chart(document.getElementById('chartOps'), {
  type: 'line',
  data: {
    labels,
    datasets: [
      { label: 'Writes/s', data: writesPerSec, borderColor: '#6dd17e', tension: 0.2, pointRadius: 0, fill: false },
      { label: 'Reads/s',  data: readsPerSec,  borderColor: '#6cc1ff', tension: 0.2, pointRadius: 0, fill: false, yAxisID: 'y2' },
    ],
  },
  options: {
    ...baseOpts,
    scales: {
      x: baseOpts.scales.x,
      y:  { ...baseOpts.scales.y, position: 'left', title: { display: true, text: 'writes/s', color: '#6dd17e' } },
      y2: { ...baseOpts.scales.y, position: 'right', title: { display: true, text: 'reads/s', color: '#6cc1ff' }, grid: { drawOnChartArea: false } },
    },
  },
  plugins: [annotationPlugin],
});

new Chart(document.getElementById('chartSuccess'), {
  type: 'line',
  data: {
    labels,
    datasets: [
      { label: '% writes succeeded', data: successPct, borderColor: '#ffce6d',
        backgroundColor: 'rgba(255,206,109,0.15)', fill: true, tension: 0.2, pointRadius: 0 },
    ],
  },
  options: {
    ...baseOpts,
    scales: {
      x: baseOpts.scales.x,
      y: { ...baseOpts.scales.y, suggestedMin: 0, suggestedMax: 100, ticks: { ...baseOpts.scales.y.ticks, callback: v => v + '%' } },
    },
  },
  plugins: [annotationPlugin],
});

new Chart(document.getElementById('chartCumBytes'), {
  type: 'line',
  data: {
    labels,
    datasets: [
      { label: 'Cumulative MiB written',
        data: cumBytesW.map(v => v / (1024*1024)),
        borderColor: '#6dd17e', backgroundColor: 'rgba(109,209,126,0.15)',
        fill: true, tension: 0.1, pointRadius: 0 },
      { label: 'Cumulative MiB read',
        data: cumBytesR.map(v => v / (1024*1024)),
        borderColor: '#6cc1ff', backgroundColor: 'rgba(108,193,255,0.10)',
        fill: true, tension: 0.1, pointRadius: 0,
        yAxisID: 'y2' },
    ],
  },
  options: {
    ...baseOpts,
    scales: {
      x: baseOpts.scales.x,
      y:  { ...baseOpts.scales.y, position: 'left', title: { display: true, text: 'MiB written', color: '#6dd17e' } },
      y2: { ...baseOpts.scales.y, position: 'right', title: { display: true, text: 'MiB read', color: '#6cc1ff' }, grid: { drawOnChartArea: false } },
    },
  },
  plugins: [annotationPlugin],
});
</script>
</body>
</html>
`
