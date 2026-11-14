#!/usr/bin/env python3
"""Make one HTML page from the JSON lines of a benchmark run.

It reads a run directory with <scenario>.jsonl files, optional
<scenario>.summary.json files and env.json. It writes one page with
inline SVG charts. The page uses no external network.

Usage:
  report.py <run-dir> <output.html>
  report.py <single.jsonl> <output.html>

Each scenario shows the test environment, throughput against CPU,
p50 and p99 latency against throughput, memory, and the recommended
load. Text uses short sentences. It shows measured values only.
"""
import html
import json
import os
import sys

COLORS = ["#1f6feb", "#cf222e", "#1a7f37", "#8250df", "#9a6700"]


def esc(value):
    return html.escape("" if value is None else str(value), quote=True)


def fmt(value, digits=1):
    if value is None:
        return "not measured"
    if isinstance(value, float):
        return str(round(value, digits))
    return str(value)


def read_rows(path):
    """Read JSON lines from one file. Skip blank lines."""
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def load_run(source):
    """Load scenarios from a directory or one file. Return dicts."""
    scenarios = {}
    env = {}

    def add_rows(rows, fallback):
        groups = {}
        for r in rows:
            groups.setdefault(r.get("scenario", fallback), []).append(r)
        for key, group in groups.items():
            scenarios.setdefault(key, []).extend(group)

    if os.path.isdir(source):
        try:
            with open(os.path.join(source, "env.json")) as f:
                env = json.load(f)
        except OSError:
            env = {}
        if not env:
            try:
                with open(os.path.join(source, "env.txt")) as f:
                    env = {"raw": f.read()[:2000]}
            except OSError:
                env = {}
        for name in sorted(os.listdir(source)):
            if name.endswith(".jsonl"):
                add_rows(read_rows(os.path.join(source, name)), name[:-6])
    else:
        add_rows(read_rows(source), os.path.basename(source))
    summaries = {}
    if os.path.isdir(source):
        for name in sorted(os.listdir(source)):
            if name.endswith(".summary.json"):
                with open(os.path.join(source, name)) as f:
                    summaries[name[:-13]] = json.load(f)
    return scenarios, summaries, env


def pick_recommended(rows):
    """Pick the step with CPU nearest 75 percent. Skip bad steps."""
    if not rows:
        return None
    ok = [r for r in rows if not r.get("generator_limited")
          and (r.get("delivery_ratio") or 0.0) >= 0.9]
    pool = ok or rows
    return min(pool, key=lambda r: abs((r.get("cpu_pct_of_4") or 0.0) - 75.0))


def probe_p50_ms(row):
    v = row.get("probe_p50_us")
    return v / 1000.0 if isinstance(v, (int, float)) else None


def probe_p99_ms(row):
    v = row.get("probe_p99_us")
    return v / 1000.0 if isinstance(v, (int, float)) else None


def svg_chart(title, x_label, y_label, series):
    """Draw a line chart as inline SVG. Series holds (name, points)."""
    w, h = 560, 300
    ml, mr, mt, mb = 56, 12, 28, 40
    xs = [x for _, pts in series for x, _ in pts if x is not None]
    ys = [y for _, pts in series for _, y in pts if y is not None]
    if not xs or not ys:
        return "<p>No data for %s.</p>" % esc(title)
    x0, x1 = min(xs), max(xs)
    y0, y1 = 0.0, max(ys)
    if x1 == x0:
        x1 = x0 + 1.0
    if y1 == 0:
        y1 = 1.0
    y1 = y1 * 1.1

    def px(x):
        return ml + (x - x0) / (x1 - x0) * (w - ml - mr)

    def py(y):
        return h - mb - (y - y0) / (y1 - y0) * (h - mt - mb)

    parts = ['<svg viewBox="0 0 %d %d" role="img" aria-label="%s">' % (w, h, esc(title))]
    parts.append('<text x="%d" y="16" font-size="13">%s</text>' % (ml, esc(title)))
    for i in range(5):
        yv = y0 + (y1 - y0) * i / 4.0
        parts.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#ddd"/>' % (ml, py(yv), w - mr, py(yv)))
        parts.append('<text x="%d" y="%.1f" font-size="10" text-anchor="end">%.1f</text>' % (ml - 4, py(yv) + 3, yv))
    parts.append('<text x="%d" y="%d" font-size="10">%s</text>' % ((w - mr + ml) // 2, h - 6, esc(x_label)))
    for idx, (name, pts) in enumerate(series):
        col = COLORS[idx % len(COLORS)]
        pts = sorted([(x, y) for x, y in pts if x is not None and y is not None])
        line = " ".join("%.1f,%.1f" % (px(x), py(y)) for x, y in pts)
        parts.append('<polyline fill="none" stroke="%s" stroke-width="2" points="%s"/>' % (col, line))
        for x, y in pts:
            parts.append('<circle cx="%.1f" cy="%.1f" r="3" fill="%s"/>' % (px(x), py(y), col))
        parts.append('<text x="%d" y="%d" font-size="11" fill="%s">%s</text>'
                     % (ml + idx * 140, h - 22, col, esc(name)))
    parts.append('<text x="8" y="%d" font-size="10">%s</text>' % ((h - mb + mt) // 2, esc(y_label)))
    parts.append("</svg>")
    return "".join(parts)


def env_block(env):
    """Show the test environment. Show gaps plainly."""
    if not env:
        return "<p>Environment was not recorded.</p>"
    if "raw" in env and len(env) == 1:
        return "<pre>%s</pre>" % esc(env["raw"])
    rows = [
        ("CPU model", env.get("cpu_model", "not recorded")),
        ("CPU cores", env.get("cpu_cores", "not recorded")),
        ("Memory MiB", env.get("memory_mib", "not recorded")),
        ("Kernel", env.get("kernel", "not recorded")),
        ("Broker cores", env.get("broker_cores", "not recorded")),
        ("Broker memory", env.get("broker_memory", "not recorded")),
        ("Build commit", env.get("build_sha256", "not recorded")),
        ("Recorded at", env.get("recorded_at", "not recorded")),
    ]
    cells = "".join("<tr><th>%s</th><td>%s</td></tr>" % (esc(k), esc(v)) for k, v in rows)
    return "<table><tbody>%s</tbody></table>" % cells


def scenario_section(name, rows, summary):
    """Make one HTML section for one scenario."""
    parts = ["<section><h2>%s</h2>" % esc(name)]
    if summary and summary.get("desc"):
        parts.append("<p>%s.</p>" % esc(summary["desc"]))
    rec = pick_recommended(rows)
    if not rows:
        parts.append("<p>No steps ran for this scenario.</p></section>")
        return "".join(parts)
    offered = [(r.get("offered"), r.get("out_rate")) for r in rows]
    cpu = [(r.get("offered"), r.get("cpu_pct_of_4")) for r in rows]
    p50 = [(r.get("in_rate"), probe_p50_ms(r)) for r in rows]
    p99 = [(r.get("in_rate"), probe_p99_ms(r)) for r in rows]
    mem = [(r.get("offered"), r.get("mem_mib")) for r in rows]
    parts.append(svg_chart(name + ": throughput", "offered msg/s", "msg/s", [("out", offered)]))
    parts.append(svg_chart(name + ": CPU", "offered msg/s", "CPU percent of 4", [("cpu", cpu)]))
    parts.append(svg_chart(name + ": latency", "in msg/s", "latency ms", [("p50", p50), ("p99", p99)]))
    parts.append(svg_chart(name + ": memory", "offered msg/s", "memory MiB", [("mem", mem)]))
    if rec is None:
        parts.append("<p>No recommended load. No step ran.</p>")
    else:
        avg = rec.get("lat_gen_avg_ms")
        parts.append("<table><caption>Recommended load</caption><tbody>"
                     "<tr><th>Step</th><td>%s</td></tr>"
                     "<tr><th>In TPS</th><td>%s</td></tr>"
                     "<tr><th>Out TPS</th><td>%s</td></tr>"
                     "<tr><th>CPU percent</th><td>%s</td></tr>"
                     "<tr><th>Memory MiB</th><td>%s</td></tr>"
                     "<tr><th>Average latency ms</th><td>%s</td></tr>"
                     "<tr><th>P99 latency ms</th><td>%s</td></tr>"
                     "</tbody></table>"
                     % (esc(rec.get("step")), fmt(rec.get("in_rate"), 0), fmt(rec.get("out_rate"), 0),
                        fmt(rec.get("cpu_pct_of_4")), fmt(rec.get("mem_mib")),
                        fmt(avg), fmt(probe_p99_ms(rec))))
        limited = [r.get("step") for r in rows if r.get("generator_limited")]
        if limited:
            parts.append("<p>Generator limited these steps: %s. Do not use them.</p>" % esc(", ".join(limited)))
    parts.append("<table><caption>Steps</caption><thead><tr>"
                 "<th>Step</th><th>In</th><th>Out</th><th>CPU percent</th>"
                 "<th>Delivery</th><th>Limited</th></tr></thead><tbody>")
    for r in rows:
        parts.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                     % (esc(r.get("step")), fmt(r.get("in_rate"), 0), fmt(r.get("out_rate"), 0),
                        fmt(r.get("cpu_pct_of_4")), fmt(r.get("delivery_ratio"), 3),
                        esc(r.get("generator_limited"))))
    parts.append("</tbody></table></section>")
    return "".join(parts)


def make_page(scenarios, summaries, env):
    """Build the full HTML page. Keep it self contained."""
    parts = ["<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">"
             "<title>Benchmark report</title>"
             "<style>body{font-family:sans-serif;max-width:900px;margin:auto;padding:16px}"
             "table{border-collapse:collapse;margin:12px 0}th,td{border:1px solid #ccc;padding:4px 8px}"
             "svg{width:100%;height:auto;border:1px solid #eee;margin:12px 0}</style>"
             "</head><body><h1>Benchmark report</h1>"]
    parts.append("<section><h2>Test environment</h2>%s</section>" % env_block(env))
    for name in sorted(scenarios):
        parts.append(scenario_section(name, scenarios[name], summaries.get(name, {})))
    parts.append("<section><h2>Limits of the method</h2><ul>"
                 "<li>Each step runs once. Results vary between runs.</li>"
                 "<li>The generator shares the machine with the broker. Limited steps do not count.</li>"
                 "<li>The probe adds 1000 msg/s. The suite subtracts it.</li>"
                 "<li>Latency covers probe pairs only. It does not cover all traffic.</li>"
                 "<li>Memory comes from the broker cgroup. It includes all broker tasks.</li>"
                 "</ul></section>")
    parts.append("</body></html>")
    return "".join(parts)


def main(argv):
    if len(argv) != 3:
        print("Usage: report.py <run-dir|run.jsonl> <output.html>", file=sys.stderr)
        return 2
    source, dest = argv[1], argv[2]
    scenarios, summaries, env = load_run(source)
    page = make_page(scenarios, summaries, env)
    with open(dest, "w") as f:
        f.write(page)
    print("wrote %s" % dest)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
