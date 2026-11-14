"""Draw the microbench bar chart from committed data."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "docs" / "_data" / "microbench.json"
OUT = ROOT / "docs" / "assets" / "microbench.svg"
PALETTE = ["#4c78a8", "#f58518", "#54a24b"]


def render(rows):
    width, height = 640, 360
    top, bottom, left, right = 48, 64, 64, 24
    top_val = max(r["value"] for r in rows) * 1.2
    plot_w = width - left - right
    plot_h = height - top - bottom
    n = len(rows)
    slot = plot_w / n
    bar_w = min(96.0, slot * 0.5)
    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}" role="img">'
    )
    parts.append(
        "<title>In-process microbenchmark rates</title>"
    )
    parts.append(
        f'<text x="{width / 2}" y="24" text-anchor="middle" '
        f'fill="currentColor" font-size="16">'
        f"In-process microbenchmark rates</text>"
    )
    parts.append(
        f'<line x1="{left}" y1="{top}" x2="{left}" '
        f'y2="{height - bottom}" stroke="currentColor"/>'
    )
    parts.append(
        f'<line x1="{left}" y1="{height - bottom}" '
        f'x2="{width - right}" y2="{height - bottom}" '
        f'stroke="currentColor"/>'
    )
    parts.append(
        f'<text x="16" y="{height / 2}" text-anchor="middle" '
        f'fill="currentColor" font-size="12" '
        f'transform="rotate(-90 16 {height / 2})">'
        f"rate (million ops/s)</text>"
    )
    parts.append(
        f'<text x="{left + plot_w / 2}" y="{height - 12}" '
        f'text-anchor="middle" fill="currentColor" '
        f'font-size="12">benchmark</text>'
    )
    for i, row in enumerate(rows):
        cx = left + slot * i + slot / 2
        bar_h = row["value"] / top_val * plot_h
        x = cx - bar_w / 2
        y = height - bottom - bar_h
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
            f'height="{bar_h:.1f}" fill="{PALETTE[i % len(PALETTE)]}">'
            f"<title>{row['name']}: {row['value']} {row['unit']}</title>"
            f"</rect>"
        )
        parts.append(
            f'<text x="{cx:.1f}" y="{y - 6:.1f}" text-anchor="middle" '
            f'fill="currentColor" font-size="12">{row["value"]}</text>'
        )
        parts.append(
            f'<text x="{cx:.1f}" y="{height - bottom + 20:.1f}" '
            f'text-anchor="middle" fill="currentColor" '
            f'font-size="12">{row["name"]}</text>'
        )
    return "\n".join(parts) + "\n</svg>\n"


def main(argv):
    rows = json.loads(DATA.read_text())["results"]
    fresh = render(rows)
    if "--check" in argv:
        saved = OUT.read_text()
        if saved != fresh:
            print("chart is stale: rerun docs/_tools/render_charts.py")
            return 1
        print("chart is fresh")
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(fresh)
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
