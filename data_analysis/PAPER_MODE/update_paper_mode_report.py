from pathlib import Path
from string import Template

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
COMP_SUMMARY_CSV = BASE_DIR / "PAPER_MODE_comp_summary.csv"
CUM_PNL_CSV = BASE_DIR / "PAPER_MODE_cum_pnl.csv"
OVERALL_SUMMARY_CSV = BASE_DIR / "PAPER_MODE_overall performance _summary.csv"
REPORT_PATH = BASE_DIR / "PAPER_MODE_analysis_report.html"


HTML_TEMPLATE = Template("""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LTD60 PAPER MODE Analysis Report</title>
    <style>
      :root {
        color-scheme: light;
      }
      body {
        margin: 0;
        font-family: "Garamond", "Book Antiqua", "Palatino Linotype", serif;
        background: linear-gradient(180deg, #f5f7fb 0%, #ffffff 45%, #f1f3f6 100%);
        color: #1d1d1f;
      }
      .page {
        max-width: 1200px;
        margin: 0 auto;
        padding: 32px 20px 48px;
      }
      h1 {
        font-size: 32px;
        margin: 0 0 16px;
        letter-spacing: 0.5px;
      }
      h2 {
        font-size: 20px;
        margin: 24px 0 10px;
        color: #2b2b2b;
      }
      th {
        cursor: pointer;
        user-select: none;
      }
      .pnl-pos {
        color: #0a7a2f;
        font-weight: 600;
      }
      .pnl-neg {
        color: #b00020;
        font-weight: 600;
      }
      .table-wrap {
        max-height: 70vh;
        overflow: auto;
        border: 1px solid #dcdfe6;
        border-radius: 10px;
        background: #ffffff;
        box-shadow: 0 8px 24px rgba(16, 24, 40, 0.08);
      }
      table.dataframe {
        width: 100%;
        border-collapse: collapse;
      }
      table.dataframe th,
      table.dataframe td {
        padding: 6px 8px;
        border: 1px solid #e5e7eb;
        text-align: right;
      }
      table.dataframe th:first-child,
      table.dataframe td:first-child {
        text-align: left;
      }
      .chart-wrap {
        width: 100%;
        overflow: hidden;
        border-radius: 12px;
        background: #ffffff;
        border: 1px solid #dcdfe6;
        box-shadow: 0 8px 24px rgba(16, 24, 40, 0.08);
        padding: 8px 12px 16px;
      }
    </style>
  </head>
  <body>
    <div class="page">
      <h1>LTD60 PAPER MODE Analysis Report</h1>
      <h2>Competition Summary</h2>
      <div class="table-wrap">
      $table_html
      </div>
      <h2>Cumulative PnL (By Match)</h2>
      <div class="chart-wrap">
      $chart_svg
      </div>
      <h2>Overall Summary</h2>
      <div class="table-wrap">
      $overall_table
      </div>
    </div>
    <script>
      (function () {
        const table = document.querySelector("table.dataframe");
        if (!table) return;

        const headerCells = Array.from(table.querySelectorAll("thead th"));
        const pnlIndex = headerCells.findIndex((th) => th.textContent.trim() === "pnl_sum");

        if (pnlIndex >= 0) {
          const rows = Array.from(table.querySelectorAll("tbody tr"));
          rows.forEach((row) => {
            const cell = row.children[pnlIndex];
            if (!cell) return;
            const val = parseFloat(cell.textContent);
            if (Number.isNaN(val)) return;
            cell.classList.add(val >= 0 ? "pnl-pos" : "pnl-neg");
          });
        }

        function parseCell(cell) {
          const text = cell.textContent.trim();
          const num = parseFloat(text);
          return Number.isNaN(num) ? text.toLowerCase() : num;
        }

        function sortTable(colIndex, dir) {
          const tbody = table.querySelector("tbody");
          const rows = Array.from(tbody.querySelectorAll("tr"));
          rows.sort((a, b) => {
            const aVal = parseCell(a.children[colIndex]);
            const bVal = parseCell(b.children[colIndex]);
            if (aVal < bVal) return dir === "asc" ? -1 : 1;
            if (aVal > bVal) return dir === "asc" ? 1 : -1;
            return 0;
          });
          rows.forEach((row) => tbody.appendChild(row));
        }

        headerCells.forEach((th, idx) => {
          th.addEventListener("click", () => {
            const current = th.getAttribute("data-sort-dir") || "desc";
            const next = current === "asc" ? "desc" : "asc";
            headerCells.forEach((h) => h.removeAttribute("data-sort-dir"));
            th.setAttribute("data-sort-dir", next);
            sortTable(idx, next);
          });
        });
      })();
    </script>
  </body>
</html>
""")


def update_report() -> None:
    if not COMP_SUMMARY_CSV.exists():
        raise FileNotFoundError(f"Missing CSV: {COMP_SUMMARY_CSV}")
    df = pd.read_csv(COMP_SUMMARY_CSV)
    table_html = df.to_html(index=False, border=0, classes="dataframe")

    if not CUM_PNL_CSV.exists():
        raise FileNotFoundError(f"Missing CSV: {CUM_PNL_CSV}")
    pnl_df = pd.read_csv(CUM_PNL_CSV)
    pnl_vals = pd.to_numeric(pnl_df.get("cum_pnl"), errors="coerce").fillna(0.0).to_numpy()
    chart_svg = _build_cum_pnl_svg(pnl_vals)

    overall_table = ""
    if OVERALL_SUMMARY_CSV.exists():
        overall_df = pd.read_csv(OVERALL_SUMMARY_CSV)
        overall_table = overall_df.to_html(index=False, border=0, classes="dataframe")

    html = HTML_TEMPLATE.safe_substitute(
        table_html=table_html,
        chart_svg=chart_svg,
        overall_table=overall_table,
    )
    REPORT_PATH.write_text(html, encoding="ascii")


def _build_cum_pnl_svg(values) -> str:
    width = 1000
    height = 360
    pad = 40
    if len(values) == 0:
        return '<svg width="100%" height="360" viewBox="0 0 1000 360"></svg>'

    min_v = float(values.min())
    max_v = float(values.max())
    if min_v == max_v:
        min_v -= 1.0
        max_v += 1.0

    x_span = max(len(values) - 1, 1)
    y_span = max_v - min_v

    points = []
    for i, v in enumerate(values):
        x = pad + (width - 2 * pad) * (i / x_span)
        y = height - pad - (height - 2 * pad) * ((v - min_v) / y_span)
        points.append(f"{x:.2f},{y:.2f}")

    axis_y = height - pad
    axis_x = pad
    chart_w = width - 2 * pad
    chart_h = height - 2 * pad
    tick_count = 5

    def grid_lines(count, vertical):
        lines = []
        for i in range(count + 1):
            if vertical:
                x = axis_x + (chart_w * i / count)
                lines.append(
                    f'<line x1="{x:.2f}" y1="{pad}" x2="{x:.2f}" y2="{axis_y}" stroke="#e6e6e6" />'
                )
            else:
                y = pad + (chart_h * i / count)
                lines.append(
                    f'<line x1="{axis_x}" y1="{y:.2f}" x2="{width - pad}" y2="{y:.2f}" stroke="#e6e6e6" />'
                )
        return "".join(lines)

    y_label = "Cumulative PnL"
    x_label = "Match Number"
    legend_x = pad + 6
    legend_y = pad - 24

    def fmt_num(val):
        return f"{val:.0f}"

    def y_tick_label(i):
        val = max_v - (y_span * i / tick_count)
        return fmt_num(val)

    def x_tick_label(i):
        if len(values) <= 1:
            return "1"
        val = 1 + (len(values) - 1) * i / tick_count
        return f"{int(round(val))}"

    y_ticks = []
    x_ticks = []
    for i in range(tick_count + 1):
        y = pad + (chart_h * i / tick_count)
        y_ticks.append(
            f'<text x="{axis_x - 8}" y="{y + 4:.2f}" text-anchor="end" '
            f'font-family="Garamond, serif" font-size="11" fill="#555">{y_tick_label(i)}</text>'
        )
        x = axis_x + (chart_w * i / tick_count)
        x_ticks.append(
            f'<text x="{x:.2f}" y="{axis_y + 16}" text-anchor="middle" '
            f'font-family="Garamond, serif" font-size="11" fill="#555">{x_tick_label(i)}</text>'
        )

    return (
        f'<svg width="100%" height="{height}" viewBox="0 0 {width} {height}" '
        f'xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Cumulative PnL chart">'
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="white" />'
        f'{grid_lines(8, vertical=False)}'
        f'{grid_lines(8, vertical=True)}'
        f'<line x1="{axis_x}" y1="{axis_y}" x2="{width - pad}" y2="{axis_y}" stroke="#999" />'
        f'<line x1="{axis_x}" y1="{pad}" x2="{axis_x}" y2="{axis_y}" stroke="#999" />'
        f'{"".join(y_ticks)}'
        f'{"".join(x_ticks)}'
        f'<polyline fill="none" stroke="#1f5fbf" stroke-width="2.5" points="{" ".join(points)}" />'
        f'<text x="{width / 2:.2f}" y="{height - 6}" text-anchor="middle" '
        f'font-family="Garamond, serif" font-size="12" fill="#333">{x_label}</text>'
        f'<text x="12" y="{height / 2:.2f}" text-anchor="middle" '
        f'transform="rotate(-90, 12, {height / 2:.2f})" '
        f'font-family="Garamond, serif" font-size="12" fill="#333">{y_label}</text>'
        f'<rect x="{legend_x}" y="{legend_y}" width="150" height="22" fill="#fff" stroke="#ddd" />'
        f'<line x1="{legend_x + 8}" y1="{legend_y + 11}" x2="{legend_x + 36}" y2="{legend_y + 11}" '
        f'stroke="#1f5fbf" stroke-width="2.5" />'
        f'<text x="{legend_x + 44}" y="{legend_y + 15}" font-family="Garamond, serif" '
        f'font-size="12" fill="#333">Cumulative PnL</text>'
        f"</svg>"
    )


if __name__ == "__main__":
    update_report()
