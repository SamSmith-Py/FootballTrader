import csv
import json
import sqlite3
from html import escape
from pathlib import Path


HTML_PATH = Path(r"C:\Users\Sam\FootballTrader v0.3.3\data_analysis\LTD60_backtest_analysis_report.html")
DB_PATH = Path(r"C:\Users\Sam\FootballTrader v0.3.3\database\autotrader_data.db")
LEAGUE_PNL_CSV = Path(r"C:\Users\Sam\FootballTrader v0.3.3\data_analysis\league_pnl_summary_v3.csv")
FILTERED_LEAGUES_CSV = Path(r"C:\Users\Sam\FootballTrader v0.3.3\data_analysis\filtered_leagues_3.csv")
FILTERED_CUM_PNL_CSV = Path(r"C:\Users\Sam\FootballTrader v0.3.3\data_analysis\filtered_cum_pnl_overall_v3.csv")
OVERALL_SUMMARY_CSV = Path(r"C:\Users\Sam\FootballTrader v0.3.3\data_analysis\overall_performance_summary_v3.csv")

META_START = "<!-- LTD60_HISTORY_META_START -->"
META_END = "<!-- LTD60_HISTORY_META_END -->"
LEAGUE_CHANGES_START = "<!-- LTD60_LEAGUE_CHANGES_START -->"
LEAGUE_CHANGES_END = "<!-- LTD60_LEAGUE_CHANGES_END -->"
START_MARKER = "<!-- LTD60_TABLE_START -->"
END_MARKER = "<!-- LTD60_TABLE_END -->"
CHART_START = "<!-- LTD60_CHART_START -->"
CHART_END = "<!-- LTD60_CHART_END -->"
OVERALL_START = "<!-- LTD60_OVERALL_START -->"
OVERALL_END = "<!-- LTD60_OVERALL_END -->"


def load_rates(path: Path) -> dict:
    rates = {}
    if not path.exists():
        return rates
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            comp = row.get("comp", "")
            if not comp:
                continue
            rates[comp] = {
                "decisive_rate": row.get("decisive_rate", ""),
                "decisive_00at60_rate": row.get("decisive_00at60_rate", ""),
            }
    return rates


def load_backtest_history(db_path: Path) -> tuple[dict | None, dict | None]:
    if not db_path.exists():
        return None, None
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT run_ts, quarter_key, filtered_json, late_goal_json
            FROM BACKTEST_HISTORY
            ORDER BY run_ts DESC
            LIMIT 2
        """)
        rows = cur.fetchall()
    except sqlite3.Error:
        rows = []
    finally:
        conn.close()
    if not rows:
        return None, None
    latest = {
        "run_ts": rows[0][0],
        "quarter_key": rows[0][1],
        "filtered_json": rows[0][2],
        "late_goal_json": rows[0][3],
    }
    previous = None
    if len(rows) > 1:
        previous = {
            "run_ts": rows[1][0],
            "quarter_key": rows[1][1],
            "filtered_json": rows[1][2],
            "late_goal_json": rows[1][3],
        }
    return latest, previous


def _parse_json_list(payload: str | None) -> set[str]:
    if not payload:
        return set()
    try:
        items = json.loads(payload)
    except json.JSONDecodeError:
        return set()
    if isinstance(items, list):
        return {str(x) for x in items if str(x).strip()}
    return set()


def _build_list(items: list[str]) -> str:
    if not items:
        return "        <li><em>None</em></li>"
    return "\n".join(f"        <li>{escape(item)}</li>" for item in items)


def build_history_meta_html(latest: dict | None, previous: dict | None) -> str:
    if not latest:
        return '<div class="meta"><strong>No BACKTEST_HISTORY data available.</strong></div>'
    latest_label = latest.get("run_ts") or "latest run"
    prev_label = previous.get("run_ts") if previous else None
    if prev_label:
        return (
            '<div class="meta"><strong>'
            f"Compared latest LTD60 Backtest run {escape(latest_label)} "
            f"vs previous {escape(prev_label)}."
            "</strong></div>"
        )
    return (
        '<div class="meta"><strong>'
        f"Latest LTD60 Backtest run {escape(latest_label)} (no previous history)."
        "</strong></div>"
    )


def build_league_changes_html(latest: dict | None, previous: dict | None) -> str:
    if not latest:
        return (
            "<div class=\"grid\">"
            "<div class=\"card scroll-card\">"
            "<h2>Filtered leagues</h2>"
            "<p><em>No BACKTEST_HISTORY data available.</em></p>"
            "</div>"
            "<div class=\"card scroll-card\">"
            "<h2>Late goal leagues</h2>"
            "<p><em>No BACKTEST_HISTORY data available.</em></p>"
            "</div>"
            "</div>"
        )

    latest_filtered = _parse_json_list(latest.get("filtered_json"))
    latest_late = _parse_json_list(latest.get("late_goal_json"))
    prev_filtered = _parse_json_list(previous.get("filtered_json")) if previous else set()
    prev_late = _parse_json_list(previous.get("late_goal_json")) if previous else set()

    added_filtered = sorted(latest_filtered - prev_filtered)
    removed_filtered = sorted(prev_filtered - latest_filtered)
    added_late = sorted(latest_late - prev_late)
    removed_late = sorted(prev_late - latest_late)

    filtered_added_html = _build_list(added_filtered)
    filtered_removed_html = _build_list(removed_filtered)
    late_added_html = _build_list(added_late)
    late_removed_html = _build_list(removed_late)

    return f"""
  <div class="grid">
    <div class="card scroll-card">
      <h2>Filtered leagues</h2>
      <h3>Added</h3>
      <ul>
{filtered_added_html}
      </ul>
      <h3>Removed</h3>
      <ul>
{filtered_removed_html}
      </ul>
    </div>

    <div class="card scroll-card">
      <h2>Late goal leagues</h2>
      <h3>Added</h3>
      <ul>
{late_added_html}
      </ul>
      <h3>Removed</h3>
      <ul>
{late_removed_html}
      </ul>
    </div>
  </div>
""".strip("\n")


def load_table_rows(path: Path, rates: dict) -> tuple[list[str], list[dict]]:
    headers = []
    rows = []
    if not path.exists():
        return headers, rows
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        for row in reader:
            comp = row.get("comp", "")
            rate_row = rates.get(comp, {})
            row["decisive_rate"] = rate_row.get("decisive_rate", "")
            row["decisive_00at60_rate"] = rate_row.get("decisive_00at60_rate", "")
            rows.append(row)
    for extra in ("decisive_rate", "decisive_00at60_rate"):
        if extra not in headers:
            headers.append(extra)
    return headers, rows


def build_table_html(headers: list[str], rows: list[dict]) -> str:
    th_html = "\n".join(f"        <th>{escape(h)}</th>" for h in headers)
    tr_html = "\n".join(
        "        <tr>"
        + "".join(f"<td>{escape(str(r.get(h, '')))}</td>" for h in headers)
        + "</tr>"
        for r in rows
    )
    if not tr_html:
        tr_html = (
            f"        <tr><td colspan=\"{len(headers) or 1}\">"
            "<em>No data available</em></td></tr>"
        )
    return f"""
  <h2>LTD60 Competitions Performance</h2>
  <div style="font-size:12px;color:#555;margin-bottom:10px;">
    Filtered Competitions decisive rate &gt;= 76%,
    LTD60 Late Goal Competitions decisive from 0-0 at 60' rate &gt;= 70%
  </div>
  <div class="table-wrap" aria-label="LTD60 competitions performance table">
    <table>
      <thead>
        <tr>
{th_html}
        </tr>
      </thead>
      <tbody>
{tr_html}
      </tbody>
    </table>
  </div>
""".strip("\n")


def update_section(html_path: Path, start_marker: str, end_marker: str, block: str) -> None:
    text = html_path.read_text(encoding="utf-8") if html_path.exists() else ""
    if start_marker in text and end_marker in text:
        before, rest = text.split(start_marker, 1)
        _, after = rest.split(end_marker, 1)
        new_text = before + start_marker + "\n" + block + "\n" + end_marker + after
        html_path.write_text(new_text, encoding="utf-8")


def update_html(html_path: Path, block: str) -> None:
    text = html_path.read_text(encoding="utf-8") if html_path.exists() else ""
    if START_MARKER in text and END_MARKER in text:
        before, rest = text.split(START_MARKER, 1)
        _, after = rest.split(END_MARKER, 1)
        new_text = before + START_MARKER + "\n" + block + "\n" + END_MARKER + after
    else:
        insert_point = text.rfind("</body>")
        if insert_point == -1:
            new_text = text + "\n" + START_MARKER + "\n" + block + "\n" + END_MARKER + "\n"
        else:
            new_text = (
                text[:insert_point]
                + "\n"
                + START_MARKER
                + "\n"
                + block
                + "\n"
                + END_MARKER
                + "\n"
                + text[insert_point:]
            )
    html_path.write_text(new_text, encoding="utf-8")


def build_chart_html(csv_path: Path) -> str:
    cum_pnl = []
    cum_total = []
    if csv_path.exists():
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    cum_pnl.append(float(row.get("cum_pnl", 0.0)))
                    cum_total.append(float(row.get("cum_total_pnl", 0.0)))
                except Exception:
                    continue
    match_n = list(range(1, len(cum_pnl) + 1))

    data_json = {
        "match_n": match_n,
        "cum_pnl": cum_pnl,
        "cum_total_pnl": cum_total,
    }

    return f"""
  <h2>Filtered Cumulative PnL (LTD vs LTD60)</h2>
  <div id="filtered-pnl-chart" style="width:100%; max-width:none; height:360px; position:relative;"></div>
  <div id="filtered-pnl-tooltip" style="position:absolute; display:none; padding:6px 8px; font-size:12px; background:#111; color:#fff; border-radius:6px; pointer-events:none;"></div>
  <script>
    (function() {{
      var data = {data_json};
      var container = document.getElementById('filtered-pnl-chart');
      var tooltip = document.getElementById('filtered-pnl-tooltip');
      if (!container || !data.match_n.length) {{
        container.innerHTML = '<em>No data available</em>';
        return;
      }}

      var w = container.clientWidth || 900;
      var h = container.clientHeight || 360;
      var pad = {{ left: 60, right: 20, top: 20, bottom: 40 }};
      var xs = data.match_n;
      var y1 = data.cum_pnl;
      var y2 = data.cum_total_pnl;

      var xMin = xs[0];
      var xMax = xs[xs.length - 1];
      var yMin = Math.min.apply(null, y1.concat(y2));
      var yMax = Math.max.apply(null, y1.concat(y2));
      if (yMin == yMax) {{ yMin -= 1; yMax += 1; }}

      function sx(x) {{
        return pad.left + (x - xMin) * (w - pad.left - pad.right) / (xMax - xMin || 1);
      }}
      function sy(y) {{
        return pad.top + (yMax - y) * (h - pad.top - pad.bottom) / (yMax - yMin || 1);
      }}
      function ix(px) {{
        return xMin + (px - pad.left) * (xMax - xMin) / (w - pad.left - pad.right);
      }}

      function pathLine(xs, ys) {{
        var d = '';
        for (var i = 0; i < xs.length; i++) {{
          var x = sx(xs[i]);
          var y = sy(ys[i]);
          d += (i === 0 ? 'M' : 'L') + x.toFixed(2) + ' ' + y.toFixed(2) + ' ';
        }}
        return d.trim();
      }}

      var svg = '';
      svg += '<svg id="filtered-pnl-svg" width="100%" height="' + h + '" viewBox="0 0 ' + w + ' ' + h + '" xmlns="http://www.w3.org/2000/svg">';
      // Axes
      svg += '<line x1="' + pad.left + '" y1="' + pad.top + '" x2="' + pad.left + '" y2="' + (h - pad.bottom) + '" stroke="#333" stroke-width="1" />';
      svg += '<line x1="' + pad.left + '" y1="' + (h - pad.bottom) + '" x2="' + (w - pad.right) + '" y2="' + (h - pad.bottom) + '" stroke="#333" stroke-width="1" />';

      // Y ticks + horizontal grid
      var ticks = 5;
      for (var t = 0; t <= ticks; t++) {{
        var yv = yMin + (yMax - yMin) * t / ticks;
        var yy = sy(yv);
        svg += '<line x1="' + pad.left + '" y1="' + yy + '" x2="' + (w - pad.right) + '" y2="' + yy + '" stroke="#e6e6e6" stroke-width="1" />';
        svg += '<line x1="' + (pad.left - 4) + '" y1="' + yy + '" x2="' + pad.left + '" y2="' + yy + '" stroke="#333" />';
        svg += '<text x="' + (pad.left - 8) + '" y="' + (yy + 4) + '" font-size="12" text-anchor="end" fill="#333">' + yv.toFixed(0) + '</text>';
      }}

      // X ticks + vertical grid
      var xTicks = 6;
      for (var i = 0; i <= xTicks; i++) {{
        var xv = xMin + (xMax - xMin) * i / xTicks;
        var xx = sx(xv);
        svg += '<line x1="' + xx + '" y1="' + pad.top + '" x2="' + xx + '" y2="' + (h - pad.bottom) + '" stroke="#e6e6e6" stroke-width="1" />';
        svg += '<line x1="' + xx + '" y1="' + (h - pad.bottom) + '" x2="' + xx + '" y2="' + (h - pad.bottom + 4) + '" stroke="#333" />';
        svg += '<text x="' + xx + '" y="' + (h - pad.bottom + 16) + '" font-size="12" text-anchor="middle" fill="#333">' + Math.round(xv) + '</text>';
      }}

      // Lines
      svg += '<path d="' + pathLine(xs, y1) + '" fill="none" stroke="#2d6cdf" stroke-width="2" />';
      svg += '<path d="' + pathLine(xs, y2) + '" fill="none" stroke="#d04b36" stroke-width="2" />';

      // Legend
      svg += '<rect x="' + (pad.left + 10) + '" y="' + (pad.top + 6) + '" width="10" height="10" fill="#2d6cdf" />';
      svg += '<text x="' + (pad.left + 26) + '" y="' + (pad.top + 15) + '" font-size="12" fill="#333">LTD</text>';
      svg += '<rect x="' + (pad.left + 80) + '" y="' + (pad.top + 6) + '" width="10" height="10" fill="#d04b36" />';
      svg += '<text x="' + (pad.left + 96) + '" y="' + (pad.top + 15) + '" font-size="12" fill="#333">LTD60</text>';

      svg += '</svg>';
      container.innerHTML = svg;

      var svgEl = document.getElementById('filtered-pnl-svg');
      function clientToSvg(e) {{
        var rect = svgEl.getBoundingClientRect();
        var x = (e.clientX - rect.left) * (w / rect.width);
        var y = (e.clientY - rect.top) * (h / rect.height);
        return {{ x: x, y: y }};
      }}

      svgEl.addEventListener('mousemove', function(e) {{
        var pt = clientToSvg(e);
        if (pt.x < pad.left || pt.x > w - pad.right || pt.y < pad.top || pt.y > h - pad.bottom) {{
          tooltip.style.display = 'none';
          return;
        }}
        var xVal = ix(pt.x);
        var idx = Math.min(xs.length - 1, Math.max(0, Math.round(xVal - 1)));
        tooltip.style.display = 'block';
        tooltip.style.left = e.clientX + 'px';
        tooltip.style.top = e.clientY + 'px';
        tooltip.textContent = 'Match ' + xs[idx] + ' | LTD: ' + y1[idx].toFixed(2) + ' | LTD60: ' + y2[idx].toFixed(2);
      }});
      svgEl.addEventListener('mouseleave', function() {{
        tooltip.style.display = 'none';
      }});
    }})();
  </script>
""".strip("\n")


def update_chart(html_path: Path, block: str) -> None:
    text = html_path.read_text(encoding="utf-8") if html_path.exists() else ""
    if CHART_START in text and CHART_END in text:
        before, rest = text.split(CHART_START, 1)
        _, after = rest.split(CHART_END, 1)
        new_text = before + CHART_START + "\n" + block + "\n" + CHART_END + after
    else:
        insert_point = text.rfind("</body>")
        if insert_point == -1:
            new_text = text + "\n" + CHART_START + "\n" + block + "\n" + CHART_END + "\n"
        else:
            new_text = (
                text[:insert_point]
                + "\n"
                + CHART_START
                + "\n"
                + block
                + "\n"
                + CHART_END
                + "\n"
                + text[insert_point:]
            )
    html_path.write_text(new_text, encoding="utf-8")


def build_overall_html(csv_path: Path) -> str:
    headers = []
    rows = []
    if csv_path.exists():
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            for row in reader:
                rows.append(row)

    th_html = "\n".join(f"        <th>{escape(h)}</th>" for h in headers)
    tr_html = "\n".join(
        "        <tr>"
        + "".join(f"<td>{escape(str(r.get(h, '')))}</td>" for h in headers)
        + "</tr>"
        for r in rows
    )
    if not tr_html:
        tr_html = (
            f"        <tr><td colspan=\"{len(headers) or 1}\">"
            "<em>No data available</em></td></tr>"
        )
    return f"""
  <h2>Overall Performance Summary</h2>
  <div class="table-wrap" aria-label="Overall performance summary table">
    <table>
      <thead>
        <tr>
{th_html}
        </tr>
      </thead>
      <tbody>
{tr_html}
      </tbody>
    </table>
  </div>
""".strip("\n")


def update_overall(html_path: Path, block: str) -> None:
    text = html_path.read_text(encoding="utf-8") if html_path.exists() else ""
    if OVERALL_START in text and OVERALL_END in text:
        before, rest = text.split(OVERALL_START, 1)
        _, after = rest.split(OVERALL_END, 1)
        new_text = before + OVERALL_START + "\n" + block + "\n" + OVERALL_END + after
    else:
        insert_point = text.rfind("</body>")
        if insert_point == -1:
            new_text = text + "\n" + OVERALL_START + "\n" + block + "\n" + OVERALL_END + "\n"
        else:
            new_text = (
                text[:insert_point]
                + "\n"
                + OVERALL_START
                + "\n"
                + block
                + "\n"
                + OVERALL_END
                + "\n"
                + text[insert_point:]
            )
    html_path.write_text(new_text, encoding="utf-8")


def main() -> None:
    rates = load_rates(FILTERED_LEAGUES_CSV)
    headers, rows = load_table_rows(LEAGUE_PNL_CSV, rates)
    block = build_table_html(headers, rows)
    update_html(HTML_PATH, block)
    latest, previous = load_backtest_history(DB_PATH)
    meta_block = build_history_meta_html(latest, previous)
    update_section(HTML_PATH, META_START, META_END, meta_block)
    league_block = build_league_changes_html(latest, previous)
    update_section(HTML_PATH, LEAGUE_CHANGES_START, LEAGUE_CHANGES_END, league_block)
    chart_block = build_chart_html(FILTERED_CUM_PNL_CSV)
    update_chart(HTML_PATH, chart_block)
    overall_block = build_overall_html(OVERALL_SUMMARY_CSV)
    update_overall(HTML_PATH, overall_block)
    print(f"Updated: {HTML_PATH}")


if __name__ == "__main__":
    main()
