#!/usr/bin/env python3
"""
Generate LTD60_report.html from:
- SQLite DB: autotrader_data.db (tables: BACKTEST_HISTORY, archive_v3, etc.)
- Backtest outputs in OUT_DIR (CSVs): filtered_leagues_v2.csv, late_goal_leagues_v2.csv,
  league_pnl_summary_v2.csv, cum_pnl_overall.csv, cum_pnl_by_comp.csv

Usage:
  python generate_ltd60_report.py --db "C:\path\to\autotrader_data.db" --out_dir "C:\path\to\data_analysis" --out "LTD60_report.html"
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import math

import pandas as pd



def _pick_first_existing(base: Path, names: list[str]) -> Path:
    for n in names:
        p = base / n
        if p.exists():
            return p
    return base / names[0]

def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def safe_read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def connect_ro(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    try:
        return sqlite3.connect(uri, uri=True)
    except Exception:
        return sqlite3.connect(str(db_path))


def fetch_backtest_history(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        return pd.read_sql_query(
            "SELECT * FROM BACKTEST_HISTORY ORDER BY run_ts DESC LIMIT 3",
            conn
        )
    except Exception:
        return pd.DataFrame()


def parse_json_field(val: Any) -> List[Any]:
    if val is None:
        return []
    if isinstance(val, (list, dict)):
        return val if isinstance(val, list) else [val]
    s = str(val).strip()
    if not s:
        return []
    try:
        return json.loads(s)
    except Exception:
        return []


def diff_lists(new_list: List[str], old_list: List[str]) -> Tuple[List[str], List[str]]:
    new_set, old_set = set(new_list), set(old_list)
    return sorted(list(new_set - old_set)), sorted(list(old_set - new_set))



def _json_sanitize(x):
    """Recursively convert pandas/numpy/NaN into JSON-safe Python types."""
    try:
        import pandas as _pd
        import numpy as _np
        if x is _pd.NA:
            return None
        if isinstance(x, (_np.floating, float)):
            if _np.isnan(x) or _np.isinf(x):
                return None
            return float(x)
        if isinstance(x, (_np.integer, int)):
            return int(x)
        if isinstance(x, (_pd.Timestamp,)):
            return x.isoformat()
    except Exception:
        pass

    if x is None:
        return None
    if isinstance(x, (str, bool)):
        return x
    if isinstance(x, (list, tuple)):
        return [_json_sanitize(v) for v in x]
    if isinstance(x, dict):
        return {str(k): _json_sanitize(v) for k, v in x.items()}
    try:
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
    except Exception:
        pass
    return x

def df_to_records(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df.fillna("").to_dict(orient="records")


def compute_drawdown(cum_series: pd.Series) -> float:
    if cum_series.empty:
        return 0.0
    running_max = cum_series.cummax()
    dd = cum_series - running_max
    return float(dd.min())


def streaks(pnl_series: pd.Series) -> Tuple[int, int]:
    win = lose = 0
    best_win = best_lose = 0
    for v in pnl_series.fillna(0.0).astype(float):
        if v > 0:
            win += 1
            lose = 0
        elif v < 0:
            lose += 1
            win = 0
        else:
            win = 0
            lose = 0
        best_win = max(best_win, win)
        best_lose = max(best_lose, lose)
    return int(best_win), int(best_lose)


def query_live_strategy(conn: sqlite3.Connection) -> pd.DataFrame:
    for table in ("archive_v3", "archive_v2"):
        try:
            df = pd.read_sql_query(
                f"SELECT * FROM {table} WHERE strategy='LTD60' ORDER BY kickoff ASC",
                conn
            )
            if not df.empty:
                return df
        except Exception:
            continue
    return pd.DataFrame()


def normalise_comp_cols(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return df
    if "comp" not in df.columns and "League" in df.columns:
        df = df.rename(columns={"League": "comp"})
    return df


def render_html(payload: Dict[str, Any]) -> str:
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>LTD60 Strategy Data Analysis</title>

  <link rel="preconnect" href="https://cdn.jsdelivr.net" crossorigin>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/datatables.net-dt@2.1.8/css/dataTables.dataTables.min.css">
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; color: #111; }}
    h1 {{ margin: 0 0 6px 0; }}
    .muted {{ color: #555; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 12px; }}
    .card {{ border: 1px solid #e5e5e5; border-radius: 12px; padding: 14px; background: #fff; box-shadow: 0 1px 2px rgba(0,0,0,.04); }}
    .card h2 {{ font-size: 16px; margin: 0 0 10px 0; }}
    .kpis {{ display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px; }}
    .kpi {{ border: 1px solid #eee; border-radius: 10px; padding: 10px; background: #fafafa; }}
    .kpi .label {{ font-size: 12px; color: #666; }}
    .kpi .value {{ font-size: 18px; font-weight: 700; margin-top: 3px; }}
    .table-wrap {{ max-height: 380px; overflow: auto; }}
    canvas {{ width: 100% !important; height: 340px !important; }}
    .row {{ display: grid; grid-template-columns: 1fr; gap: 16px; margin-top: 16px; }}
    .two {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    .links a {{ display: inline-block; margin-right: 12px; }}
    .diff-box {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 12px; background: #0b1020; color: #d6e0ff; padding: 10px; border-radius: 10px; overflow: auto;
      max-height: 220px; white-space: pre; }}
    .small {{ font-size: 12px; }}
  </style>
</head>

<body>
  <h1>LTD60 Strategy Data Analysis</h1>
  <div class="muted small">
    Generated: <span id="generatedTs"></span> |
    Backtest last run: <span id="lastBacktestTs">N/A</span> |
    Quarter: <span id="lastQuarter">N/A</span>
  </div>

  <div class="row">
    <div class="card">
      <h2>Backtest change log (last two runs)</h2>
      <div class="grid">
        <div>
          <div class="muted small">Filtered leagues changes</div>
          <div id="diffFiltered" class="diff-box"></div>
        </div>
        <div>
          <div class="muted small">Late-goal leagues changes</div>
          <div id="diffLate" class="diff-box"></div>
        </div>
      </div>
    </div>

    <div class="two">
      <div class="card">
        <h2>Filtered leagues (decisive-rate qualifier)</h2>
        <div class="table-wrap"><table id="tblFiltered" class="display" style="width:100%"></table></div>
      </div>
      <div class="card">
        <h2>Filtered leagues decisive rate</h2>
        <canvas id="chartFiltered"></canvas>
      </div>
    </div>

    <div class="two">
      <div class="card">
        <h2>Late-goal leagues (0-0 @60 to decisive)</h2>
        <div class="table-wrap"><table id="tblLate" class="display" style="width:100%"></table></div>
      </div>
      <div class="card">
        <h2>Late-goal decisive rate (0-0 @60 sample)</h2>
        <canvas id="chartLate"></canvas>
      </div>
    </div>

    <div class="two">
      <div class="card">
        <h2>League PnL summary (Backtest)</h2>
        <div class="table-wrap"><table id="tblPnl" class="display" style="width:100%"></table></div>
      </div>
      <div class="card">
        <h2>Cumulative PnL (Backtest): Entry 1 vs Entry 1+2</h2>
        <canvas id="chartCumOverall"></canvas>
      </div>
    </div>

    <div class="card">
      <h2>Backtest cumulative PnL by competition (select competition)</h2>
      <div style="display:flex; gap:10px; align-items:center; margin-bottom:10px;">
        <div class="muted small">Competition</div>
        <select id="compSelect"></select>
      </div>
      <canvas id="chartCumComp"></canvas>
    </div>

    <div class="card">
      <h2>Backtest performance summary</h2>
      <div class="kpis" id="kpisBacktest"></div>
    </div>

    <div class="card">
      <h2>Strategy live performance (archive_v3, strategy='LTD60')</h2>
      <div class="kpis" id="kpisLive"></div>
      <div class="two" style="margin-top:16px;">
        <div>
          <h3 style="margin:0 0 8px 0; font-size:14px;">Competition breakdown</h3>
          <div class="table-wrap"><table id="tblLiveByComp" class="display" style="width:100%"></table></div>
        </div>
        <div>
          <h3 style="margin:0 0 8px 0; font-size:14px;">Cumulative PnL: Live vs Backtest (matched length)</h3>
          <canvas id="chartLiveVsBacktest"></canvas>
        </div>
      </div>
    </div>

    <div class="card links">
      <h2>Outputs</h2>
      <div>
        <a id="lnkFiltered" href="#" target="_blank" rel="noopener">filtered_leagues_v2.csv</a>
        <a id="lnkLate" href="#" target="_blank" rel="noopener">late_goal_leagues_v2.csv</a>
      </div>
      <div class="muted small" style="margin-top:8px;">
        Links point to local files; depending on where you open the HTML, your browser may block local file links.
        If so, open the folder and double-click the CSVs, or serve the report via a small local web server.
      </div>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/datatables.net@2.1.8/js/dataTables.min.js"></script>

  <script>
    const DATA = {data_json};

    function fmtNum(x, dp=2) {{
      if (x === null || x === undefined || x === "") return "";
      const n = Number(x);
      if (Number.isNaN(n)) return String(x);
      return n.toFixed(dp);
    }}

    function setText(id, value) {{
      const el = document.getElementById(id);
      if (el) el.textContent = value ?? "";
    }}

    function diffText(diffObj) {{
      const added = diffObj?.added ?? [];
      const removed = diffObj?.removed ?? [];
      const lines = [];
      lines.push("ADDED (" + added.length + "):");
      lines.push(...added.map(x => "  + " + x));
      lines.push("");
      lines.push("REMOVED (" + removed.length + "):");
      lines.push(...removed.map(x => "  - " + x));
      return lines.join("\\n");
    }}

    function renderTable(tableId, rows, columns) {{
      const el = document.getElementById(tableId);
      if (!el) return;
      const dtCols = columns.map(c => ({{ title: c.title, data: c.key, render: c.render }}));
      new DataTable(el, {{
        data: rows,
        columns: dtCols,
        pageLength: 10,
        lengthChange: false,
        searching: true,
        info: true,
        order: [],
      }});
    }}

    function barChart(canvasId, labels, values, yLabel) {{
      const ctx = document.getElementById(canvasId);
      if (!ctx) return null;
      return new Chart(ctx, {{
        type: 'bar',
        data: {{ labels, datasets: [{{ label: yLabel, data: values }}] }},
        options: {{ responsive: true, plugins: {{ legend: {{ display: true }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
      }});
    }}

    function lineChart(canvasId, labels, series, dashedSecond=false) {{
      const ctx = document.getElementById(canvasId);
      if (!ctx) return null;
      const datasets = series.map((s, idx) => {{
        const ds = {{ label: s.label, data: s.data, tension: 0.2, pointRadius: 0 }};
        if (dashedSecond && idx === 1) ds.borderDash = [6,6];
        return ds;
      }});
      return new Chart(ctx, {{
        type: 'line',
        data: {{ labels, datasets }},
        options: {{ responsive: true, plugins: {{ legend: {{ display: true }} }}, scales: {{ x: {{ ticks: {{ maxTicksLimit: 10 }} }} }} }}
      }});
    }}

    function renderKpis(containerId, kpis) {{
      const el = document.getElementById(containerId);
      if (!el) return;
      el.innerHTML = "";
      for (const k of kpis) {{
        const div = document.createElement("div");
        div.className = "kpi";
        div.innerHTML = `<div class="label">${{k.label}}</div><div class="value">${{k.value}}</div>`;
        el.appendChild(div);
      }}
    }}

    setText("generatedTs", DATA.generated_ts);
    setText("lastBacktestTs", DATA.backtest?.last_run_ts ?? "N/A");
    setText("lastQuarter", DATA.backtest?.quarter_key ?? "N/A");
    document.getElementById("diffFiltered").textContent = diffText(DATA.backtest?.diff_filtered ?? {{}});
    document.getElementById("diffLate").textContent = diffText(DATA.backtest?.diff_late ?? {{}});

    const l1 = document.getElementById("lnkFiltered");
    const l2 = document.getElementById("lnkLate");
    if (l1) l1.href = DATA.paths?.filtered_csv ?? "#";
    if (l2) l2.href = DATA.paths?.late_csv ?? "#";

    renderTable("tblFiltered", DATA.filtered_leagues ?? [], [
      {{ key: "comp", title: "comp" }},
      {{ key: "matches", title: "matches" }},
      {{ key: "decisive_rate", title: "decisive_rate", render: (d)=>fmtNum(d, 3) }},
    ]);

    renderTable("tblLate", DATA.late_goal_leagues ?? [], [
      {{ key: "comp", title: "comp" }},
      {{ key: "samples", title: "samples" }},
      {{ key: "decisive_rate_from_00at60", title: "decisive_rate_from_00at60", render: (d)=>fmtNum(d, 3) }},
    ]);

    renderTable("tblPnl", DATA.league_pnl ?? [], [
      {{ key: "comp", title: "comp" }},
      {{ key: "matches", title: "matches" }},
      {{ key: "decisive_rate", title: "decisive_rate", render: (d)=>fmtNum(d,3) }},
      {{ key: "00at60_samples", title: "00at60_samples" }},
      {{ key: "00at60_decisive_rate", title: "00at60_decisive_rate", render: (d)=>fmtNum(d,3) }},
      {{ key: "entry1_count", title: "entry1_count" }},
      {{ key: "entry2_count", title: "entry2_count" }},
      {{ key: "pnl_entry1_sum", title: "pnl_entry1_sum", render: (d)=>fmtNum(d,2) }},
      {{ key: "pnl_entry2_sum", title: "pnl_entry2_sum", render: (d)=>fmtNum(d,2) }},
      {{ key: "pnl_total_sum", title: "pnl_total_sum", render: (d)=>fmtNum(d,2) }},
      {{ key: "pnl_entry1_avg_on_bets", title: "pnl_entry1_avg_on_bets", render: (d)=>fmtNum(d,2) }},
      {{ key: "pnl_entry2_avg_on_bets", title: "pnl_entry2_avg_on_bets", render: (d)=>fmtNum(d,2) }},
    ]);
renderTable("tblLiveByComp", DATA.live?.by_comp ?? [], [
      {{ key: "comp", title: "comp" }},
      {{ key: "matches", title: "matches" }},
      {{ key: "wins", title: "wins" }},
      {{ key: "draws", title: "draws" }},
      {{ key: "hit_rate", title: "hit_rate", render: (d)=>fmtNum(d,3) }},
      {{ key: "pnl_total", title: "pnl_total", render: (d)=>fmtNum(d,2) }},
    ]);

    if ((DATA.filtered_leagues ?? []).length) {{
      barChart("chartFiltered", DATA.filtered_leagues.map(r => r.comp), DATA.filtered_leagues.map(r => Number(r.decisive_rate ?? 0)), "decisive_rate");
    }}
    if ((DATA.late_goal_leagues ?? []).length) {{
      barChart("chartLate", DATA.late_goal_leagues.map(r => r.comp), DATA.late_goal_leagues.map(r => Number(r.decisive_rate_from_00at60 ?? 0)), "decisive_rate_from_00at60");
    }}
    if (DATA.cum_overall?.labels?.length) {{
      lineChart("chartCumOverall", DATA.cum_overall.labels, [
        {{ label: "Entry 1 cum pnl", data: DATA.cum_overall.entry1 }},
        {{ label: "Entry 1+2 cum pnl", data: DATA.cum_overall.total }},
      ]);
    }}

    let compChart = null;
    const compSel = document.getElementById("compSelect");
    const compSeries = DATA.cum_by_comp ?? {{}};
    const comps = Object.keys(compSeries).sort();
    if (compSel && comps.length) {{
      for (const c of comps) {{
        const opt = document.createElement("option");
        opt.value = c; opt.textContent = c;
        compSel.appendChild(opt);
      }}
      function renderComp(c) {{
        const s = compSeries[c];
        if (!s) return;
        if (compChart) compChart.destroy();
        compChart = lineChart("chartCumComp", s.labels, [
          {{ label: "Entry 1 cum pnl", data: s.entry1 }},
          {{ label: "Entry 1+2 cum pnl", data: s.total }},
        ]);
      }}
      compSel.addEventListener("change", (e)=>renderComp(e.target.value));
      renderComp(comps[0]);
    }}

    if (DATA.live?.cum?.labels?.length && DATA.backtest?.cum_for_compare?.labels?.length) {{
      lineChart("chartLiveVsBacktest", DATA.live.cum.labels, [
        {{ label: "Live cum pnl", data: DATA.live.cum.total }},
        {{ label: "Backtest cum pnl (matched)", data: DATA.backtest.cum_for_compare.total }},
      ], true);
    }}

    renderKpis("kpisBacktest", DATA.kpis_backtest ?? []);
    renderKpis("kpisLive", DATA.kpis_live ?? []);
  </script>
</body>
</html>"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--out", default="LTD60_report.html")
    args = ap.parse_args()

    db_path = Path(args.db)
    out_dir = Path(args.out_dir)
    out_html = Path(args.out)

    df_filtered = normalise_comp_cols(safe_read_csv((_pick_first_existing(out_dir, ["filtered_leagues_v2.csv","filtered_leagues.csv"]))))
    df_late = normalise_comp_cols(safe_read_csv((_pick_first_existing(out_dir, ["late_goal_leagues_v2.csv","late_goal_leagues.csv"]))))
    df_pnl = normalise_comp_cols(safe_read_csv((_pick_first_existing(out_dir, ["league_pnl_summary_v2.csv","league_pnl_summary.csv"]))))
    df_cum_overall = safe_read_csv(out_dir / "cum_pnl_overall.csv")
    df_cum_by_comp = normalise_comp_cols(safe_read_csv(out_dir / "cum_pnl_by_league.csv"))

    conn = connect_ro(db_path)
    bt_hist = fetch_backtest_history(conn)

    last_run_ts = None
    quarter_key = None
    diff_filtered = {"added": [], "removed": []}
    diff_late = {"added": [], "removed": []}

    if not bt_hist.empty:
        last_run_ts = bt_hist.iloc[0].get("run_ts", None)
        quarter_key = bt_hist.iloc[0].get("quarter_key", None)

        if len(bt_hist) >= 2:
            cur_f = parse_json_field(bt_hist.iloc[0].get("filtered_json"))
            prev_f = parse_json_field(bt_hist.iloc[1].get("filtered_json"))
            cur_l = parse_json_field(bt_hist.iloc[0].get("late_goal_json"))
            prev_l = parse_json_field(bt_hist.iloc[1].get("late_goal_json"))

            def to_names(x):
                out = []
                for it in x:
                    if isinstance(it, dict):
                        out.append(str(it.get("comp") or it.get("league") or it.get("League") or ""))
                    else:
                        out.append(str(it))
                out = [s.strip() for s in out if str(s).strip()]
                return out

            a, r = diff_lists(to_names(cur_f), to_names(prev_f))
            diff_filtered = {"added": a, "removed": r}
            a, r = diff_lists(to_names(cur_l), to_names(prev_l))
            diff_late = {"added": a, "removed": r}

    cum_overall = {"labels": [], "entry1": [], "total": []}
    if df_cum_overall is not None and not df_cum_overall.empty:
        label_col = "n" if "n" in df_cum_overall.columns else df_cum_overall.columns[0]
        e1_col = "cum_pnl_entry1" if "cum_pnl_entry1" in df_cum_overall.columns else None
        tot_col = "cum_pnl_total" if "cum_pnl_total" in df_cum_overall.columns else None
        if e1_col and tot_col:
            cum_overall["labels"] = df_cum_overall[label_col].astype(str).tolist()
            cum_overall["entry1"] = df_cum_overall[e1_col].fillna(0).astype(float).tolist()
            cum_overall["total"] = df_cum_overall[tot_col].fillna(0).astype(float).tolist()

    cum_by_comp: Dict[str, Any] = {}
    if df_cum_by_comp is not None and not df_cum_by_comp.empty:
        needed = {"comp", "n", "cum_pnl_entry1", "cum_pnl_total"}
        if needed.issubset(set(df_cum_by_comp.columns)):
            for comp, g in df_cum_by_comp.groupby("comp"):
                g = g.sort_values("n")
                cum_by_comp[str(comp)] = {
                    "labels": g["n"].astype(str).tolist(),
                    "entry1": g["cum_pnl_entry1"].fillna(0).astype(float).tolist(),
                    "total": g["cum_pnl_total"].fillna(0).astype(float).tolist(),
                }

    league_pnl_records = []
    kpis_backtest = []
    if df_pnl is not None and not df_pnl.empty:
        tmp = df_pnl.copy()
        if "wins" not in tmp.columns and {"matches", "decisive_rate"}.issubset(set(tmp.columns)):
            tmp["wins"] = (pd.to_numeric(tmp["matches"], errors="coerce").fillna(0) * pd.to_numeric(tmp["decisive_rate"], errors="coerce").fillna(0)).round().astype(int)
            tmp["draws"] = (pd.to_numeric(tmp["matches"], errors="coerce").fillna(0).astype(int) - tmp["wins"]).astype(int)
        if "wins" not in tmp.columns:
            tmp["wins"] = ""
        if "draws" not in tmp.columns:
            tmp["draws"] = ""

        out = pd.DataFrame({
            "comp": tmp.get("comp", ""),
            "matches": tmp.get("matches", ""),
            "wins": tmp.get("wins", ""),
            "draws": tmp.get("draws", ""),
            "decisive_rate": tmp.get("decisive_rate", ""),
            "decisive_rate_from_00at60": tmp.get("00at60_decisive_rate", tmp.get("00at60_decisive_rate", "")),
            "pnl_entry1": tmp.get("pnl_entry1_sum", ""),
            "pnl_entry2": tmp.get("pnl_entry2_sum", ""),
            "pnl_total": tmp.get("pnl_total_sum", ""),
            "entry2_count": tmp.get("entry2_count", ""),
        })
        league_pnl_records = df_to_records(out)

        pnl_total = float(pd.to_numeric(tmp.get("pnl_total_sum"), errors="coerce").fillna(0).sum())
        pnl_e1 = float(pd.to_numeric(tmp.get("pnl_entry1_sum"), errors="coerce").fillna(0).sum())
        pnl_e2 = float(pd.to_numeric(tmp.get("pnl_entry2_sum"), errors="coerce").fillna(0).sum())
        matches = int(pd.to_numeric(tmp.get("matches"), errors="coerce").fillna(0).sum())

        cum_total = pd.Series(cum_overall["total"]) if cum_overall["total"] else pd.Series(dtype=float)
        pnl_increments = cum_total.diff().fillna(cum_total) if not cum_total.empty else pd.Series(dtype=float)
        max_dd = compute_drawdown(cum_total) if not cum_total.empty else 0.0
        win_streak, lose_streak = streaks(pnl_increments)

        kpis_backtest = [
            {"label": "Total matches", "value": str(matches)},
            {"label": "Entry1 PnL", "value": f"{pnl_e1:.2f}"},
            {"label": "Entry2 PnL", "value": f"{pnl_e2:.2f}"},
            {"label": "Total PnL", "value": f"{pnl_total:.2f}"},
            {"label": "Max drawdown", "value": f"{max_dd:.2f}"},
            {"label": "Win / lose streak", "value": f"{win_streak} / {lose_streak}"},
        ]

    live_df = normalise_comp_cols(query_live_strategy(conn))
    kpis_live = []
    live_by_comp = []
    live_cum = {"labels": [], "total": []}

    if live_df is not None and not live_df.empty:
        live_df["pnl"] = pd.to_numeric(live_df.get("pnl"), errors="coerce").fillna(0.0)
        live_df["result"] = pd.to_numeric(live_df.get("result"), errors="coerce")
        wins = int((live_df["result"] == 1).sum()) if "result" in live_df.columns else 0
        draws = int((live_df["result"] == 0).sum()) if "result" in live_df.columns else 0
        matches = int(len(live_df))
        pnl_total = float(live_df["pnl"].sum())
        hit_rate = float(wins / matches) if matches else 0.0

        live_df = live_df.sort_values("kickoff")
        live_df["cum_pnl"] = live_df["pnl"].cumsum()
        live_cum["labels"] = list(range(1, matches + 1))
        live_cum["total"] = live_df["cum_pnl"].astype(float).tolist()

        max_dd = compute_drawdown(live_df["cum_pnl"])
        win_streak, lose_streak = streaks(live_df["pnl"])

        kpis_live = [
            {"label": "Total matches", "value": str(matches)},
            {"label": "Wins / draws", "value": f"{wins} / {draws}"},
            {"label": "Hit rate", "value": f"{hit_rate:.3f}"},
            {"label": "Total PnL", "value": f"{pnl_total:.2f}"},
            {"label": "Max drawdown", "value": f"{max_dd:.2f}"},
            {"label": "Win / lose streak", "value": f"{win_streak} / {lose_streak}"},
        ]

        if "comp" in live_df.columns:
            g = live_df.groupby("comp").agg(
                matches=("event_id", "count"),
                wins=("result", lambda s: int((pd.to_numeric(s, errors="coerce") == 1).sum())),
                draws=("result", lambda s: int((pd.to_numeric(s, errors="coerce") == 0).sum())),
                pnl_total=("pnl", "sum"),
            ).reset_index()
            g["hit_rate"] = g["wins"] / g["matches"]
            live_by_comp = df_to_records(g.sort_values("pnl_total", ascending=False))

    cum_for_compare = {"labels": [], "total": []}
    if live_cum["labels"] and cum_overall["total"]:
        n = len(live_cum["labels"])
        cum_for_compare = {"labels": live_cum["labels"], "total": cum_overall["total"][:n]}

    payload = {
        "generated_ts": iso_now(),
        "paths": {
            "filtered_csv": str(((_pick_first_existing(out_dir, ["filtered_leagues_v2.csv","filtered_leagues.csv"]))).resolve()),
            "late_csv": str(((_pick_first_existing(out_dir, ["late_goal_leagues_v2.csv","late_goal_leagues.csv"]))).resolve()),
        },
        "backtest": {
            "last_run_ts": last_run_ts,
            "quarter_key": quarter_key,
            "diff_filtered": diff_filtered,
            "diff_late": diff_late,
            "cum_for_compare": cum_for_compare,
        },
        "filtered_leagues": df_to_records(df_filtered),
        "late_goal_leagues": df_to_records(df_late),
        "league_pnl": league_pnl_records,
        "cum_overall": cum_overall,
        "cum_by_comp": cum_by_comp,
        "kpis_backtest": kpis_backtest,
        "kpis_live": kpis_live,
        "live": {"by_comp": live_by_comp, "cum": live_cum},
    }

    out_html.write_text(render_html(payload), encoding="utf-8")
    print(f"Wrote report: {out_html.resolve()}")
    print(league_pnl_records)

if __name__ == "__main__":
    main()
