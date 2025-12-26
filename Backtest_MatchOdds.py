"""
Full analysis + LTD backtest (filtered leagues; pie + bar charts)
- Outputs: report.html, CSVs, images into OUT_DIR
- Requires: pandas, numpy, matplotlib, plotly

SCRIPT MAP
1. Imports & Config
2. Helpers
3. Load DB
4. Column detection & normalize
5. Preprocess (dates/scores)
6. League summary + filtered leagues
7. 0–0@60 per-league outcomes (league_outcome_df)
8. CSV exports (filtered + late-goal + others)
9. KPIs
10. Team/fixture stats
11. Charts (pie + stacked bar)
12. LTD baseline backtest
13. Baseline cumulative PnL
14. Progressive LTD sim (2nd entry @60′)
15. Progressive cumulative PnL
16. Perf summary
17. Build HTML (incl. download links + quarterly diffs)
18. Write files
"""

# 1) Imports & Config
import os
import re
import json
import sqlite3
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.offline import plot as plotly_plot

DB_PATH = r"C:\Users\Sam\FootballTrader v0.3.3\database\autotrader_data.db"
OUT_DIR = r"C:\Users\Sam\FootballTrader v0.3.3\data_analysis"
TABLE_NAME = "archive_v2"
REPORT_HTML = os.path.join(OUT_DIR, "report.html")

START_DATE = pd.Timestamp("2025-07-01")
MIN_LEAGUE_MATCHES = 10
DECISIVE_THRESHOLD = 0.75

ODDS_COL = "Odds Betfair Draw"
STAKE = 100.0
MAX_ODDS_ACCEPT = 4.0

EARLY_DRAW_PCT = 0.5
EARLY_1GOAL_PROFIT = 50.0

os.makedirs(OUT_DIR, exist_ok=True)

# 2) Helpers
def parse_ht_score_to_pair(x):
    """
    Parses ht_score like '0-0', '1 - 0', '0:0' into (home, away).
    Returns (np.nan, np.nan) if parsing fails.
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return (np.nan, np.nan)

    s = str(x).strip()
    if not s:
        return (np.nan, np.nan)

    # normalize separators
    s = s.replace(":", "-").replace(" ", "")
    if "-" not in s:
        return (np.nan, np.nan)

    left, right = s.split("-", 1)
    try:
        return (int(left), int(right))
    except Exception:
        return (np.nan, np.nan)

def to_int_safe(x):
    """Converts band goal counts to int where possible, else NaN."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    try:
        # Handles "0", "1", 0, 1.0 etc.
        return int(float(str(x).strip()))
    except Exception:
        return np.nan
def parse_score_pair(s):
    if pd.isna(s):
        return (np.nan, np.nan)
    s = str(s)
    nums = re.findall(r"-?\d+", s)
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])
    return (np.nan, np.nan)

def save_fig(fig, name):
    path = os.path.join(OUT_DIR, name)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path

def df_to_html_with_id(dfobj, table_id, maxrows=1000):
    if dfobj is None or dfobj.empty:
        return "<div><em>No data</em></div>"
    html = dfobj.head(maxrows).to_html(index=False, classes="display", table_id=table_id, escape=False)
    if 'id="' not in html:
        html = html.replace('<table ', f'<table id="{table_id}" ', 1)
    return html

def kpi_color(val):
    if pd.isna(val): return "#777777"
    if val >= 0.7:   return "#1a7f37"
    if val >= 0.4:   return "#d27d00"
    return "#a30000"

def compute_quarter_key(dt=None):
    dt = dt or datetime.utcnow()
    q = (dt.month - 1)//3 + 1
    return f"{dt.year}Q{q}"

def ensure_history_table(conn_):
    conn_.execute("""
        CREATE TABLE IF NOT EXISTS BACKTEST_HISTORY (
            run_ts TEXT NOT NULL,
            quarter_key TEXT NOT NULL,
            filtered_json TEXT NOT NULL,
            late_goal_json TEXT NOT NULL
        )
    """)
    conn_.commit()

def save_history_row(db_path, quarter_key, filtered_list, late_goal_list):
    conn_ = sqlite3.connect(db_path)
    try:
        ensure_history_table(conn_)
        payload_filtered = json.dumps(sorted(set(filtered_list)))
        payload_late = json.dumps(sorted(set(late_goal_list)))
        conn_.execute(
            "INSERT INTO BACKTEST_HISTORY (run_ts, quarter_key, filtered_json, late_goal_json) VALUES (?,?,?,?)",
            (datetime.utcnow().isoformat(), quarter_key, payload_filtered, payload_late)
        )
        conn_.commit()
    finally:
        conn_.close()

def load_previous_quarter_lists(db_path, current_qkey):
    conn_ = sqlite3.connect(db_path)
    try:
        ensure_history_table(conn_)
        cur = conn_.cursor()
        cur.execute("""
            SELECT quarter_key, filtered_json, late_goal_json
            FROM BACKTEST_HISTORY
            WHERE quarter_key <> ?
            ORDER BY run_ts DESC
            LIMIT 1
        """, (current_qkey,))
        row = cur.fetchone()
        if not row:
            return None, None, None
        prev_q, f_json, l_json = row
        return set(json.loads(f_json)), set(json.loads(l_json)), prev_q
    finally:
        conn_.close()

# 3) Load DB
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"Database file not found at: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME};", conn)
conn.close()
print(f"Loaded {len(df)} rows from {DB_PATH} (table {TABLE_NAME})")

# 4) Column detection & normalize
cols = list(df.columns)
def find_col(tokens):
    tl = [t.lower() for t in tokens]
    for c in cols:
        cl = c.lower()
        for t in tl:
            if t in cl:
                return c
    return None

league_col = find_col(["league","competition","div","tournament","cup"])
date_col   = find_col(["date","match_date","kickoff"])
home_col   = find_col(["home_team","home team","hometeam","home"])
away_col   = find_col(["away_team","away team","awayteam","away"])
fth_col    = find_col(["home_score","fthg","home_goals"])
fta_col    = find_col(["away_score","ftag","away_goals"])
g15_col    = find_col(["goals_15","goals15","1-15"])
g60_col    = find_col(["goals_60","goals60","1-60"])
odds_col_detected = ODDS_COL if ODDS_COL in df.columns else find_col(["odds","draw","betfair","odds betfair draw","odds_draw"])

rename_map = {}
if league_col: rename_map[league_col] = "league"
if date_col:   rename_map[date_col]   = "match_date"
if home_col:   rename_map[home_col]   = "home_team"
if away_col:   rename_map[away_col]   = "away_team"
if fth_col:    rename_map[fth_col]    = "ft_home"
if fta_col:    rename_map[fta_col]    = "ft_away"
if g15_col:    rename_map[g15_col]    = "goals_15_raw"
if g60_col:    rename_map[g60_col]    = "goals_60_raw"
if odds_col_detected: rename_map[odds_col_detected] = "odds_draw"

df = df.rename(columns=rename_map)

if "league" not in df.columns:
    raise KeyError("No league column detected.")
if "odds_draw" not in df.columns:
    raise KeyError(f"Odds column '{ODDS_COL}' not found and no alternative detected.")

# -----------------------------
# 5) Preprocess (dates/scores)
# -----------------------------
if "match_date" in df.columns:
    df["match_date_parsed"] = pd.to_datetime(df["match_date"], errors="coerce", dayfirst=True)
    before = len(df)
    df = df[df["match_date_parsed"] >= START_DATE].copy()
    print(f"Filtered to dates >= {START_DATE.date()}: {before} -> {len(df)}")
else:
    df["match_date_parsed"] = pd.NaT
    print("No match_date column detected; date-based features limited.")

# FT scores
df["ft_home"] = pd.to_numeric(df.get("ft_home"), errors="coerce")
df["ft_away"] = pd.to_numeric(df.get("ft_away"), errors="coerce")
df = df.dropna(subset=["league","home_team","away_team","ft_home","ft_away"]).copy()
df["ft_home"] = df["ft_home"].astype(int)
df["ft_away"] = df["ft_away"].astype(int)

# Decisive result
df["decisive"] = (df["ft_home"] != df["ft_away"]).astype(int)

# --- HT score parsing (needed for proxy) ---
if "ht_score" in df.columns:
    ht_parsed = df["ht_score"].apply(parse_ht_score_to_pair)
    df["ht_home"], df["ht_away"] = zip(*ht_parsed)
else:
    df["ht_home"], df["ht_away"] = np.nan, np.nan

df["is_00_at_ht"] = ((df["ht_home"] == 0) & (df["ht_away"] == 0))

# --- Band goals: we only need the 45-60 band for the proxy ---
# IMPORTANT: ensure this column name matches your v2 schema
# You referred to "goals_60" as a band (45-60). If your actual column is "goals_60_raw", use that.
band60_col = None
for c in ["goals_60", "goals_60_raw", "g60", "goals60"]:
    if c in df.columns:
        band60_col = c
        break

if band60_col:
    df["goals_in_45_60"] = df[band60_col].apply(to_int_safe)
else:
    df["goals_in_45_60"] = np.nan

# If goals_in_45_60 is NaN, we *cannot* safely assume 0 goals; treat as unknown (False)
df["no_goal_45_60"] = (df["goals_in_45_60"] == 0)

# --- LTD60 proxy: 0-0 at 60 if HT was 0-0 AND no goals in 45-60 band ---
df["is_00_at60_proxy"] = df["is_00_at_ht"] & df["no_goal_45_60"]

# If you want "draw at 60" for LTD60, this proxy implies draw (0-0) when true:
df["draw_at_60_proxy"] = df["is_00_at60_proxy"].astype(int)

# Keep odds parsing
df["odds_draw"] = pd.to_numeric(df.get("odds_draw"), errors="coerce")

# --- 0-15 band goals (v2 is a band count, not a scoreline) ---
band15_col = None
for c in ["goals_15", "goals_15_raw", "g15", "goals15"]:
    if c in df.columns:
        band15_col = c
        break

if band15_col:
    df["goals_in_0_15"] = df[band15_col].apply(to_int_safe)
else:
    df["goals_in_0_15"] = np.nan

# Maintain old column names so the rest of your script doesn't break
df["g15_home"] = np.nan
df["g15_away"] = np.nan

# Replace your old any_goal_0_15 definition with this:
df["any_goal_0_15"] = (df["goals_in_0_15"].fillna(0) > 0)

df["decisive"] = (df["ft_home"] != df["ft_away"]).astype(int)
df["any_goal_0_15"] = ((df["g15_home"].fillna(0) + df["g15_away"].fillna(0)) > 0)
df["is_00_at60"] = df["is_00_at60_proxy"]
df["draw_at_60"] = df["is_00_at60_proxy"]  # for LTD60 you only care about 0-0
df["lead_diff_60"] = 0  # not meaningful in v2 with band-only data


# 6) League summary + filtered leagues
league_summary = df.groupby("league").agg(
    matches=("league","size"),
    decisive_matches=("decisive","sum")
).reset_index()
league_summary["decisive_pct"] = league_summary["decisive_matches"] / league_summary["matches"]

leagues_filtered = league_summary[
    (league_summary["matches"] >= MIN_LEAGUE_MATCHES) &
    (league_summary["decisive_pct"] >= DECISIVE_THRESHOLD)
].copy().sort_values("decisive_pct", ascending=False).reset_index(drop=True)

df_filtered = df[df["league"].isin(leagues_filtered["league"])].copy()
print(f"Using {len(leagues_filtered)} filtered leagues; {len(df_filtered)} matches remain")

# 7) 0–0@60 per-league outcomes (league_outcome_df)
league_outcome_rows = []
for league, g in df_filtered[df_filtered["is_00_at60"]==True].groupby("league"):
    n = len(g)
    f00 = ((g["ft_home"]==0) & (g["ft_away"]==0)).sum()
    fdraw = ((g["ft_home"]==g["ft_away"]) & (g["ft_home"]>0)).sum()
    fdec = n - (f00 + fdraw)
    if n>0:
        league_outcome_rows.append({
            "league":league,
            "n":n,
            "p_00":f00/n,
            "p_other_draw":fdraw/n,
            "p_decisive":fdec/n
        })
league_outcome_df = pd.DataFrame(league_outcome_rows).sort_values("p_decisive", ascending=False)

# 8) CSV exports (single clean block)
filtered_csv = os.path.join(OUT_DIR, "filtered_leagues.csv")
leagues_filtered[["league","matches","decisive_matches","decisive_pct"]].to_csv(filtered_csv, index=False)

late_goal_cut = 0.65
if not league_outcome_df.empty:
    late_goal_df = league_outcome_df.loc[
        league_outcome_df["p_decisive"] >= late_goal_cut,
        ["league","n","p_00","p_other_draw","p_decisive"]
    ].copy()
    late_goal_df = late_goal_df.rename(columns={
        "n":"samples_00_at60",
        "p_decisive":"p_decisive_if_00_at_60"
    })
else:
    late_goal_df = pd.DataFrame(columns=["league","samples_00_at60","p_00","p_other_draw","p_decisive_if_00_at_60"])
late_goal_csv = os.path.join(OUT_DIR, "late_goal_leagues.csv")
late_goal_df.to_csv(late_goal_csv, index=False)

late_goal_leagues = late_goal_df["league"].tolist()

# 8b) Persist & quarterly compare
current_quarter = compute_quarter_key()
save_history_row(DB_PATH, current_quarter, leagues_filtered["league"].tolist(), late_goal_leagues)
prev_filtered_set, prev_late_set, prev_quarter = load_previous_quarter_lists(DB_PATH, current_quarter)

added_filtered = sorted(set(leagues_filtered["league"]) - (prev_filtered_set or set()))
removed_filtered = sorted((prev_filtered_set or set()) - set(leagues_filtered["league"]))
added_late = sorted(set(late_goal_leagues) - (prev_late_set or set()))
removed_late = sorted((prev_late_set or set()) - set(late_goal_leagues))

# 9) KPIs
subset_first15 = df_filtered[df_filtered["any_goal_0_15"] == True].copy()
p_decisive_given_first15 = subset_first15["decisive"].mean() if len(subset_first15)>0 else np.nan

def first15_scoring_team_win(row):
    gh, ga = row["g15_home"], row["g15_away"]
    if pd.isna(gh) and pd.isna(ga): return np.nan
    if gh>0 and (pd.isna(ga) or ga==0): return 1 if row["ft_home"] > row["ft_away"] else 0
    if ga>0 and (pd.isna(gh) or gh==0): return 1 if row["ft_away"] > row["ft_home"] else 0
    return np.nan
if subset_first15.empty:
    df["first15_team_wins"] = False  # or np.nan, whichever you prefer
else:
    subset_first15["first15_team_wins"] = subset_first15.apply(first15_scoring_team_win, axis=1)
    df.loc[subset_first15.index, "first15_team_wins"] = subset_first15["first15_team_wins"]

subset_first15["first15_team_wins"] = subset_first15.apply(first15_scoring_team_win, axis=1)
p_first15_team_wins = subset_first15["first15_team_wins"].dropna().mean()

subset_00_60 = df_filtered[df_filtered["is_00_at60"] == True].copy()
finish_00 = ((subset_00_60["ft_home"]==0) & (subset_00_60["ft_away"]==0)).sum()
finish_draw = ((subset_00_60["ft_home"]==subset_00_60["ft_away"]) & (subset_00_60["ft_home"]>0)).sum()
finish_decisive = len(subset_00_60) - (finish_00 + finish_draw)
p_finish_00_if_00_at60 = (finish_00 / len(subset_00_60)) if len(subset_00_60)>0 else np.nan

subset_draw60 = df_filtered[df_filtered["draw_at_60"] == True].copy()
p_finish_draw_if_draw60 = ((subset_draw60["ft_home"]==subset_draw60["ft_away"]).mean()) if len(subset_draw60)>0 else np.nan

subset_lead1 = df_filtered[df_filtered["lead_diff_60"].abs() == 1].copy()
subset_lead2 = df_filtered[df_filtered["lead_diff_60"].abs() >= 2].copy()

def leader_wins_flag(row):
    if pd.isna(row["lead_diff_60"]): return np.nan
    if row["lead_diff_60"] > 0: return 1 if row["ft_home"] > row["ft_away"] else 0
    if row["lead_diff_60"] < 0: return 1 if row["ft_away"] > row["ft_home"] else 0
    return np.nan

subset_lead1["leader_wins"] = subset_lead1.apply(leader_wins_flag, axis=1)
subset_lead2["leader_wins"] = subset_lead2.apply(leader_wins_flag, axis=1)
p_leader_wins_if_1 = subset_lead1["leader_wins"].mean() if len(subset_lead1)>0 else np.nan
p_leader_wins_if_2plus = subset_lead2["leader_wins"].mean() if len(subset_lead2)>0 else np.nan

# 10) Team/fixture stats
if not df_filtered.empty:
    home_stats = df_filtered.groupby("home_team").agg(home_matches=("home_team","size"), home_decisive=("decisive","sum")).reset_index().rename(columns={"home_team":"team"})
    away_stats = df_filtered.groupby("away_team").agg(away_matches=("away_team","size"), away_decisive=("decisive","sum")).reset_index().rename(columns={"away_team":"team"})
    team_stats = pd.merge(home_stats, away_stats, on="team", how="outer").fillna(0)
    team_stats["home_decisive_pct"] = team_stats["home_decisive"] / team_stats["home_matches"].replace(0, np.nan)
    team_stats["away_decisive_pct"] = team_stats["away_decisive"] / team_stats["away_matches"].replace(0, np.nan)
    team_stats["overall_decisive_pct"] = (
        (team_stats["home_decisive_pct"].fillna(0)*team_stats["home_matches"] +
         team_stats["away_decisive_pct"].fillna(0)*team_stats["away_matches"]) /
        (team_stats["home_matches"] + team_stats["away_matches"]).replace(0, np.nan)
    )
else:
    team_stats = pd.DataFrame()

fixture_stats = (
    df_filtered.groupby(["home_team","away_team"]).agg(matches=("league","size"), decisive_matches=("decisive","sum")).reset_index()
    if not df_filtered.empty else pd.DataFrame()
)
if not fixture_stats.empty:
    fixture_stats["fixture_decisive_pct"] = fixture_stats["decisive_matches"] / fixture_stats["matches"]

# 11) Charts (pie + stacked bar)
fig_pie, axp = plt.subplots(figsize=(5,5))
labels = ["Finished 0–0", "Other Draw", "Decisive"]
values = [finish_00, finish_draw, finish_decisive]
colors = ["#0072B2", "#56B4E9", "#E69F00"]
axp.pie(values, labels=labels, autopct="%1.1f%%", startangle=90, colors=colors)
axp.set_title("Outcome for matches 0–0 at 60 mins (filtered leagues)")
p_pie = save_fig(fig_pie, "outcome_00at60_pie.png")

if not league_outcome_df.empty:
    fig_bar, axb = plt.subplots(figsize=(14,6))
    x = league_outcome_df["league"]
    p00 = league_outcome_df["p_00"]
    pod = league_outcome_df["p_other_draw"]
    pdv = league_outcome_df["p_decisive"]
    axb.bar(x, p00, label="Finished 0–0")
    axb.bar(x, pod, bottom=p00, label="Other Draw")
    axb.bar(x, pdv, bottom=(p00+pod), label="Decisive")
    axb.set_xticklabels(x, rotation=35, ha="right", fontsize=9)
    axb.set_ylabel("Proportion")
    axb.set_title("Outcome breakdown of matches 0–0 at 60 by league (filtered)")
    axb.legend()
    p_bar = save_fig(fig_bar, "outcome_00at60_bar_by_league.png")
else:
    p_bar = None

# 12) LTD baseline backtest
df_filtered["matched"] = (~df_filtered["odds_draw"].isna()) & (df_filtered["odds_draw"] <= MAX_ODDS_ACCEPT)
df_filtered["liability"] = (df_filtered["odds_draw"] - 1.0) * STAKE

def compute_full_pnl(row):
    if not row["matched"]: return np.nan
    return STAKE if row["ft_home"] != row["ft_away"] else -row["liability"]

df_filtered["full_pnl"] = df_filtered.apply(compute_full_pnl, axis=1)

def compute_early_pnl(row):
    if not row["matched"]: return np.nan
    if row["is_00_at60"] or row["draw_at_60"]:
        if pd.isna(row["liability"]): return np.nan
        return -EARLY_DRAW_PCT * row["liability"]
    if not pd.isna(row["lead_diff_60"]) and abs(row["lead_diff_60"]) == 1:
        return EARLY_1GOAL_PROFIT
    return row["full_pnl"]

df_filtered["early_pnl"] = df_filtered.apply(compute_early_pnl, axis=1)

matched = df_filtered[df_filtered["matched"]==True].copy()
league_pnl = matched.groupby("league").agg(
    matches=("league","size"),
    wins=("decisive","sum"),
    draws=("decisive", lambda x: ((x==0).sum())),
    total_full_pnl=("full_pnl","sum"),
    avg_odds=("odds_draw","mean")
).reset_index()
league_pnl["roi_pct"] = (league_pnl["total_full_pnl"] / (league_pnl["matches"] * STAKE)) * 100.0

# 13) Baseline cumulative PnL
matched_seq = matched.copy()
if matched_seq["match_date_parsed"].isna().any():
    matched_seq = matched_seq.assign(_order=np.arange(len(matched_seq)))
    matched_seq = matched_seq.sort_values(["league", "match_date_parsed", "_order"])
else:
    matched_seq = matched_seq.sort_values(["league", "match_date_parsed"])

cum_list = []
for lg, g in matched_seq.groupby("league"):
    g = g.copy()
    g["pnl"] = np.where(g["ft_home"] != g["ft_away"], STAKE, -g["liability"])
    g["cum_pnl"] = g["pnl"].cumsum()
    g["seq"] = np.arange(1, len(g) + 1)
    cum_list.append(g[["league", "seq", "cum_pnl"]])
cum_df = pd.concat(cum_list, ignore_index=True) if cum_list else pd.DataFrame(columns=["league", "seq", "cum_pnl"])

overall_seq = matched_seq.copy().sort_values("match_date_parsed")[["ft_home","ft_away","liability","match_date_parsed","league"]].copy()
overall_seq["pnl"] = np.where(overall_seq["ft_home"] != overall_seq["ft_away"], STAKE, -overall_seq["liability"])
overall_seq["cum_pnl"] = overall_seq["pnl"].cumsum()
overall_seq["seq"] = np.arange(1, len(overall_seq) + 1)

# Save PnL CSVs
cum_df.rename(columns={"seq":"match_seq"}).to_csv(os.path.join(OUT_DIR, "cum_pnl_by_league.csv"), index=False)
overall_seq[["seq","cum_pnl"]].rename(columns={"seq":"match_seq"}).to_csv(os.path.join(OUT_DIR, "cum_pnl_overall.csv"), index=False)

# Debug prints (kept)
print("Matched bets:", len(matched))
print("Matched_seq rows:", len(matched_seq))
print("cum_df shape:", cum_df.shape)
print("overall_seq shape:", overall_seq.shape)
print("Example cum_df head:")
print(cum_df.head(10))
print("Example overall_seq head:")
print(overall_seq.head(10))

# 14) Progressive LTD sim (2nd entry @60′)
print("\n--- Running Progressive LTD Simulation ---")
late_goal_leagues = late_goal_df["league"].tolist()
print(f"Leagues eligible for 2nd entry (late goals): {len(late_goal_leagues)}")

SECOND_LAY_ODDS = 2.0
SECOND_STAKE = STAKE

df_filtered["second_entry"] = (
    df_filtered["draw_at_60"] &
    df_filtered["league"].isin(late_goal_leagues)
)
df_filtered["second_liability"] = np.where(
    df_filtered["second_entry"], (SECOND_LAY_ODDS - 1) * SECOND_STAKE, 0
)
df_filtered["second_pnl"] = np.where(
    df_filtered["second_entry"] & (df_filtered["ft_home"] != df_filtered["ft_away"]),
    SECOND_STAKE,
    np.where(df_filtered["second_entry"], -df_filtered["second_liability"], 0)
)
df_filtered["progressive_total_pnl"] = df_filtered["full_pnl"] + df_filtered["second_pnl"]

progressive_summary = (
    df_filtered[df_filtered["matched"]]
    .groupby("league")
    .agg(
        matches=("league", "size"),
        first_entry_pnl=("full_pnl", "sum"),
        second_entry_pnl=("second_pnl", "sum"),
        total_pnl=("progressive_total_pnl", "sum")
    )
    .reset_index()
)
progressive_summary["qualified_for_second"] = np.where(
    progressive_summary["league"].isin(late_goal_leagues), "Yes", "No"
)
progressive_summary["roi_pct"] = (progressive_summary["total_pnl"] / (progressive_summary["matches"] * STAKE)) * 100
progressive_summary.to_csv(os.path.join(OUT_DIR, "progressive_ltd_summary.csv"), index=False)

# 15) Progressive cumulative PnL
matched_prog = df_filtered[df_filtered["matched"]].copy()
matched_prog = matched_prog.sort_values(["league", "match_date_parsed"])
matched_prog["seq"] = np.arange(1, len(matched_prog) + 1)

cum_prog_rows = []
for lg, g in matched_prog.groupby("league"):
    g = g.copy()
    g["baseline_cum"] = g["full_pnl"].cumsum()
    g["progressive_cum"] = g["progressive_total_pnl"].cumsum()
    g["seq"] = np.arange(1, len(g) + 1)
    cum_prog_rows.append(g[["league", "seq", "baseline_cum", "progressive_cum"]])
cum_prog_df = pd.concat(cum_prog_rows, ignore_index=True) if cum_prog_rows else pd.DataFrame()

overall_prog = matched_prog.copy()
overall_prog["baseline_cum"] = overall_prog["full_pnl"].cumsum()
overall_prog["progressive_cum"] = overall_prog["progressive_total_pnl"].cumsum()
overall_prog["seq"] = np.arange(1, len(overall_prog) + 1)

# 16) Perf summary
def max_drawdown(series):
    if series.empty: return 0
    running_max = series.cummax()
    dd = series - running_max
    return dd.min()

def longest_streak(pnls, win=True):
    if pnls.empty: return 0
    seq = (pnls > 0) if win else (pnls <= 0)
    return seq.groupby((~seq).cumsum()).transform("size").max() if not seq.empty else 0

total_matches = len(matched_prog)
baseline_pnl = matched_prog["full_pnl"].sum()
progressive_pnl = matched_prog["progressive_total_pnl"].sum()
improvement = progressive_pnl - baseline_pnl
max_dd = max_drawdown(overall_prog["progressive_cum"])
longest_win = longest_streak(matched_prog["progressive_total_pnl"], win=True)
longest_loss = longest_streak(matched_prog["progressive_total_pnl"], win=False)

# 17) Build HTML
html = []
html.append("<!doctype html><html><head><meta charset='utf-8'><title>Football Analysis + LTD Backtest</title>")
html.append("<link rel='stylesheet' href='https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css'>")
html.append("<style>body{font-family:Arial,Helvetica,sans-serif;margin:18px;} h1{font-size:28px;} .kpi-row{display:flex;flex-wrap:wrap;gap:14px;margin-bottom:20px;} .kpi{flex:0 0 32%;padding:16px;border-radius:8px;color:#fff;} .kpi h3{margin:0;font-size:14px;color:#fff;} .kpi .val{font-size:28px;font-weight:700;margin-top:8px;} table{border-collapse:collapse;} th,td{padding:6px;border:1px solid #ddd;text-align:left;}</style>")
html.append("<script src='https://code.jquery.com/jquery-3.6.0.min.js'></script>")
html.append("<script src='https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js'></script>")
html.append("<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>")
html.append("</head><body>")

html.append(f"<h1>Football Analysis Report + LTD Backtest — Filtered Leagues (>= {MIN_LEAGUE_MATCHES} matches & >= {int(DECISIVE_THRESHOLD*100)}% decisive)</h1>")
html.append(f"<p>Generated: {datetime.utcnow().isoformat()} UTC</p>")

metrics = [
    ("P(decisive | goal 0–15)", p_decisive_given_first15),
    ("P(scoring team in 0–15 wins)", p_first15_team_wins),
    ("P(finish 0–0 | 0–0 @ 60)", p_finish_00_if_00_at60),
    ("P(finish draw | draw @ 60)", p_finish_draw_if_draw60),
    ("P(win | 1-goal lead @ 60)", p_leader_wins_if_1),
    ("P(win | 2+ lead @ 60)", p_leader_wins_if_2plus)
]
html.append("<div class='kpi-row'>")
for title, val in metrics:
    color = kpi_color(val)
    val_txt = "n/a" if pd.isna(val) else f"{val:.4f}"
    html.append(f"<div class='kpi' style='background:{color}'><h3>{title}</h3><div class='val'>{val_txt}</div></div>")
html.append("</div>")

fig1, ax1 = plt.subplots(figsize=(14,6))
ax1.bar(leagues_filtered["league"], leagues_filtered["decisive_pct"], color="teal")
ax1.set_xticklabels(leagues_filtered["league"], rotation=40, ha="right", fontsize=9)
ax1.set_title("League decisive % (≥75% decisive & ≥10 matches)")
ax1.set_ylabel("Decisive %")
p1 = save_fig(fig1, "league_decisive_filtered.png")

html.append("<h2>League decisive % (filtered leagues)</h2>")
html.append(df_to_html_with_id(leagues_filtered.sort_values("decisive_pct", ascending=False), "league_table"))
html.append(f"<img src='league_decisive_filtered.png' style='max-width:100%;margin-top:10px;'>")

# CSV download links
html.append("<p style='margin-top:10px;'>"
            f"<a href='{os.path.basename(filtered_csv)}' download>Download filtered_leagues.csv</a> &nbsp;|&nbsp; "
            f"<a href='{os.path.basename(late_goal_csv)}' download>Download late_goal_leagues.csv</a>"
            "</p>")

# Quarterly changes
html.append("<h3>Quarterly League Changes (vs previous backtest)</h3>")
if prev_quarter is None:
    html.append("<p><em>No comparison available (first recorded quarter in BACKTEST_HISTORY).</em></p>")
else:
    html.append(f"<p>Compared against previous quarter: <strong>{prev_quarter}</strong></p>")

    df_added_f = pd.DataFrame({"Added (filtered)": added_filtered})
    df_removed_f = pd.DataFrame({"Removed (filtered)": removed_filtered})
    df_added_l = pd.DataFrame({"Added (late-goal)": added_late})
    df_removed_l = pd.DataFrame({"Removed (late-goal)": removed_late})

    html.append(df_to_html_with_id(df_added_f, "filtered_added_tbl"))
    html.append(df_to_html_with_id(df_removed_f, "filtered_removed_tbl"))
    html.append(df_to_html_with_id(df_added_l, "lategoal_added_tbl"))
    html.append(df_to_html_with_id(df_removed_l, "lategoal_removed_tbl"))

# 0-0 at 60: pie + stacked bar
html.append("<h2>0–0 at 60 — Outcome breakdown (filtered leagues)</h2>")
html.append(f"<p>From {len(subset_00_60)} matches that were 0–0 at 60 (filtered leagues): Finished 0–0: {finish_00}, Other Draw: {finish_draw}, Decisive: {finish_decisive}.</p>")
html.append(f"<img src='{os.path.basename(p_pie)}' style='max-width:350px;margin-right:20px;'>")
if p_bar:
    html.append(f"<img src='{os.path.basename(p_bar)}' style='max-width:100%;'>")

# Team/fixture tables
html.append("<h2>Team decisive % (filtered leagues)</h2>")
html.append(df_to_html_with_id(team_stats.sort_values("overall_decisive_pct", ascending=False).head(200), "team_table"))

html.append("<h2>Top fixture decisive % (filtered leagues, min 3 matches)</h2>")
if not fixture_stats.empty:
    html.append(df_to_html_with_id(fixture_stats[fixture_stats["matches"]>=3].sort_values("fixture_decisive_pct", ascending=False).head(200), "fixture_table"))
else:
    html.append("<p><em>No fixture stats available</em></p>")

# LTD PnL section
html.append("<h2>Lay-the-Draw (LTD) — PnL Analysis (matched bets only)</h2>")
html.append("<p>Only matched bets where draw odds ≤ 4.0 are included. Stake £100.</p>")

html.append("<h3>League PnL Summary</h3>")
html.append(df_to_html_with_id(league_pnl.sort_values("total_full_pnl", ascending=False), "league_pnl_table"))

# Plotly Baseline PnL (dropdown)
plot_div = ""
if not cum_df.empty or not overall_seq.empty:
    fig = go.Figure()
    if not overall_seq.empty:
        fig.add_trace(go.Scatter(
            x=overall_seq["seq"].astype(int),
            y=overall_seq["cum_pnl"].astype(float),
            name="All leagues (filtered)",
            mode="lines+markers",
            visible=True
        ))
    leagues = sorted(cum_df["league"].unique()) if not cum_df.empty else []
    base = 1 if not overall_seq.empty else 0
    for idx, lg in enumerate(leagues):
        tmp = cum_df[cum_df["league"] == lg].sort_values("seq")
        fig.add_trace(go.Scatter(
            x=tmp["seq"].astype(int),
            y=tmp["cum_pnl"].astype(float),
            name=lg,
            mode="lines+markers",
            visible=False
        ))
    n_traces = len(fig.data)
    buttons = []
    vis_all = [False] * n_traces
    if n_traces > 0:
        vis_all[0] = True
    buttons.append(dict(
        label="All",
        method="update",
        args=[{"visible": vis_all},
              {"title": "Cumulative PnL — All (filtered, match sequence)",
               "yaxis": {"autorange": True}}]
    ))
    for i, lg in enumerate(leagues):
        vis = [False] * n_traces
        li = base + i
        vis[li] = True
        buttons.append(dict(
            label=lg,
            method="update",
            args=[{"visible": vis},
                  {"title": f"Cumulative PnL — {lg} (match sequence)",
                   "yaxis": {"autorange": True}}]
        ))
    fig.update_layout(
        updatemenus=[dict(active=0, buttons=buttons, x=0.0, y=1.15, xanchor='left')],
        title="Cumulative PnL — All (filtered, match sequence)",
        xaxis_title="Match Sequence",
        yaxis_title="Cumulative PnL (£)",
        height=520,
        margin=dict(l=60, r=40, t=80, b=80),
        yaxis=dict(autorange=True)
    )
    plot_div = plotly_plot(fig, include_plotlyjs=True, output_type="div")
else:
    plot_div = "<p><em>No PnL data to plot.</em></p>"

html.append("<h3>Interactive cumulative PnL over time</h3>")
html.append("<p>Use the dropdown above the chart to select 'All' or a specific league.</p>")
html.append(plot_div if plot_div else "<p><em>No matched bets with dates available to plot.</em></p>")

# Progressive part
fig_prog = go.Figure()
fig_prog.add_trace(go.Scatter(
    x=overall_prog["seq"], y=overall_prog["baseline_cum"],
    name="Baseline LTD — All", mode="lines"
))
fig_prog.add_trace(go.Scatter(
    x=overall_prog["seq"], y=overall_prog["progressive_cum"],
    name="Progressive LTD — All", mode="lines"
))
for lg in sorted(cum_prog_df["league"].unique()):
    tmp = cum_prog_df[cum_prog_df["league"] == lg].sort_values("seq")
    fig_prog.add_trace(go.Scatter(x=tmp["seq"], y=tmp["baseline_cum"], name=f"Baseline — {lg}", mode="lines", visible=False))
    fig_prog.add_trace(go.Scatter(x=tmp["seq"], y=tmp["progressive_cum"], name=f"Progressive — {lg}", mode="lines", visible=False))

buttons = []
visible_all = [True, True] + [False]*(len(fig_prog.data)-2)
buttons.append(dict(
    label="All Leagues",
    method="update",
    args=[{"visible": visible_all},
          {"title": "Progressive vs Baseline LTD — All Filtered Leagues",
           "yaxis": {"autorange": True}}]
))
for i, lg in enumerate(sorted(cum_prog_df["league"].unique())):
    vis = [False]*len(fig_prog.data)
    vis[2+(2*i)] = True
    vis[2+(2*i)+1] = True
    buttons.append(dict(
        label=lg,
        method="update",
        args=[{"visible": vis},
              {"title": f"Progressive vs Baseline LTD — {lg}",
               "yaxis": {"autorange": True}}]
    ))
fig_prog.update_layout(
    updatemenus=[dict(active=0, buttons=buttons, x=0.0, y=1.15, xanchor='left')],
    title="Progressive vs Baseline LTD — All Filtered Leagues",
    xaxis_title="Match Sequence",
    yaxis_title="Cumulative PnL (£)",
    height=520, margin=dict(l=60, r=40, t=80, b=80),
    yaxis=dict(autorange=True),
    legend=dict(x=0, y=1.15, orientation="h")
)
progressive_plot_div = plotly_plot(fig_prog, include_plotlyjs=False, output_type="div")

html.append("<h2>Progressive LTD Strategy — Two-Stage Entry</h2>")
html.append("<p>Filtered leagues only (≥75% decisive). Second lay entry at 60′ if still drawing, "
            "only in leagues where P(decisive | 0–0 @ 60) ≥ 65%. £100 stake per entry.</p>")

html.append("<h3>Performance Summary</h3>")
html.append("<table border='1' cellpadding='6'>")
html.append(f"<tr><td>Total Matches</td><td>{total_matches}</td></tr>")
html.append(f"<tr><td>Total Baseline PnL (£)</td><td>£{baseline_pnl:.2f}</td></tr>")
html.append(f"<tr><td>Total Progressive PnL (£)</td><td>£{progressive_pnl:.2f}</td></tr>")
html.append(f"<tr><td>Improvement (£)</td><td>£{improvement:.2f}</td></tr>")
html.append(f"<tr><td>Max Drawdown (£)</td><td>£{max_dd:.2f}</td></tr>")
html.append(f"<tr><td>Longest Winning Streak</td><td>{longest_win}</td></tr>")
html.append(f"<tr><td>Longest Losing Streak</td><td>{longest_loss}</td></tr>")
html.append("</table>")

summary_tbl = progressive_summary[["league","qualified_for_second","matches","first_entry_pnl","second_entry_pnl","total_pnl","roi_pct"]].copy()
summary_tbl["qualified_for_second"] = summary_tbl["qualified_for_second"].map(lambda x: f"<span style='color:{'#1a7f37' if x=='Yes' else '#a30000'}'>{x}</span>")
html.append(df_to_html_with_id(summary_tbl.sort_values("total_pnl", ascending=False), "progressive_summary_table"))

html.append("<h3>Interactive Cumulative PnL — Progressive vs Baseline</h3>")
html.append("<p>Use the dropdown above the chart to view 'All Leagues' or any specific league.</p>")
html.append(progressive_plot_div)

# State PnL comparisons
state_summaries = {}
for state_name, mask in [("0-0@60", df_filtered["is_00_at60"]),
                         ("draw@60", df_filtered["draw_at_60"]),
                         ("1-goal@60", df_filtered["lead_diff_60"].abs()==1)]:
    subset = df_filtered[mask & df_filtered["matched"]==True].copy()
    n = len(subset)
    total_full = subset["full_pnl"].sum() if n>0 else 0.0
    total_early = subset["early_pnl"].sum() if n>0 else 0.0
    better = "early" if total_early > total_full else ("full-time" if total_full > total_early else "equal")
    state_summaries[state_name] = {"matches": n, "full_pnl": total_full, "early_pnl": total_early, "better": better}

html.append("<h3>60-minute state PnL comparison — overall (matched bets)</h3>")
html.append("<table border='1' cellpadding='6'><tr><th>State</th><th>Matches</th><th>Full-time PnL (£)</th><th>Early-exit PnL (£)</th><th>Better Option</th></tr>")
for state, info in state_summaries.items():
    html.append(f"<tr><td>{state}</td><td>{info['matches']}</td><td>£{info['full_pnl']:.2f}</td><td>£{info['early_pnl']:.2f}</td><td>{info['better']}</td></tr>")
html.append("</table>")

# Per-league breakdowns
perleague_00 = pd.DataFrame([
    {"league": lg, "matches": len(g), "full_pnl": g["full_pnl"].sum(), "early_pnl": g["early_pnl"].sum(),
     "better": ("early" if g["early_pnl"].sum() > g["full_pnl"].sum() else ("full-time" if g["full_pnl"].sum() > g["early_pnl"].sum() else "equal"))}
    for lg, g in df_filtered[df_filtered["is_00_at60"] & df_filtered["matched"]].groupby("league")
]).sort_values("matches", ascending=False)

perleague_draw = pd.DataFrame([
    {"league": lg, "matches": len(g), "full_pnl": g["full_pnl"].sum(), "early_pnl": g["early_pnl"].sum(),
     "better": ("early" if g["early_pnl"].sum() > g["full_pnl"].sum() else ("full-time" if g["full_pnl"].sum() > g["early_pnl"].sum() else "equal"))}
    for lg, g in df_filtered[df_filtered["draw_at_60"] & df_filtered["matched"]].groupby("league")
]).sort_values("matches", ascending=False)

perleague_lead1 = pd.DataFrame([
    {"league": lg, "matches": len(g), "full_pnl": g["full_pnl"].sum(), "early_pnl": g["early_pnl"].sum(),
     "better": ("early" if g["early_pnl"].sum() > g["full_pnl"].sum() else ("full-time" if g["full_pnl"].sum() > g["early_pnl"].sum() else "equal"))}
    for lg, g in df_filtered[(df_filtered["lead_diff_60"].abs()==1) & df_filtered["matched"]].groupby("league")
]).sort_values("matches", ascending=False)

html.append("<h3>Per-league breakdown: 0–0@60 (matched)</h3>")
html.append(df_to_html_with_id(perleague_00, "perleague_00"))
html.append("<h3>Per-league breakdown: draw@60 (matched)</h3>")
html.append(df_to_html_with_id(perleague_draw, "perleague_draw"))
html.append("<h3>Per-league breakdown: 1-goal@60 (matched)</h3>")
html.append(df_to_html_with_id(perleague_lead1, "perleague_lead1"))

# Footer
html.append("<h2>Notes & caveats</h2>")
html.append("<ul>")
html.append("<li>Metrics computed on filtered leagues (≥10 matches and ≥75% decisive).</li>")
html.append("<li>LTD matched bets require draw odds ≤ 4.0.</li>")
html.append("<li>Early-exit assumptions: 50% liability reduction for draw/0-0; +£50 for 1-goal lead.</li>")
html.append("<li>CSVs and charts saved to the output folder.</li>")
html.append("</ul>")

# DataTables init (single block)
html.append("<script>$(document).ready(function(){ "
            "$('#league_table').DataTable({pageLength:10}); "
            "$('#team_table').DataTable({pageLength:10}); "
            "$('#fixture_table').DataTable({pageLength:10}); "
            "$('#league_pnl_table').DataTable({pageLength:10}); "
            "$('#perleague_00').DataTable({pageLength:10}); "
            "$('#perleague_draw').DataTable({pageLength:10}); "
            "$('#perleague_lead1').DataTable({pageLength:10}); "
            "$('#filtered_added_tbl').DataTable({paging:false, searching:false, info:false}); "
            "$('#filtered_removed_tbl').DataTable({paging:false, searching:false, info:false}); "
            "$('#lategoal_added_tbl').DataTable({paging:false, searching:false, info:false}); "
            "$('#lategoal_removed_tbl').DataTable({paging:false, searching:false, info:false}); "
            "});</script>")

html.append("</body></html>")

# 18) Write files
with open(REPORT_HTML, "w", encoding="utf-8") as fh:
    fh.write("\n".join(html))

print("Report written to:", REPORT_HTML)
print("CSV and images saved to:", OUT_DIR)
print("Filtered leagues CSV:", filtered_csv)
print("Late-goal leagues CSV:", late_goal_csv)
