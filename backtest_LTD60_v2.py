# Backtest_LTD60_v2_clean.py
import os
import json
import sqlite3
from datetime import datetime, timezone

import numpy as np
import pandas as pd


# ===================== USER RULES / CONSTANTS =====================
DB_PATH = r"C:\Users\Sam\FootballTrader v0.3.3\database\autotrader_data.db"
OUT_DIR = r"C:\Users\Sam\FootballTrader v0.3.3\data_analysis"
TABLE_NAME = "archive_v2"

START_DATE = pd.Timestamp("2025-07-01")
MIN_LEAGUE_MATCHES = 10
DECISIVE_THRESHOLD = 0.75

ODDS_COL = "Odds Betfair Draw"
STAKE = 100.0
MAX_ODDS_ACCEPT = 4.0

# Late scoring league rule:
LATE_GOAL_THRESHOLD = 0.7
MIN_00AT60_MATCHES = 5   # minimum number of 0-0@60 samples per league to qualify for late-goal calc


# ===================== HELPERS =====================
def ensure_outdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def quarter_key(ts: pd.Timestamp) -> str:
    q = ((ts.month - 1) // 3) + 1
    return f"{ts.year}Q{q}"


def parse_scoreline(x):
    """
    Parses score strings like:
      "0 - 0", "1-2", " 3 -1 ", None
    Returns: (home:int, away:int) or (None, None)
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None, None
    s = str(x).strip()
    if not s:
        return None, None
    # Normalize separators
    s = s.replace("–", "-").replace("—", "-")
    if "-" not in s:
        return None, None
    left, right = s.split("-", 1)
    try:
        h = int(left.strip())
        a = int(right.strip())
        return h, a
    except Exception:
        return None, None


def load_archive_v2(db_path: str, table: str) -> pd.DataFrame:
    con = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)
    finally:
        con.close()
    return df


def write_backtest_history(db_path: str, run_ts_iso: str, qkey: str,
                           filtered_leagues: list, late_goal_leagues: list) -> None:
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS BACKTEST_HISTORY (
                run_ts        TEXT NOT NULL,
                quarter_key   TEXT NOT NULL,
                filtered_json TEXT NOT NULL,
                late_goal_json TEXT NOT NULL
            )
        """)
        cur.execute(
            "INSERT INTO BACKTEST_HISTORY (run_ts, quarter_key, filtered_json, late_goal_json) VALUES (?,?,?,?)",
            (run_ts_iso, qkey, json.dumps(filtered_leagues), json.dumps(late_goal_leagues))
        )
        con.commit()
    finally:
        con.close()

import os
import pandas as pd


def export_cumulative_pnl(
    df_filt: pd.DataFrame,
    out_dir: str,
    date_col_candidates=("match_date_parsed", "kickoff", "match_date"),
    league_col="comp",
    pnl_ltd_col="pnl_entry1",   # LTD only
    pnl_ltd60_col="pnl_total",  # LTD + second entry at 60'
):
    """
    Builds running cumulative PnL:
      - overall (all matches in time order)
      - per league (each league independently, time order within league)

    Writes:
      - cum_pnl_overall.csv
      - cum_pnl_by_league.csv
    """

    os.makedirs(out_dir, exist_ok=True)

    # ---- pick the best available date column ----
    date_col = None
    for c in date_col_candidates:
        if c in df_filt.columns:
            date_col = c
            break
    if date_col is None:
        raise ValueError(f"No usable date column found. Tried: {date_col_candidates}")

    df = df_filt.copy()

    # ---- normalize date column to datetime ----
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True)

    # Drop rows without a date (cannot be ordered for running pnl)
    df = df.dropna(subset=[date_col]).copy()

    # ---- ensure required columns exist ----
    for col, default in {
        league_col: "UNKNOWN",
        pnl_ltd_col: 0.0,
        pnl_ltd60_col: 0.0,
    }.items():
        if col not in df.columns:
            df[col] = default

    df[pnl_ltd_col] = pd.to_numeric(df[pnl_ltd_col], errors="coerce").fillna(0.0)
    df[pnl_ltd60_col] = pd.to_numeric(df[pnl_ltd60_col], errors="coerce").fillna(0.0)

    # Optional: stable tie-breaker if you have an event_id or event_name
    tie_cols = []
    for c in ("event_id", "event_name", "Home", "Away"):
        if c in df.columns:
            tie_cols.append(c)

    # =========================
    # 1) Overall cumulative pnl
    # =========================
    sort_cols = [date_col] + tie_cols
    overall = df.sort_values(sort_cols).reset_index(drop=True)

    overall["match_n"] = overall.index + 1
    overall["cum_pnl_ltd"] = overall[pnl_ltd_col].cumsum()
    overall["cum_pnl_ltd60"] = overall[pnl_ltd60_col].cumsum()

    overall_out_cols = [date_col, "match_n", league_col, pnl_ltd_col, pnl_ltd60_col, "cum_pnl_ltd", "cum_pnl_ltd60"]
    overall_out_cols += [c for c in ("event_id", "event_name") if c in overall.columns]
    overall[overall_out_cols].to_csv(os.path.join(out_dir, "cum_pnl_overall.csv"), index=False)

    # =========================
    # 2) Per-league cumulative pnl
    # =========================
    by_league = overall.copy()
    by_league["league_match_n"] = by_league.groupby(league_col).cumcount() + 1
    by_league["cum_pnl_ltd_league"] = by_league.groupby(league_col)[pnl_ltd_col].cumsum()
    by_league["cum_pnl_ltd60_league"] = by_league.groupby(league_col)[pnl_ltd60_col].cumsum()

    league_out_cols = [
        league_col, date_col, "league_match_n",
        pnl_ltd_col, pnl_ltd60_col,
        "cum_pnl_ltd_league", "cum_pnl_ltd60_league",
    ]
    league_out_cols += [c for c in ("event_id", "event_name") if c in by_league.columns]
    by_league[league_out_cols].to_csv(os.path.join(out_dir, "cum_pnl_by_league.csv"), index=False)

    return overall, by_league



# ===================== MAIN =====================
def main():
    ensure_outdir(OUT_DIR)

    run_ts = pd.Timestamp.now(tz="UTC")
    run_ts_iso = run_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    qkey = quarter_key(run_ts)

    df = load_archive_v2(DB_PATH, TABLE_NAME)

    # --- Required columns check ---
    required = ["League", "ft_score", "ht_score", "goals_60", ODDS_COL]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns in {TABLE_NAME}: {missing}")

    # --- Date filtering (optional but per your rule) ---
    # Your v2 export sometimes uses match_date / start_date variants; try best-effort.
    date_col = None
    for cand in ["match_date", "match_date_parsed", "start_date", "kickoff", "date"]:
        if cand in df.columns:
            date_col = cand
            break

    if date_col:
        # Parse leniently
        df["_date_parsed"] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True, utc=False)
        before = len(df)
        df = df[df["_date_parsed"] >= START_DATE].copy()
        print(f"Filtered to dates >= {START_DATE.date()}: {before} -> {len(df)}")
    else:
        print("No usable date column found; skipping START_DATE filter.")

    # --- Parse FT / HT scorelines ---
    ft_pairs = df["ft_score"].apply(parse_scoreline)
    df["ft_home"], df["ft_away"] = zip(*ft_pairs)

    ht_pairs = df["ht_score"].apply(parse_scoreline)
    df["ht_home"], df["ht_away"] = zip(*ht_pairs)

    # Drop rows without FT scores (can’t determine decisive)
    before = len(df)
    df = df.dropna(subset=["comp", "ft_home", "ft_away"]).copy()
    df["ft_home"] = df["ft_home"].astype(int)
    df["ft_away"] = df["ft_away"].astype(int)
    print(f"Dropped rows without parsable FT score: {before} -> {len(df)}")

    # --- Decisive flag from FT ---
    df["decisive"] = (df["ft_home"] != df["ft_away"]).astype(int)
    

    # --- Filtered leagues: decisive rate >= 75% with >=10 matches ---
    league_stats = (
        df.groupby("comp")
          .agg(matches=("decisive", "size"), decisive_rate=("decisive", "mean"))
          .reset_index()
    )
    print(league_stats)
    filtered = league_stats[
        (league_stats["matches"] >= MIN_LEAGUE_MATCHES) &
        (league_stats["decisive_rate"] >= DECISIVE_THRESHOLD)
    ].copy()

    filtered_leagues = filtered["comp"].sort_values().tolist()
    print(f"Filtered leagues: {len(filtered_leagues)}")

    # Save filtered leagues CSV
    filtered_out = filtered.sort_values(["decisive_rate", "matches"], ascending=[False, False])
    filtered_out.to_csv(os.path.join(OUT_DIR, "filtered_leagues_v2.csv"), index=False)

    # --- Late-goal leagues ---
    # Rule: 0-0 at HT AND goals_60 == 0 => assume 0-0 at 60
    df["goals_60_num"] = pd.to_numeric(df["goals_60"], errors="coerce").fillna(0).astype(int)
    df["is_00_ht"] = (df["ht_home"] == 0) & (df["ht_away"] == 0)
    df["is_00_at60"] = df["is_00_ht"] & (df["goals_60_num"] == 0)

    df_filt = df[df["comp"].isin(filtered_leagues)].copy()

    late_pool = df_filt[df_filt["is_00_at60"]].copy()
    late_stats = (
        late_pool.groupby("comp")
                 .agg(samples=("decisive", "size"), decisive_rate_from_00at60=("decisive", "mean"))
                 .reset_index()
    )
    late_stats = late_stats[late_stats["samples"] >= MIN_00AT60_MATCHES].copy()

    late_goal = late_stats[late_stats["decisive_rate_from_00at60"] >= LATE_GOAL_THRESHOLD].copy()
    late_goal_leagues = late_goal["comp"].sort_values().tolist()
    print(f"Late-goal leagues: {len(late_goal_leagues)}")

    late_goal.sort_values(["decisive_rate_from_00at60", "samples"], ascending=[False, False]) \
             .to_csv(os.path.join(OUT_DIR, "late_goal_leagues_v2.csv"), index=False)

    # --- PnL logic (basic LTD, pre-match entry) ---
    df_filt[ODDS_COL] = pd.to_numeric(df_filt[ODDS_COL], errors="coerce")
    df_filt["bet_ok"] = df_filt[ODDS_COL].notna() & (df_filt[ODDS_COL] <= MAX_ODDS_ACCEPT)

    # -----------------------------
    # 1) Build bet flags + pnls FIRST
    # -----------------------------

    # Lay the draw profit model:
    #  - decisive => +STAKE
    #  - draw     => -STAKE*(odds-1)
    def lay_draw_pnl(decisive: int, odds: float) -> float:
        if pd.isna(odds):
            return 0.0
        if int(decisive) == 1:
            return float(STAKE)
        return float(-STAKE * (float(odds) - 1.0))

    # Entry 1
    df_filt["pnl_entry1"] = np.where(
        df_filt["bet_ok"],
        df_filt.apply(lambda r: lay_draw_pnl(r["decisive"], r[ODDS_COL]), axis=1),
        0.0
    )

    # Entry 2: only if league is late-goal league AND 0-0 at 60
    df_filt["entry2_ok"] = df_filt["bet_ok"] & df_filt["comp"].isin(late_goal_leagues) & df_filt["is_00_at60"]

    df_filt["pnl_entry2"] = np.where(
        df_filt["entry2_ok"],
        df_filt.apply(lambda r: lay_draw_pnl(r["decisive"], r[ODDS_COL]), axis=1),
        0.0
    )

    df_filt["pnl_total"] = df_filt["pnl_entry1"] + df_filt["pnl_entry2"]


    # -----------------------------
    # 2) Defensive: ensure columns exist (prevents KeyError)
    # -----------------------------
    for col, default in {
        "entry2_ok": False,
        "pnl_entry1": 0.0,
        "pnl_entry2": 0.0,
        "pnl_total": 0.0,
    }.items():
        if col not in df_filt.columns:
            df_filt[col] = default


    # -----------------------------
    # 3) Now build league summary
    # -----------------------------
    at60 = (
        df_filt[df_filt["is_00_at60"]]
        .groupby("comp")
        .agg(
            _00at60_samples=("decisive", "size"),
            _00at60_decisive_rate=("decisive", "mean"),
        )
        .reset_index()
    )

    league_pnl = (
        df_filt.groupby("comp")
            .agg(
                matches=("comp", "size"),
                decisive_rate=("decisive", "mean"),

                entry1_count=("bet_ok", "sum"),
                entry2_count=("entry2_ok", "sum"),

                pnl_entry1_sum=("pnl_entry1", "sum"),
                pnl_entry2_sum=("pnl_entry2", "sum"),
                pnl_total_sum=("pnl_total", "sum"),

                pnl_entry1_avg=("pnl_entry1", "mean"),
                pnl_entry2_avg=("pnl_entry2", "mean"),
                pnl_total_avg=("pnl_total", "mean"),
            )
            .reset_index()
    )

    entry1_onbets = (
        df_filt[df_filt["bet_ok"]]
        .groupby("comp")["pnl_entry1"]
        .mean()
        .rename("pnl_entry1_avg_on_bets")
        .reset_index()
    )

    entry2_onbets = (
        df_filt[df_filt["entry2_ok"]]
        .groupby("comp")["pnl_entry2"]
        .mean()
        .rename("pnl_entry2_avg_on_bets")
        .reset_index()
    )

    league_pnl = (
        league_pnl
            .merge(at60, on="comp", how="left")
            .merge(entry1_onbets, on="comp", how="left")
            .merge(entry2_onbets, on="comp", how="left")
            .rename(columns={
                "_00at60_samples": "00at60_samples",
                "_00at60_decisive_rate": "00at60_decisive_rate",
            })
    )

    league_pnl["00at60_samples"] = league_pnl["00at60_samples"].fillna(0).astype(int)
    league_pnl = league_pnl.sort_values(["pnl_total_sum"], ascending=False)
    league_pnl.to_csv(os.path.join(OUT_DIR, "league_pnl_summary_v2.csv"), index=False)

    export_cumulative_pnl(
    df_filt=df_filt,
    out_dir=OUT_DIR,
    date_col_candidates=("_date_parsed",),  # use your parsed date column first
    league_col="comp",
    pnl_ltd_col="pnl_entry1",
    pnl_ltd60_col="pnl_total",
    )


    # --- Save run metadata into BACKTEST_HISTORY ---
    write_backtest_history(DB_PATH, run_ts_iso, qkey, filtered_leagues, late_goal_leagues)

    print("Done.")
    print(f"Outputs written to: {OUT_DIR}")
    print(" - filtered_leagues_v2.csv")
    print(" - late_goal_leagues_v2.csv")
    print(" - league_pnl_summary_v2.csv")
    print("BACKTEST_HISTORY row inserted.")


if __name__ == "__main__":
    main()
