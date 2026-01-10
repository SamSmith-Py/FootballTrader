# Backtest_LTD60_v3.py
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from core.settings import (
    BACKTEST_DIR,
    BACKTEST_DECISIVE_COMP_MIN,
    BACKTEST_LATE_GOAL_COMP_MIN,
    BACKTEST_MAX_PRICE_ENTRY_2,
    BACKTEST_MAX_PRICE,
    BACKTEST_STAKE,
    FILTERED_LEAGUES_CSV_V3,
    LATE_GOAL_LEAGUES_CSV_V3,
    MATCHES_MIN_PLAYED,
)
import update_ltd60_backtest_report_v3


# ===================== USER RULES / CONSTANTS =====================
DB_PATH = r"C:\Users\Sam\FootballTrader v0.3.3\database\autotrader_data.db"
OUT_DIR = str(BACKTEST_DIR)
TABLES = ["archive_v2", "archive_v3"]


# ===================== HELPERS =====================
def ensure_outdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def quarter_key(ts: datetime) -> str:
    q = ((ts.month - 1) // 3) + 1
    return f"{ts.year}Q{q}"


def write_backtest_history(
    db_path: str,
    run_ts_iso: str,
    qkey: str,
    filtered_leagues: list,
    late_goal_leagues: list,
) -> None:
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS BACKTEST_HISTORY (
                run_ts        TEXT NOT NULL,
                quarter_key   TEXT NOT NULL,
                filtered_json TEXT NOT NULL,
                late_goal_json TEXT NOT NULL
            )
            """
        )
        # Keep only one backtest per quarter.
        cur.execute("DELETE FROM BACKTEST_HISTORY WHERE quarter_key = ?", (qkey,))
        cur.execute(
            "INSERT INTO BACKTEST_HISTORY (run_ts, quarter_key, filtered_json, late_goal_json) VALUES (?,?,?,?)",
            (
                run_ts_iso,
                qkey,
                json.dumps(sorted(set(filtered_leagues))),
                json.dumps(sorted(set(late_goal_leagues))),
            ),
        )
        con.commit()
    finally:
        con.close()


def list_tables(db_path: str) -> set:
    con = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", con)
    finally:
        con.close()
    return set(df["name"].tolist())


def load_table(db_path: str, table: str) -> pd.DataFrame:
    con = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)
    finally:
        con.close()
    return df


def parse_scoreline(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None, None
    s = str(x).strip()
    if not s:
        return None, None
    if "-" not in s:
        return None, None
    left, right = s.split("-", 1)
    try:
        return int(left.strip()), int(right.strip())
    except Exception:
        return None, None


def require_columns(df: pd.DataFrame, table: str, cols: list) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns in {table}: {missing}")


def build_archive_v2_signals(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "marketStartTime",
        "ft_score",
        "ht_score",
        "goals_60",
        "Odds Betfair Draw",
        "comp",
        "event_name",
    ]
    require_columns(df, "archive_v2", required)

    ft_pairs = df["ft_score"].apply(parse_scoreline)
    df["ft_home"], df["ft_away"] = zip(*ft_pairs)

    ht_pairs = df["ht_score"].apply(parse_scoreline)
    df["ht_home"], df["ht_away"] = zip(*ht_pairs)

    df["decisive_ft"] = (df["ft_home"] != df["ft_away"]).astype("Int64")

    goals_60_num = pd.to_numeric(df["goals_60"], errors="coerce").fillna(0).astype(int)
    df["is_00_ht"] = (df["ht_home"] == 0) & (df["ht_away"] == 0)
    df["is_00_at60"] = df["is_00_ht"] & (goals_60_num == 0)

    df["decisive_from_00at60"] = np.where(
        df["is_00_at60"],
        df["decisive_ft"],
        pd.NA,
    )

    out_cols = [
        "marketStartTime",
        "comp",
        "event_name",
        "ft_score",
        "ht_score",
        "goals_60",
        "Odds Betfair Draw",
        "decisive_ft",
        "is_00_at60",
        "decisive_from_00at60",
    ]
    renamed = df[out_cols].rename(
        columns={
            "marketStartTime": "kickoff",
            "Odds Betfair Draw": "d_SP",
            "decisive_ft": "result",
        }
    )
    return renamed.copy()


def build_archive_v3_signals(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "kickoff",
        "comp",
        "event_name",
        "ft_score",
        "h_goals60",
        "a_goals60",
        "d_SP",
        "result",
    ]
    require_columns(df, "archive_v3", required)

    df["is_00_at60"] = (df["h_goals60"] == 0) & (df["a_goals60"] == 0)

    df["decisive_from_00at60"] = np.where(
        df["is_00_at60"],
        df["result"],
        pd.NA,
    )

    out_cols = [
        "kickoff",
        "comp",
        "event_name",
        "ft_score",
        "h_goals60",
        "a_goals60",
        "d_SP",
        "result",
        "is_00_at60",
        "decisive_from_00at60",
    ]
    return df[out_cols].copy()


def calc_drawdown_and_streaks(df: pd.DataFrame) -> pd.Series:
    df = df.sort_values("kickoff_parsed").copy()
    pnl = df["total_pnl"].fillna(0.0).to_numpy()
    cum = np.cumsum(pnl)
    peak = np.maximum.accumulate(cum)
    drawdown = cum - peak
    max_drawdown = float(drawdown.min()) if len(drawdown) else 0.0

    wins = pnl > 0
    losses = pnl < 0
    max_win_streak = 0
    max_loss_streak = 0
    cur_win = 0
    cur_loss = 0
    for w, l in zip(wins, losses):
        if w:
            cur_win += 1
            cur_loss = 0
        elif l:
            cur_loss += 1
            cur_win = 0
        else:
            cur_win = 0
            cur_loss = 0
        if cur_win > max_win_streak:
            max_win_streak = cur_win
        if cur_loss > max_loss_streak:
            max_loss_streak = cur_loss

    win_rate = float(wins.mean() * 100.0) if len(wins) else 0.0
    return pd.Series(
        {
            "win_rate": win_rate,
            "max_drawdown": max_drawdown,
            "max_win_streak": int(max_win_streak),
            "max_loss_streak": int(max_loss_streak),
        }
    )


# ===================== MAIN =====================
def main():
    ensure_outdir(OUT_DIR)

    # Validate required tables exist before running backtest logic.
    tables_on_disk = list_tables(DB_PATH)
    for table in TABLES:
        if table not in tables_on_disk:
            raise RuntimeError(f"Missing table in DB: {table}")

    # Load source tables.
    df_v2 = load_table(DB_PATH, "archive_v2")
    df_v3 = load_table(DB_PATH, "archive_v3")

    # Build normalized signal extracts for each archive.
    signals_v2 = build_archive_v2_signals(df_v2)
    signals_v3 = build_archive_v3_signals(df_v3)

    # Write per-archive outputs and merged signals.
    v2_path = os.path.join(OUT_DIR, "archive_v2_ltd60_signals.csv")
    v3_path = os.path.join(OUT_DIR, "archive_v3_ltd60_signals.csv")
    merged_path = os.path.join(OUT_DIR, "archive_v2_v3_ltd60_signals.csv")

    signals_v2 = signals_v2.sort_values("kickoff", kind="mergesort")
    signals_v3 = signals_v3.sort_values("kickoff", kind="mergesort")
    signals_v2.to_csv(v2_path, index=False)
    signals_v3.to_csv(v3_path, index=False)

    merged_cols = [
        "kickoff",
        "comp",
        "event_name",
        "d_SP",
        "result",
        "decisive_from_00at60",
    ]
    merged = pd.concat(
        [
            signals_v2[merged_cols],
            signals_v3[merged_cols],
        ],
        ignore_index=True,
    )
    merged["kickoff_parsed"] = pd.to_datetime(merged["kickoff"], errors="coerce", utc=True)
    merged = merged.sort_values("kickoff_parsed", kind="mergesort")
    merged = merged.drop(columns=["kickoff_parsed"])
    merged.to_csv(merged_path, index=False)

    # Compute league-level decisive and 00@60 rates.
    merged["result"] = pd.to_numeric(merged["result"], errors="coerce")
    merged["decisive_from_00at60"] = pd.to_numeric(
        merged["decisive_from_00at60"], errors="coerce"
    )
    comp_stats = (
        merged.groupby("comp")
        .agg(
            matches=("result", "size"),
            decisive_rate=("result", "mean"),
            samples_00at60=("decisive_from_00at60", lambda s: s.notna().sum()),
            decisive_00at60_rate=("decisive_from_00at60", "mean"),
        )
        .reset_index()
    )
    comp_stats["decisive_rate"] = comp_stats["decisive_rate"] * 100.0
    comp_stats["decisive_00at60_rate"] = comp_stats["decisive_00at60_rate"] * 100.0
    comp_stats[["decisive_rate", "decisive_00at60_rate"]] = comp_stats[
        ["decisive_rate", "decisive_00at60_rate"]
    ].round(2)
    filtered = comp_stats[
        (comp_stats["decisive_rate"] >= BACKTEST_DECISIVE_COMP_MIN)
        & (comp_stats["matches"] >= MATCHES_MIN_PLAYED)
    ].copy()

    # Export filtered and late-goal league lists.
    filtered_leagues_path = os.path.join(OUT_DIR, Path(FILTERED_LEAGUES_CSV_V3).name)
    filtered.to_csv(filtered_leagues_path, index=False)

    late_goal = filtered[filtered["decisive_00at60_rate"] >= BACKTEST_LATE_GOAL_COMP_MIN].copy()
    late_goal_path = os.path.join(OUT_DIR, Path(LATE_GOAL_LEAGUES_CSV_V3).name)
    late_goal.to_csv(late_goal_path, index=False)

    run_ts = datetime.utcnow()
    run_ts_iso = run_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    qkey = quarter_key(run_ts)
    filtered_leagues = filtered["comp"].dropna().astype(str).tolist()
    late_goal_leagues = late_goal["comp"].dropna().astype(str).tolist()
    write_backtest_history(DB_PATH, run_ts_iso, qkey, filtered_leagues, late_goal_leagues)

    # Build filtered overall PnL with price constraints and 00@60 logic.
    filtered_leagues_df = pd.read_csv(filtered_leagues_path)
    filtered_comps = set(filtered_leagues_df["comp"].dropna().astype(str))
    merged["comp"] = merged["comp"].astype(str)
    filtered_merged = merged[merged["comp"].isin(filtered_comps)].copy()
    filtered_merged["result"] = pd.to_numeric(filtered_merged["result"], errors="coerce")
    filtered_merged["d_SP"] = pd.to_numeric(filtered_merged["d_SP"], errors="coerce")
    filtered_merged["decisive_from_00at60"] = pd.to_numeric(
        filtered_merged["decisive_from_00at60"], errors="coerce"
    )
    bet_ok = filtered_merged["d_SP"].notna() & (filtered_merged["d_SP"] <= BACKTEST_MAX_PRICE)
    liability = (filtered_merged["d_SP"] - 1.0) * BACKTEST_STAKE
    filtered_merged["pnl"] = np.where(
        bet_ok & (filtered_merged["result"] == 1),
        BACKTEST_STAKE,
        np.where(bet_ok & (filtered_merged["result"] == 0), -liability, 0.0),
    )
    filtered_merged = filtered_merged[bet_ok].copy()
    liability = (filtered_merged["d_SP"] - 1.0) * BACKTEST_STAKE
    late_goal_df = pd.read_csv(late_goal_path)
    late_goal_comps = set(late_goal_df["comp"].dropna().astype(str))
    filtered_merged["comp"] = filtered_merged["comp"].astype(str)
    in_late_goal = filtered_merged["comp"].isin(late_goal_comps)
    ltd60_liability = (BACKTEST_MAX_PRICE_ENTRY_2 - 1.0) * BACKTEST_STAKE
    has_00at60 = filtered_merged["decisive_from_00at60"].notna()
    filtered_merged["ltd60_pnl"] = np.where(
        in_late_goal & has_00at60 & (filtered_merged["decisive_from_00at60"] == 1),
        BACKTEST_STAKE * 2.0,
        np.where(in_late_goal & has_00at60, -ltd60_liability, 0.0),
    )
    filtered_merged["total_pnl"] = filtered_merged["pnl"] + filtered_merged["ltd60_pnl"]
    filtered_merged["kickoff_parsed"] = pd.to_datetime(
        filtered_merged["kickoff"], errors="coerce", utc=True
    )
    filtered_merged = filtered_merged.sort_values("kickoff_parsed", kind="mergesort")
    filtered_merged["cum_pnl"] = filtered_merged["pnl"].cumsum()
    filtered_merged["cum_total_pnl"] = filtered_merged["total_pnl"].cumsum()
    num_cols = filtered_merged.select_dtypes(include=["number"]).columns
    filtered_merged[num_cols] = filtered_merged[num_cols].round(2)
    cum_pnl_path = os.path.join(OUT_DIR, "filtered_cum_pnl_overall_v3.csv")
    filtered_merged.to_csv(cum_pnl_path, index=False)

    league_summary = (
        filtered_merged.groupby("comp")
        .agg(
            matches=("comp", "size"),
            pnl_sum=("pnl", "sum"),
            ltd60_pnl_sum=("ltd60_pnl", "sum"),
            total_pnl_sum=("total_pnl", "sum"),
            pnl_avg=("pnl", "mean"),
            ltd60_pnl_avg=("ltd60_pnl", "mean"),
            total_pnl_avg=("total_pnl", "mean"),
        )
        .reset_index()
    )
    streaks = (
        filtered_merged.groupby("comp", group_keys=False)
        .apply(calc_drawdown_and_streaks)
        .reset_index()
    )
    league_summary = league_summary.merge(streaks, on="comp", how="left")
    league_summary = league_summary.sort_values("total_pnl_sum", ascending=False)
    league_num_cols = league_summary.select_dtypes(include=["number"]).columns
    league_summary[league_num_cols] = league_summary[league_num_cols].round(2)
    league_summary_path = os.path.join(OUT_DIR, "league_pnl_summary_v3.csv")
    league_summary.to_csv(league_summary_path, index=False)

    overall_perf = calc_drawdown_and_streaks(filtered_merged).to_frame().T
    overall_perf.insert(0, "matches", len(filtered_merged))
    overall_perf["pnl_sum"] = filtered_merged["pnl"].sum()
    overall_perf["ltd60_pnl_sum"] = filtered_merged["ltd60_pnl"].sum()
    overall_perf["total_pnl_sum"] = filtered_merged["total_pnl"].sum()
    overall_perf = overall_perf[
        [
            "matches",
            "pnl_sum",
            "ltd60_pnl_sum",
            "total_pnl_sum",
            "win_rate",
            "max_drawdown",
            "max_win_streak",
            "max_loss_streak",
        ]
    ]
    overall_num_cols = overall_perf.select_dtypes(include=["number"]).columns
    overall_perf[overall_num_cols] = overall_perf[overall_num_cols].round(2)
    overall_path = os.path.join(OUT_DIR, "overall_performance_summary_v3.csv")
    overall_perf.to_csv(overall_path, index=False)

    update_ltd60_backtest_report_v3.main()

    print("Done.")
    print(f"Outputs written to: {OUT_DIR}")
    print(" - archive_v2_ltd60_signals.csv")
    print(" - archive_v3_ltd60_signals.csv")
    print(" - archive_v2_v3_ltd60_signals.csv")
    print(f" - {filtered_leagues_path}")
    print(f" - {late_goal_path}")
    print(f" - {cum_pnl_path}")
    print(f" - {league_summary_path}")
    print(f" - {overall_path}")


if __name__ == "__main__":
    main()
