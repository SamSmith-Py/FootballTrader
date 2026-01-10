import sys
import sqlite3
from pathlib import Path
import runpy

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.settings import DB_PATH, BACKTEST_DIR


OUT_CUM_PNL = Path(BACKTEST_DIR) / "PAPER_MODE" / "PAPER_MODE_cum_pnl.csv"
OUT_COMP_SUMMARY = Path(BACKTEST_DIR) / "PAPER_MODE" / "PAPER_MODE_comp_summary.csv"
OUT_OVERALL_SUMMARY = Path(BACKTEST_DIR) / "PAPER_MODE" / "PAPER_MODE_overall performance _summary.csv"
REPORT_SCRIPT = Path(BACKTEST_DIR) / "PAPER_MODE" / "update_paper_mode_report.py"


def calc_drawdown_and_streaks(df: pd.DataFrame) -> pd.Series:
    df = df.sort_values("kickoff_parsed").copy()
    pnl = df["pnl"].fillna(0.0).to_numpy()
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


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT
                event_id,
                kickoff,
                comp,
                event_name,
                strategy,
                paper,
                pnl
            FROM archive_v3
            WHERE strategy LIKE '%LTD60%'
              AND paper = 1
            """,
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        print("No LTD60 paper-mode rows found in archive_v3.")
        return

    df["kickoff_parsed"] = pd.to_datetime(df["kickoff"], errors="coerce", utc=True)
    df = df.sort_values("kickoff_parsed", kind="mergesort")
    df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0)
    df["cum_pnl"] = df["pnl"].cumsum()

    OUT_CUM_PNL.parent.mkdir(parents=True, exist_ok=True)
    df.drop(columns=["kickoff_parsed"]).to_csv(OUT_CUM_PNL, index=False)

    comp_summary = (
        df.groupby("comp")
        .agg(
            matches=("comp", "size"),
            pnl_sum=("pnl", "sum"),
            pnl_avg=("pnl", "mean"),
        )
        .reset_index()
    )
    streaks = (
        df.groupby("comp", group_keys=False)
        .apply(calc_drawdown_and_streaks)
        .reset_index()
    )
    comp_summary = comp_summary.merge(streaks, on="comp", how="left")
    comp_summary = comp_summary.sort_values("pnl_sum", ascending=False)
    num_cols = comp_summary.select_dtypes(include=["number"]).columns
    comp_summary[num_cols] = comp_summary[num_cols].round(2)
    comp_summary.to_csv(OUT_COMP_SUMMARY, index=False)

    overall = calc_drawdown_and_streaks(df)
    ltd60_pnl_sum = float(df.loc[df["strategy"] == "LTD60", "pnl"].sum())
    total_pnl_sum = float(df["pnl"].sum())
    overall_row = {
        "matches": int(len(df)),
        "ltd60_pnl_sum": ltd60_pnl_sum,
        "win_rate": float(overall["win_rate"]),
        "max_drawdown": float(overall["max_drawdown"]),
        "max_win_streak": float(overall["max_win_streak"]),
        "max_loss_streak": float(overall["max_loss_streak"]),
    }
    overall_df = pd.DataFrame([overall_row], columns=[
        "matches",
        "ltd60_pnl_sum",
        "win_rate",
        "max_drawdown",
        "max_win_streak",
        "max_loss_streak",
    ])
    overall_df = overall_df.round(2)
    overall_df.to_csv(OUT_OVERALL_SUMMARY, index=False)

    if REPORT_SCRIPT.exists():
        runpy.run_path(str(REPORT_SCRIPT), run_name="__main__")

    print("Outputs written:")
    print(f" - {OUT_CUM_PNL}")
    print(f" - {OUT_COMP_SUMMARY}")
    print(f" - {OUT_OVERALL_SUMMARY}")


if __name__ == "__main__":
    main()
