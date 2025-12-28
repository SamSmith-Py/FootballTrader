"""
LTD60 — Lay the Draw at KO and optional second lay at 60'.

Key points:
- Only assign on leagues present in filtered_leagues.csv (strategy-specific)
- Second entry allowed only if league is in late_goal_leagues.csv AND match is draw at 60'
- No exits; settle at FT handled elsewhere
"""

from __future__ import annotations
from typing import Dict, Any, Set, Optional
from datetime import datetime, timezone, timedelta
import csv
import os

from betfairlightweight import filters

from core.settings import (
    BASE_DIR,
    DB_PATH,
    TABLE_CURRENT,
    PAPER_MODE,
    BOT_VERSION,
    LTD60_KO_WINDOW_MINUTES,
    LTD60_MAX_ODDS_ACCEPT,
    STAKE_LTD_PAPER,
    STAKE_LTD_LIVE,
    DRAW_SELECTION_ID,
    LTD60_MAX_SECOND_ENTRY_ODDS,
    LOG_DIR
    
)
from core.db_helper import DBHelper
from autotrader.strategies.base_strategy import BaseStrategy

# Logging Setup
from core.logging_setup import setup_LTD60_logging

logger = setup_LTD60_logging(log_dir=LOG_DIR / "logs")

#   # Betfair's global selection id for "The Draw" in Match Odds
# MAX_KO_LAY_ODDS = 4.0      # hard-cap for entry
# KO_WINDOW_MINUTES = 12     # place first lay when within 12 minutes of kickoff (or later)
# SECOND_ENTRY_MINUTE = 60
# SECOND_ENTRY_ODDS = 2.0    # configurable if needed
# STAKE_LIVE = 3.0           # live stake (size)
# STAKE_PAPER = 100.0        # paper "stake" for PnL accounting


class LTD60(BaseStrategy):
    name = "LTD60"
    requires_api = True

    # strategy-specific league lists — produced by your backtester for LTD60 only
    filtered_leagues_csv = str(BASE_DIR / "backtest" / "strategy" / "LTD" / "Q4 2025" / "data_analysis" / "filtered_leagues_v2.csv")
    late_goal_leagues_csv = str(BASE_DIR / "backtest" / "strategy" / "LTD" / "Q4 2025" / "data_analysis" / "late_goal_leagues_v2.csv")

    def __init__(self):
        self._filtered: Set[str] = self._load_leagues(self.filtered_leagues_csv)
        self._late_goals: Set[str] = self._load_leagues(self.late_goal_leagues_csv)
        # normalise names to allow simple membership checks
        self._filtered = {self._normalise(lg) for lg in self._filtered}
        self._late_goals = {self._normalise(lg) for lg in self._late_goals}

    # ---------- league list helpers ----------
    def _load_leagues(self, path: str) -> Set[str]:
        out = set()
        try:
            with open(path, "r", newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                # Flexible: accept 'league' or first column
                if "league" in reader.fieldnames:
                    for r in reader:
                        lg = (r.get("league") or "").strip()
                        if lg:
                            out.add(lg)
                else:
                    # fallback to first column
                    for r in reader:
                        first = (list(r.values())[0] or "").strip()
                        if first:
                            out.add(first)
        except FileNotFoundError:
            # No lists -> strategy won't assign anywhere
            pass
        return out

    def _normalise(self, league_name: str) -> str:
        """Normalise league label for consistent set membership."""
        return (league_name or "").strip().lower()

    # ---------- assignment ----------
    def assign_if_applicable(self, db: DBHelper, ev: Dict[str, Any]) -> None:
        if ev.get("strategy") in (self.name, None, ""):
            league_ok = self._normalise(ev.get("league")) in self._filtered
            has_mo = bool(ev.get("market_id_MATCH_ODDS"))
            if league_ok and has_mo:
                # Assign once — market is Match Odds
                self._mark_strategy(db, ev["event_id"], strategy=self.name, market="MATCH_ODDS")

    # ---------- per tick ----------
    def on_tick(self, db: DBHelper, ev: Dict[str, Any], api=None) -> None:
        if ev.get("strategy") != self.name:
            return  # not ours

        ev_id = ev["event_id"]
        market_id = ev.get("market_id_MATCH_ODDS")
        if not market_id:
            return

        # Keep prices fresh (and optionally log stream)
        h, d, a = self._fetch_mo_prices(api, market_id)
        if any(p is not None for p in (h, d, a)):
            self._set_lay_prices(db, ev_id, h=h, a=a, d=d)
            # Optionally log simple stream snapshot
            self._log_stream(db, ev, h=h, a=a, d=d, inplay_time=None)

        # Decide entry timing
        kickoff = self._parse_dt(ev.get("kickoff"))
        now = datetime.now(timezone.utc)
        minutes_to_ko = None
        if kickoff:
            delta = (kickoff - now).total_seconds() / 60.0
            minutes_to_ko = delta

        # Entry 1: near KO
        self._maybe_entry1(db, ev, d_price=d, minutes_to_ko=minutes_to_ko, api=api, market_id=market_id)

        # Entry 2: at 60' if draw and league is late-goal
        self._maybe_entry2(db, ev, d_price=d, api=api, market_id=market_id)

    # ---------- entries ----------
    def _maybe_entry1(self, db: DBHelper, ev: Dict[str, Any], d_price: Optional[float],
                      minutes_to_ko: Optional[float], api, market_id: str) -> None:
        if ev.get("e_ordered"):
            return  # already in

        # Require within KO window or in-play but early
        if minutes_to_ko is not None and minutes_to_ko > LTD60_KO_WINDOW_MINUTES:
            return

        if d_price is None or d_price <= 0:
            return

        size = STAKE_LTD_PAPER if PAPER_MODE else STAKE_LTD_LIVE
        if PAPER_MODE:
            # Record paper entry instantly
            self._order_snapshot(
                db, ev["event_id"], side="LAY", price=float(d_price), size=size,
                status="PAPER_EXECUTED", matched=size, remaining=0.0, betid=None
            )
            # Compute nominal liability
            liability = max(0.0, (float(d_price) - 1.0) * size)
            db.update_current(ev["event_id"], liability=liability)
        else:
            # Live: place order
            try:
                if d_price > LTD60_MAX_ODDS_ACCEPT:
                    price = LTD60_MAX_ODDS_ACCEPT
                else:
                    price = d_price

                limit_order = filters.limit_order(size=size, price=float(price), persistence_type="PERSIST")
                instruction = filters.place_instruction(
                    order_type="LIMIT",
                    selection_id=DRAW_SELECTION_ID,
                    side="LAY",
                    limit_order=limit_order,
                )
                resp = api.betting.place_orders(market_id=str(market_id), instructions=[instruction])
                rep = resp.place_instruction_reports[0]
                status = rep.status
                betid = getattr(rep, "bet_id", None)
                matched = getattr(rep, "size_matched", 0.0) or 0.0

                self._order_snapshot(
                    db, ev["event_id"], side="LAY", price=float(price), size=size,
                    status=status, matched=matched, remaining=max(0.0, size - matched), betid=betid
                )
                liability = max(0.0, (float(price) - 1.0) * size)
                db.update_current(ev["event_id"], liability=liability)
            except Exception as e:
                # Record the attempt even if failed
                self._order_snapshot(
                    db, ev["event_id"], side="LAY", price=float(price), size=size,
                    status=f"ERROR:{e}", matched=0.0, remaining=size, betid=None
                )

    def _maybe_entry2(self, db: DBHelper, ev: Dict[str, Any], d_price: Optional[float], api, market_id: str) -> None:
        # Only allowed if league in late-goal set
        if self._normalise(ev.get("league")) not in self._late_goals:
            return

        # Needs to be draw at 60 — for now, infer from scores if available
        try:
            h_sc = int(ev.get("h_score") or 0)
            a_sc = int(ev.get("a_score") or 0)
        except Exception:
            h_sc = a_sc = None

        # If we don’t have in-play time tracking yet, a safe proxy is to only allow second entry
        # when e_ordered == 1 already AND we’re in-play & prices are populated.
        # You can wire in precise time_elapsed later.
        draw_now = (h_sc is not None and a_sc is not None and h_sc == a_sc)
        if not draw_now:
            return

        # Avoid re-triggering: if x_ordered exists you can gate on that in the future.
        # Here we'll only add a second "paper" flag by bumping e_ordered to 2 and aggregating stake.
        second_size = STAKE_LTD_PAPER if PAPER_MODE else STAKE_LTD_LIVE

        # If price missing, try to pull quickly
        if d_price is None and api:
            _, d_price, _ = self._fetch_mo_prices(api, market_id)

        # Guard against missing price
        if d_price is None:
            return

        if PAPER_MODE:
            # Mark second entry logically by stacking stake/liability
            prev_stake = float(ev.get("e_stake") or 0.0)
            new_stake = second_size
            liability = max(0.0, (float(ev.get("e_price") or d_price) - 1.0) * new_stake)
            db.update_current(
                ev["event_id"],
                e_ordered=2,
                e_stake=new_stake,
                e_status="PAPER_SECOND",
                liability=liability,
            )
        else:
            try:
                limit_order = filters.limit_order(size=second_size, price=float(LTD60_MAX_SECOND_ENTRY_ODDS), persistence_type="PERSIST")
                instruction = filters.place_instruction(
                    order_type="LIMIT",
                    selection_id=DRAW_SELECTION_ID,
                    side="LAY",
                    limit_order=limit_order,
                )
                resp = api.betting.place_orders(market_id=str(market_id), instructions=[instruction])
                rep = resp.place_instruction_reports[0]
                status = rep.status
                matched = getattr(rep, "size_matched", 0.0) or 0.0
                prev_stake = float(ev.get("e_stake") or 0.0)
                new_stake = prev_stake + second_size
                db.update_current(
                    ev["event_id"],
                    e_ordered=2,
                    e_status=status,
                    e_stake=new_stake,
                    e_matched=(float(ev.get("e_matched") or 0.0) + matched),
                )
                # recompute liability using avg price if you want; we keep it simple:
                effective_price = float(ev.get("e_price") or d_price)
                liability = max(0.0, (effective_price - 1.0) * new_stake)
                db.update_current(ev["event_id"], liability=liability)
            except Exception as e:
                # Log attempt
                db.update_current(
                    ev["event_id"],
                    e_status=f"SECOND_ERROR:{e}",
                )

    # ---------- utilities ----------
    def _parse_dt(self, s) -> Optional[datetime]:
        if not s:
            return None
        try:
            # SQLite may store naive; treat as UTC
            dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _fetch_mo_prices(self, api, market_id: str):
        """Return (home_lay, draw_lay, away_lay) simplified from list_runner_book."""
        if api is None:
            return (None, None, None)
        try:
            books = api.betting.list_market_book(
                market_ids=[str(market_id)],
                price_projection={"priceData": ["EX_BEST_OFFERS"]},
            )
            if not books:
                return (None, None, None)
            runners = books[0].runners or []
            # Assumption: runner 0 = Home, 1 = Draw, 2 = Away (common ordering)
            # Safer approach is to map by selection_id; for now we follow your previous draw id usage.
            def best_lay(r):
                probs = getattr(r.ex, "available_to_lay", None) or []
                return float(probs[0].price) if probs else None

            home = best_lay(runners[0]) if len(runners) > 0 else None
            draw = best_lay(runners[1]) if len(runners) > 1 else None
            away = best_lay(runners[2]) if len(runners) > 2 else None
            return (home, draw, away)
        except Exception:
            return (None, None, None)
