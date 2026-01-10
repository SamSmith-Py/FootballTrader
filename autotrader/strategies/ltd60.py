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
import logging
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
    LOG_DIR,
    LTD60_SECOND_ENTRY_TIME,
    FILTERED_LEAGUES_CSV_V3,
    LATE_GOAL_LEAGUES_CSV_V3
    
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
    filtered_leagues_csv = str(FILTERED_LEAGUES_CSV_V3)
    late_goal_leagues_csv = str(LATE_GOAL_LEAGUES_CSV_V3)

    def __init__(self):
        self._filtered: Set[str] = self._load_leagues(self.filtered_leagues_csv)
        
        self._late_goals: Set[str] = self._load_leagues(self.late_goal_leagues_csv)
        # normalise names to allow simple membership checks
        self._filtered = {self._normalise(lg) for lg in self._filtered}
        
        self._late_goals = {self._normalise(lg) for lg in self._late_goals}

    # ---------- Logging Helpers -------------
    def _ev_tag(self, ev: dict) -> str:
        return f'{ev.get("comp")} | {ev.get("event_name")}'

    def _log_order(self, level, action: str, ev: dict, **extra):
        # Example output: "ENTRY1_PLACED | 123 | TeamA v TeamB | price=4.0 size=3 betid=..."
        msg = f"{action} | {self._ev_tag(ev)}"
        if extra:
            msg += " | " + " ".join([f"{k}={v}" for k, v in extra.items()])
        logger.log(20, msg)

    # ---------- league list helpers ----------
    def _load_leagues(self, path: str) -> Set[str]:
        out = set()
        try:
            with open(path, "r", newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                # Flexible: accept 'league' or first column
                if "comp" in reader.fieldnames:
                    for r in reader:
                        lg = (r.get("comp") or "").strip()
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
        if not ev.get("strategy"):
        
            league_ok = self._normalise(ev.get("comp")) in self._filtered
            
            has_mo = bool(ev.get("market_id_MATCH_ODDS"))
            if league_ok and has_mo:
                # Assign once — market is Match Odds
                self._mark_strategy(db, ev["event_id"], strategy=self.name, market="MATCH_ODDS")
                # ------ LOGGING ASSIGNMENT ------------
                self._log_order(logger.info, "ASSIGNED", ev)

    # ---------- per tick ----------
    def on_tick(self, db: DBHelper, ev: Dict[str, Any], api=None) -> None:
        if ev.get("strategy") != self.name:
            return  # not ours

        ev_id = ev["event_id"]
        market_id = ev.get("market_id_MATCH_ODDS")
        if not market_id:
            return

        # Always refresh from DB so we have latest h_score/a_score/time_elapsed/e_* fields
        row0 = db.fetch_current(ev_id)
        if not row0:
            return
        ev = dict(row0)

        # Keep prices fresh (and optionally log stream)
        h, d, a = self._fetch_mo_prices(api, market_id)
        if any(p is not None for p in (h, d, a)):
            self._set_lay_prices(db, ev_id, h=h, a=a, d=d)

            # refresh again so stream/log sees latest state
            row1 = db.fetch_current(ev_id)
            if row1:
                ev = dict(row1)

            # Optional stream snapshot
            self._log_stream(db, ev, h=h, a=a, d=d, inplay_time=ev.get("time_elapsed"))

        # Decide entry timing
        kickoff = self._parse_dt(ev.get("kickoff"))
        now = datetime.now(timezone.utc)
        minutes_to_ko = None
        if kickoff:
            minutes_to_ko = (kickoff - now).total_seconds() / 60.0

        # Entry 1: near KO
        self._maybe_entry1(db, ev, d_price=d, minutes_to_ko=minutes_to_ko, api=api, market_id=market_id)

        # Refresh after entry1 so we have e_betid/e_status/e_matched updated
        row2 = db.fetch_current(ev_id)
        if not row2:
            return
        ev = dict(row2)

        # --- sync entry 1 order state ---
        sync = self._sync_order_state(
            db=db,
            api=api,
            logger=logger,
            ev=ev,
            market_id=market_id,
            prefix="e"
        )

        # Refresh ev snapshot if anything changed
        if sync:
            fresh = db.fetch_current(ev["event_id"])
            if fresh:
                ev = dict(fresh)

        # Cancel unmatched capped entry1 if goal OR 60' (but ONLY if 0 matched)
        self._maybe_cancel_entry1(
            db=db,
            ev=ev,
            api=api,
            market_id=market_id,
            time_elapsed=ev.get("time_elapsed"),
            h_score=ev.get("h_score"),
            a_score=ev.get("a_score"),
        )
        self._maybe_cancel_entry2(
            db=db,
            ev=ev,
            api=api,
            market_id=market_id,
            time_elapsed=ev.get("time_elapsed"),
            h_score=ev.get("h_score"),
            a_score=ev.get("a_score"),
        )

        # Refresh after potential cancel
        row3 = db.fetch_current(ev_id)
        if not row3:
            return
        ev = dict(row3)

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
        price = float(LTD60_MAX_ODDS_ACCEPT if d_price > LTD60_MAX_ODDS_ACCEPT else d_price)

        if PAPER_MODE:
            # Simulate LIMIT order if price above accepted
            if price >= LTD60_MAX_ODDS_ACCEPT:
                matched = 0
                liability = 0
                remaining = size
            else: 
                matched = size
                remaining = 0
                # Compute nominal liability
                liability = max(0.0, (float(price) - 1.0) * size)
            # Record paper entry instantly
            self._order_snapshot(
                db, ev["event_id"], side="LAY", price=float(price), size=size,
                status="PAPER_EXECUTED", matched=matched, remaining=remaining, betid=None
            )
            
            
            db.update_current(ev["event_id"], liability=liability)

            # --------- LOGGING PAPER ENTRY 1 PLACED ---------
            self._log_order(logger.info, "ENTRY1_PAPER_EXEC", ev, price=price, size=size)


        else:
            # Live: place order
            try:
                
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

                # ---------LOGGING LIVE ENTRY 1 PLACED ------------
                self._log_order(logger.info, "ENTRY1_PLACED", ev,
                price=price, size=size, betid=betid, status=status, matched=matched)

            except Exception as e:
                # Record the attempt even if failed
                self._order_snapshot(
                    db, ev["event_id"], side="LAY", price=float(price), size=size,
                    status=f"ERROR:{e}", matched=0.0, remaining=size, betid=None
                )

    def _maybe_entry2(self, db: DBHelper, ev: Dict[str, Any], d_price: Optional[float], api, market_id: str) -> None:
        # Only allowed if league in late-goal set
        if self._normalise(ev.get("comp")) not in self._late_goals:
            return


        # Check no entry 2 already ordered
        if ev.get('e_ordered'):              
            already_ordered = int(ev.get('e_ordered')) == 2 
            if already_ordered:
                return
        else:
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
        draw_now = (h_sc is not None and a_sc is not None and h_sc == 0 == a_sc)
        if not draw_now:
            return
        
        # Check if time reached to trigger second entry
        if ev.get("time_elapsed"):
            time_elapsed = int(ev.get("time_elapsed"))
             
            time_reached = time_elapsed > LTD60_SECOND_ENTRY_TIME
            if not time_reached:
                return
        else:
            return
        

        # Avoid re-triggering: if x_ordered exists you can gate on that in the future.
        # Here we'll only add a second "paper" flag by bumping e_ordered to 2 and aggregating stake.
        second_size = STAKE_LTD_PAPER if PAPER_MODE else STAKE_LTD_LIVE
        price = float(LTD60_MAX_SECOND_ENTRY_ODDS if d_price > LTD60_MAX_SECOND_ENTRY_ODDS else d_price)

        # If price missing, try to pull quickly
        if d_price is None and api:
            _, d_price, _ = self._fetch_mo_prices(api, market_id)

        # Guard against missing price
        if d_price is None:
            return

        if PAPER_MODE:
            prev_matched = float(ev.get("e_matched") or 0.0)
            prev_liability = float(ev.get("liability") or 0.0)
            prev_stake = float(ev.get("e_stake") or 0.0)
            prev_remaining = float(ev.get("e_remaining") or 0.0)
            # Simulate LIMIT order if price above accepted
            if price >= LTD60_MAX_SECOND_ENTRY_ODDS:
                matched = prev_matched
                liability = prev_liability
                remaining = prev_remaining + STAKE_LTD_PAPER
            else: 
                matched = prev_matched + STAKE_LTD_PAPER
                remaining = 0
                # Compute nominal liability
                liability = prev_liability + max(0.0, (float(price) - 1.0) * STAKE_LTD_PAPER)
            # Mark second entry logically by stacking stake/liability
            
            new_stake = prev_stake + second_size
            
            # liability = max(0.0, (float(ev.get("e_price") or price) - 1.0) * STAKE_LTD_PAPER) + prev_liability
            db.update_current(
                ev["event_id"],
                e_ordered=2,
                e_stake=new_stake,
                e_status="PAPER_SECOND",
                e_matched=matched,
                e_remaining=remaining,
                liability=liability,
            )

            # --------- LOGGING PAPER ENTRY 2 PLACED ---------
            self._log_order(logger.info, "ENTRY2_PAPER_EXEC", ev, price=price, size=new_stake)
            
        else:
            try:
                
                limit_order = filters.limit_order(size=second_size, price=float(price), persistence_type="PERSIST")
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
                prev_stake = float(ev.get("e_stake") or 0.0)
                new_stake = prev_stake + second_size
                db.update_current(
                    ev["event_id"],
                    e_ordered=2,
                    e_status=status,
                    e_stake=new_stake,
                    e_matched=(float(ev.get("e_matched") or 0.0) + matched),
                    e_betid=betid
                )
                # recompute liability using avg price if you want; we keep it simple:
                effective_price = float(ev.get("e_price") or price)
                liability = max(0.0, (effective_price - 1.0) * new_stake)
                db.update_current(ev["event_id"], liability=liability)

                # ---------- LOGGING LIVE ENTRY 2 PLACED -----------
                self._log_order(logger.info, "ENTRY2_PLACED", ev,
                price=price, size=second_size, betid=betid, status=status, matched=matched)

            except Exception as e:
                # Log attempt
                db.update_current(
                    ev["event_id"],
                    e_status=f"SECOND_ERROR:{e}",
                )

    # ----------cancel entrys -------
    def _maybe_cancel_entry1(
        self,
        db: DBHelper,
        ev: Dict[str, Any],
        api,
        market_id: str,
        time_elapsed: Optional[int],
        h_score: Optional[int],
        a_score: Optional[int],
    ) -> None:
        """
        Cancel entry1 ONLY if:
        - Live mode
        - entry1 is a capped limit (e_price == LTD60_MAX_ODDS_ACCEPT)
        - 0 matched
        - and (goal scored OR time_elapsed >= LTD60_SECOND_ENTRY_TIME)
        """

        if api is None:
            return

        if not ev.get("e_ordered"):
            return

        if PAPER_MODE == 0:
            betid = ev.get("e_betid")
            if not betid:
                return

        # Status guards
        status = (ev.get("e_status") or "").upper()
        if "CANCEL" in status or "EXECUTION_COMPLETE" in status:
            return

        # Only cancel capped orders
        e_price = ev.get("e_price")
        if e_price is None or float(e_price) != float(LTD60_MAX_ODDS_ACCEPT):
            return

        matched = float(ev.get("e_matched") or 0.0)
        if matched > 0.0:
            return  # IMPORTANT: do not cancel if any matched

        # Cancel triggers
        # goals = (int(h_score or 0) + int(a_score or 0))
        # cancel_for_goal = goals > 0
        cancel_for_60 = (time_elapsed is not None and int(time_elapsed) >= LTD60_SECOND_ENTRY_TIME)

        if not (cancel_for_60):
            return

        reason =  "UNMATCHED BY 60MIN"

        # -----LOGGING CANCEL ENTRY 1 TRIGGER ----------
        self._log_order(logging.WARNING, "ENTRY1_CANCEL_TRIGGER", ev,
                        reason=reason, betid=ev.get("e_betid"), price=ev.get("e_price"))
        try:
            if PAPER_MODE == 0:
                instr = filters.cancel_instruction(bet_id=str(betid))
                api.betting.cancel_orders(market_id=str(market_id), instructions=[instr])

            # Mark cancelled in DB. Keep e_ordered=1 to indicate "attempted" (recommended).
            db.update_current(
                ev["event_id"],
                e_status=f"CANCELLED_{reason}",
                e_remaining=0.0,
                e_matched=0.0
            )

            # ---------- LOGGING CANCELLED ENTRY 1 ----------
            self._log_order(logger.info, "ENTRY1_CANCELLED", ev, reason=reason, betid=ev.get("e_betid"))
        except Exception as e:
            db.update_current(ev["event_id"], e_status=f"CANCEL_ERR_{reason}:{e}")

            # -----LOGGING FAIL CANCEL ENTRY 1 TRIGGER ----------
            self._log_order(logger.info, "ENTRY1_CANCEL_FAIL", ev, reason=reason, err=str(e))

    def _maybe_cancel_entry2(
        self,
        db: DBHelper,
        ev: Dict[str, Any],
        api,
        market_id: str,
        time_elapsed: Optional[int],
        h_score: Optional[int],
        a_score: Optional[int],
    ) -> None:
        """
        Cancel entry2 ONLY if:
        - Live mode
        - entry2 is a capped limit (e_price == LTD60_MAX_SECOND_ENTRY_ODDS)
        - 0 matched
        - and (goal scored OR time_elapsed >= 75)
        """

        if api is None:
            return

        # Check entry 2 has been ordered
        e_ordered = ev.get("e_ordered")
        if int(e_ordered or 0) != 2:
            return


        if PAPER_MODE == 0:
            betid = ev.get("e_betid")
            if not betid:
                return

        # Status guards
        status = (ev.get("e_status") or "").upper()
        if "CANCEL" in status or "EXECUTION_COMPLETE" in status:
            return

        # Only cancel capped orders
        e_price = ev.get("e_price")
        if e_price is None or float(e_price) != float(LTD60_MAX_SECOND_ENTRY_ODDS):
            return

        # Check if mathced minus the second stake 
        second_stake = float(ev.get("e_stake") or 0.0) / 2
        matched = float(ev.get("e_matched") or 0.0) - second_stake
        if matched > 0.0:
            return  # IMPORTANT: do not cancel if any matched

        # Cancel triggers
        # goals = (int(h_score or 0) + int(a_score or 0))
        # cancel_for_goal = goals > 0
        cancel_for_75 = (time_elapsed is not None and int(time_elapsed) >= 75)

        if not (cancel_for_75):
            return

        reason = "UNMATCHED BY 75MIN"

        # -----LOGGING CANCEL ENTRY 1 TRIGGER ----------
        self._log_order(logging.WARNING, "ENTRY2_CANCEL_TRIGGER", ev,
                        reason=reason, betid=ev.get("e_betid"), price=ev.get("e_price"))
        try:
            if PAPER_MODE == 0:
                instr = filters.cancel_instruction(bet_id=str(betid))
                api.betting.cancel_orders(market_id=str(market_id), instructions=[instr])

            # Mark cancelled in DB. Keep e_ordered=1 to indicate "attempted" (recommended).
            db.update_current(
                ev["event_id"],
                e_status=f"CANCELLED_{reason}",
                e_remaining=0.0,
            )

            # ---------- LOGGING CANCELLED ENTRY 1 ----------
            self._log_order(logger.info, "ENTRY2_CANCELLED", ev, reason=reason, betid=ev.get("e_betid"))
        except Exception as e:
            db.update_current(ev["event_id"], e_status=f"CANCEL_ERR_{reason}:{e}")

            # -----LOGGING FAIL CANCEL ENTRY 1 TRIGGER ----------
            self._log_order(logger.info, "ENTRY2_CANCEL_FAIL", ev, reason=reason, err=str(e))


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
            away = best_lay(runners[1]) if len(runners) > 1 else None
            draw = best_lay(runners[2]) if len(runners) > 2 else None
            return (home, draw, away)
        except Exception:
            return (None, None, None)
