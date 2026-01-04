"""
AutoTrader v2 — full live loop.
Fetches in-play data + runs strategies.
"""

from __future__ import annotations
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Type, Optional, Dict, Any

from core.settings import (
    DB_PATH,
    TABLE_CURRENT,
    PAPER_MODE,
    BOT_VERSION,
    CONFIG_PATH,
    SP_CAPTURE_WINDOW_SEC,
    SP_FALLBACK_INPLAY,
    LOG_DIR
)

from core.db_helper import DBHelper
from core.betfair_session import BetfairSession
from core.config_loader import load_betfair_credentials

# Strategy registry
from autotrader.strategies.base_strategy import BaseStrategy
from autotrader.strategies.ltd60 import LTD60

# Logging Setup
from core.logging_setup import setup_bot_logging

logger = setup_bot_logging(log_dir=LOG_DIR / "logs")

# logger = logging.getLogger("autotrader")
# logger.setLevel(logging.INFO)
#if not logger.handlers:
 #   h = logging.StreamHandler()
  #  h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
   # logger.addHandler(h)


class AutoTrader:
    def __init__(self):
        self.strategies: List[BaseStrategy] = []
        self._install_strategies([LTD60])
        self.logged_kickoff = set()
        self.logged_finished = set()
        self.last_logged_band = {}  # event_id -> last band logged (15/30/...)
        self._last_heartbeat = 0

        logger.info("AutoTrader initialised. Paper=%s Bot=%s", PAPER_MODE, BOT_VERSION)

    def _install_strategies(self, strategy_types: List[Type[BaseStrategy]]):
        for cls in strategy_types:
            self.strategies.append(cls())
    
    # ========== helpers ============
    def _band_for_time(self, t: int) -> int:
        if t < 0: return -1
        if 0 <= t < 15: return 0
        if 15 <= t < 30: return 15  # i think this should be if t >= 15 and t < 30 OR 15 <= t < 30
        if 30 <= t < 45: return 30
        if 45 <= t < 60: return 45
        if 60 <= t < 75: return 60
        if 75 <= t < 90: return 75
        return 90
    
    def _cleanup_stale_matches(self, db: DBHelper) -> None:
        now = datetime.now(timezone.utc)

        rows = db.list_current()
        
        to_delete = []

        for r in rows:
            ev = dict(r)
            event_id = r["event_id"]
            kickoff = ev.get("kickoff")
            if not kickoff:
                continue

            # parse ISO kickoff
            try:
                ko = datetime.fromisoformat(kickoff.replace("Z", "+00:00"))
            except Exception:
                continue

            age = now - ko

            inplay = ev.get("inplay_status")
            h_score = ev.get("h_score")
            a_score = ev.get("a_score")
            ft = ev.get("ft_score")

            # ---- Purge window (dead matches) ----
            dead = (
                inplay is None
                and ft is None
                and h_score is None
                and a_score is None
            )

            if age >= timedelta(hours=24) and dead:
                to_delete.append(event_id)

        if to_delete:
            with db.tx():
                for event_id in to_delete:
                    db.conn.execute("DELETE FROM current_matches WHERE event_id=?", (event_id,))
            logger.info("AutoTrader | PURGED stale rows | count=%d", len(to_delete))


    # ========== MAIN LOOP ==========
    def start(self):
        """Main live loop: updates matches + runs strategies."""
        logger.info("AutoTrader run started. Waiting for matches...")
        username, password, app_key = load_betfair_credentials(CONFIG_PATH)

        while True:
            with DBHelper(DB_PATH) as db:
                self._cleanup_stale_matches(db)
                rows = db.list_current(where_sql="", params=())
                # then sort in Python if you want deterministic ordering:
                rows = sorted(rows, key=lambda r: (r["kickoff"] or ""))


                if not rows:
                    time.sleep(10)
                    continue

                # Prepare API session
                needs_api = any(s.requires_api for s in self.strategies)
                api = None
                if needs_api:
                    try:
                        api = BetfairSession(username, password, app_key).connect()
                    except Exception as e:
                        logger.warning(f"Betfair session unavailable: {e}")
                        api = None

                # Update each match + run strategies
                for row in rows:
                    ev = dict(row)
                    event_id = ev["event_id"]
                    # IMPORTANT: refresh event snapshot after DB updates
                    fresh = db.fetch_current(event_id)
                    if fresh:
                        ev = dict(fresh)



                    try:
                        # ===== Fetch Betfair in-play data =====
                        if api:
                            self._update_inplay_info(db, api, ev)
                        fresh_after = db.fetch_current(event_id)
                        if not fresh_after:
                            continue  # it was archived (or removed)
                        ev = dict(fresh_after)

                        # ===== ARCHIVE CHECK ===================
                        self.decide_to_archive(db, api, ev)

                        #===== LOGGING KICKOFF ==================
                        ips = ev.get("inplay_status")
                        te = ev.get("time_elapsed")

                        if ev["event_id"] not in self.logged_kickoff:
                            # consider kickoff when time_elapsed >= 0 or status indicates kickoff
                            if (isinstance(te, (int, float)) and int(te) >= 0) or ips in ("KickOff", "InPlay", "SecondHalfKickOff"):
                                logger.info(
                                    "KICKOFF | %s | %s | SP(H/D/A)=%.3f/%.3f/%.3f | fav=%s | strat=%s",
                                    ev.get("comp"),
                                    ev.get("event_name"),
                                    ev.get("h_SP") or -1,
                                    ev.get("d_SP") or -1,
                                    ev.get("a_SP") or -1,
                                    ev.get("fav"),
                                    ev.get("strategy"),
                                )
                                self.logged_kickoff.add(ev["event_id"])

                        # ===== LOGGING INTERVAL =================
                        
                        if (isinstance(te, (int, float)) and int(te) >= 0) or ips in ("KickOff", "InPlay", "SecondHalfKickOff"):  
                            t = int(te)
                            band = self._band_for_time(t)
                            last = self.last_logged_band.get(ev["event_id"])
                            if band in (15,30,45,60,75,90) and last != band and ips not in ("Finished", "Cancelled", "Abandoned"):
                                logger.info(
                                    "BAND %s' | %s | %s | %s | %s-%s | RC(H/A)=%s/%s | strat=%s",
                                    band,
                                    ev.get("time_elapsed"),
                                    ev.get("comp"),
                                    ev.get("event_name"),
                                    ev.get("h_score"),
                                    ev.get("a_score"),
                                    ev.get("h_red_cards"),
                                    ev.get("a_red_cards"),
                                    ev.get("strategy"),
                                )
                            self.last_logged_band[ev["event_id"]] = band

                    except Exception as e:
                        logger.warning("Skipping live update for %s: %s", event_id, e)

                    # ===== Run strategy logic =====
                    for strat in self.strategies:
                        try:
                            
                            strat.assign_if_applicable(db, ev)
                            strat.on_tick(db, ev, api=api)
                        except Exception as e:
                            logger.error("[%s] error on %s: %s", strat.name, ev.get("event_id"), e)

                if api:
                    try:
                        api.logout()
                    except Exception:
                        pass
                # ===== HEARTBEAT CHECK ============
                now = time.time()
                if now - self._last_heartbeat > 60:
                    total = db.conn.execute("SELECT COUNT(*) FROM current_matches").fetchone()[0]
                    inplay = db.conn.execute(
                        "SELECT COUNT(*) FROM current_matches WHERE inplay_status IS NOT NULL AND inplay_status != ''"
                    ).fetchone()[0]
                    with_strat = db.conn.execute(
                        "SELECT COUNT(*) FROM current_matches WHERE strategy IS NOT NULL AND strategy != 'None'"
                    ).fetchone()[0]

                    logger.info("HEARTBEAT | total=%s inplay=%s with_strategy=%s", total, inplay, with_strat)
                    self._last_heartbeat = now
            # Short cooldown between ticks
            time.sleep(10)


    def _compute_result(self, h: Optional[int], a: Optional[int]) -> Optional[int]:
        if h is None or a is None:
            return None
        return 1 if int(h) != int(a) else 0


    def _update_inplay_info(self, db: DBHelper, api, ev: Dict[str, Any]):
        """Fetches in-play scores, red cards, market prices, SP (once), fav (once), and goal timeline."""
        event_id = ev["event_id"]
        market_id = ev.get("market_id_MATCH_ODDS")
        inplay_status = None
        time_elapsed = None
        h_score = None
        a_score = None
        h_red = None
        a_red = None


        # 1) In-play status & score
        try:
            scores = api.in_play_service.get_scores(event_ids=[event_id])
        except Exception:
            scores = None

        if scores:
            s = scores[0]

            inplay_status = getattr(s, "match_status", None)
            time_elapsed = getattr(s, "time_elapsed", None)

            # Goals (current)
            h_score = getattr(getattr(s, "score", None).home, "score", None) if getattr(s, "score", None) else None
            a_score = getattr(getattr(s, "score", None).away, "score", None) if getattr(s, "score", None) else None

            # Red cards
            h_red = getattr(getattr(s, "score", None).home, "number_of_red_cards", None) if getattr(s, "score", None) else None
            a_red = getattr(getattr(s, "score", None).away, "number_of_red_cards", None) if getattr(s, "score", None) else None

            db.update_current(
                event_id,
                inplay_status=inplay_status,
                time_elapsed=time_elapsed,
                h_score=h_score,
                a_score=a_score,
                h_red_cards=h_red,
                a_red_cards=a_red,
            )

            # Update goal timeline columns (writes bands once; backfills on finish)
            self._update_goal_timeline(
                db=db,
                event_id=event_id,
                time_elapsed=time_elapsed,
                inplay_status=inplay_status,
                h_score=h_score,
                a_score=a_score,
                ft_score=ev.get("ft_score"),
            )

        # 2) Market prices + market state + Starting Prices (once) + fav (once)
        if not market_id:
            return

        market_books = api.betting.list_market_book(market_ids=[market_id], price_projection={'priceData': ['EX_ALL_OFFERS']})
        if not market_books:
            return

        book = market_books[0]
        runners = getattr(book, "runners", None) or []
        market_state = getattr(book, "status", None)  # e.g. OPEN, SUSPENDED, CLOSED

        # Update prices (best available back)
        if len(runners) >= 3:
            def best_back_price(r):
                ex = getattr(r, "ex", None)
                atb = getattr(ex, "available_to_back", None) if ex else None
                if atb and len(atb) > 0:
                    return atb[0].price
                return None
            
            def best_lay_price(r):
                ex = getattr(r, "ex", None)
                atl = getattr(ex, "available_to_lay", None) if ex else None
                if atl and len(atl) > 0:
                    return atl[0].price
                return None

            h_back_price = best_back_price(runners[0])
            a_back_price = best_back_price(runners[1])
            d_back_price = best_back_price(runners[2])

            h_lay_price = best_lay_price(runners[0])
            a_lay_price = best_lay_price(runners[1])
            d_lay_price = best_lay_price(runners[2])

            db.update_current(
                event_id,
                h_back_price=h_back_price,
                a_back_price=a_back_price,
                d_back_price=d_back_price,
                h_lay_price=h_lay_price,
                a_lay_price=a_lay_price,
                d_lay_price=d_lay_price,
                market_state=market_state,
            )

        # ---- SP + fav one-time updater ----
        # ---- PRE-KO SNAPSHOT "SP" + fav one-time updater ----
        row = db.fetch_current(event_id)
        h_sp_cur = row["h_SP"] if row else None
        a_sp_cur = row["a_SP"] if row else None
        d_sp_cur = row["d_SP"] if row else None
        fav_cur  = row["fav"]  if row else None
        kickoff_iso = row["kickoff"] if row else None

        needs_sp = (h_sp_cur is None or a_sp_cur is None or d_sp_cur is None)
        needs_fav = (fav_cur is None)

        def _parse_kickoff_dt(iso_str: str):
            if not iso_str:
                return None
            try:
                return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
            except Exception:
                return None

        def _best_back_price(r):
            ex = getattr(r, "ex", None)
            atb = getattr(ex, "available_to_back", None) if ex else None
            if atb and len(atb) > 0:
                return atb[0].price
            return None

        # Determine whether we are allowed to capture a snapshot now
        capture_now = False

        ko_dt = _parse_kickoff_dt(kickoff_iso)
        now_utc = datetime.now(timezone.utc)

        # 1) Pre-kickoff window capture (preferred)
        if ko_dt is not None:
            seconds_to_ko = (ko_dt - now_utc).total_seconds()
            if 0 <= seconds_to_ko <= SP_CAPTURE_WINDOW_SEC:
                capture_now = True

        # 2) Fallback: first in-play capture if we missed the window
        # (only if still NULL and you want this behaviour)
        if not capture_now and SP_FALLBACK_INPLAY and needs_sp:
            # Use either time_elapsed or inplay_status as your "in play started" signal
            te = ev.get("time_elapsed")
            ips = ev.get("inplay_status")
            if (isinstance(te, (int, float)) and int(te) >= 0) or (ips in ("KickOff", "InPlay", "SecondHalfKickOff")):
                capture_now = True

        if capture_now and len(runners) >= 3:
            h_snap = _best_back_price(runners[0])
            a_snap = _best_back_price(runners[1])
            d_snap = _best_back_price(runners[2])

            updates = {}

            # Only write once
            if h_sp_cur is None and h_snap is not None:
                updates["h_SP"] = float(h_snap)
            if a_sp_cur is None and a_snap is not None:
                updates["a_SP"] = float(a_snap)
            if d_sp_cur is None and d_snap is not None:
                updates["d_SP"] = float(d_snap)

            # Favourite derived from (effective) snapshot prices (home vs away)
            effective_h = h_sp_cur if h_sp_cur is not None else h_snap
            effective_a = a_sp_cur if a_sp_cur is not None else a_snap

            if needs_fav and effective_h is not None and effective_a is not None:
                if float(effective_h) < float(effective_a):
                    updates["fav"] = 1  # home fav
                elif float(effective_a) < float(effective_h):
                    updates["fav"] = 2  # away fav
                else:
                    updates["fav"] = 0  # equal

            if updates:
                db.update_current(event_id, **updates)

    # ========== GOAL TIMELINE LOGIC ==========
    def _update_goal_timeline(
        self,
        db: DBHelper,
        event_id: str,
        time_elapsed: Optional[int],
        inplay_status: Optional[str],
        h_score: Optional[int],
        a_score: Optional[int],
        ft_score: Optional[str],
    ) -> None:
        """Update h_goalsXX/a_goalsXX bands at 15-min intervals (write-once per band, backfill on finish)."""

        row = db.fetch_current(event_id)
        if not row:
            return

        # Current stored bands
        h15, a15 = row["h_goals15"], row["a_goals15"]
        h30, a30 = row["h_goals30"], row["a_goals30"]
        h45, a45 = row["h_goals45"], row["a_goals45"]
        h60, a60 = row["h_goals60"], row["a_goals60"]
        h75, a75 = row["h_goals75"], row["a_goals75"]
        h90, a90 = row["h_goals90"], row["a_goals90"]

        def maybe_write(tag: str, cur_h, cur_a):
            if h_score is not None and a_score is not None:
                db.update_current(event_id, **{f"h_goals{tag}": int(h_score), f"a_goals{tag}": int(a_score)})

        # Write once when we enter each band (first value wins)
        if isinstance(time_elapsed, (int, float)):
            t = int(time_elapsed)
            if t <= 15:
                maybe_write("15", h15, a15)
            elif 15 < t <= 30:
                maybe_write("30", h30, a30)
            elif 30 < t <= 45:
                maybe_write("45", h45, a45)
            elif 45 < t <= 60:
                maybe_write("60", h60, a60)
            elif 60 < t <= 75:
                maybe_write("75", h75, a75)
            elif t > 75:
                maybe_write("90", h90, a90)

        # Backfill missing bands at finish using FT
        if inplay_status == "Finished":
            fth, fta = None, None
            if ft_score and "-" in ft_score:
                try:
                    left, right = ft_score.split("-", 1)
                    fth, fta = int(left.strip()), int(right.strip())
                except Exception:
                    fth, fta = None, None

            # Fallback to last known scores if ft_score malformed/missing
            if fth is None or fta is None:
                if h_score is not None and a_score is not None:
                    fth, fta = int(h_score), int(a_score)

            if fth is None or fta is None:
                return

            for tag, cur_h, cur_a in [
                ("15", h15, a15),
                ("30", h30, a30),
                ("45", h45, a45),
                ("60", h60, a60),
                ("75", h75, a75),
                ("90", h90, a90),
            ]:
                if cur_h is None and cur_a is None:
                    db.update_current(event_id, **{f"h_goals{tag}": fth, f"a_goals{tag}": fta})
    
    def _parse_ft(self, ft: str) -> tuple[Optional[int], Optional[int]]:
        if not ft or "-" not in ft:
            return None, None
        try:
            left, right = ft.split("-", 1)
            return int(left.strip()), int(right.strip())
        except Exception:
            return None, None

    # ========== ARCHIVE LOGIC ===========
    def decide_to_archive(self, db: DBHelper, api, ev: Dict[str, Any]):
        inplay_status = ev["inplay_status"]
        event_id = ev["event_id"]
        h_score = ev["h_score"]
        a_score = ev["a_score"]

        # If finished, set FT score (once)
        if inplay_status == "Finished" and h_score is not None and a_score is not None:
            db.update_current(event_id, ft_score=f"{int(h_score)}-{int(a_score)}")

        # ---- ARCHIVE ON FINISH ----
        if inplay_status == "Finished":
            # Re-read row to ensure ft_score exists (DBHelper enforces ft_score for archive)
            row_now = db.fetch_current(event_id)
            if row_now and row_now["ft_score"]:
                ft = row_now["ft_score"]
                if ft and "-" in ft:
                    try:
                        left, right = ft.split("-", 1)
                        fth, fta = int(left.strip()), int(right.strip())
                    except Exception:
                        fth, fta = None, None
                else:
                    fth, fta = None, None

                # If parsing failed but we have live scores, use them
                if (fth is None or fta is None) and h_score is not None and a_score is not None:
                    fth, fta = int(h_score), int(a_score)
                    ft = f"{fth}-{fta}"
                    db.update_current(event_id, ft_score=ft)

                # Set result (1 decisive, 0 draw) and archive
                result_val = self._compute_result(fth, fta)
                if result_val is not None:
                    db.update_current(event_id, result=result_val)

                # Calculate PnL
                pnl = BaseStrategy.calculate_pnl(self, logger=logger, ev=ev, result_val=result_val)

                db.update_current(event_id, inplay_status="Finished", ft_score=ft, result=result_val, pnl=pnl)

                # ===== LOGGING ARCHIVE ==========
                remaining = db.conn.execute("SELECT COUNT(*) FROM current_matches").fetchone()[0]

                logger.info(
                    "FINISH | %s | %s | FT=%s | result=%s | pnl=%s | strat=%s | remaining_current=%s",
                    row_now["comp"],
                    row_now["event_name"],
                    row_now["ft_score"],
                    result_val,
                    pnl,
                    row_now["strategy"],
                    remaining - 1  # because we are about to remove it
                )

                # IMPORTANT: pnl stays NULL unless a strategy sets it
                # strategy should be 'None' if none assigned; that’s already your schema expectation
                db.archive_match(event_id)

        # ========== ARCHIVE IF COMPLETE BUT NOT 'FINISHED' ==========
        # Archive if not 'Finished' but enough time has passed after kickoff and data recorded.
        kickoff = ev["kickoff"] if ev else None
        if kickoff and inplay_status not in ("Finished", "Cancelled", "Abandoned"):
            try:
                ko_dt = datetime.fromisoformat(kickoff.replace("Z", "+00:00"))
                if datetime.now(timezone.utc) - ko_dt > timedelta(hours=4):
                    # print('test 3 not finished archive', ev['event_id'])
                    # Force finish using last known score
                    ft = ev["ft_score"] or (f"{int(h_score)}-{int(a_score)}" if h_score is not None and a_score is not None else None)
                    if ft:
                        fth, fta = self._parse_ft(ft)
                        result_val = self._compute_result(fth, fta)
                        

                        # Calculate PnL
                        pnl = BaseStrategy.calculate_pnl(self, logger=logger, ev=ev, result_val=result_val)
                        
                        db.update_current(event_id, inplay_status="Finished", ft_score=ft, result=result_val, pnl=pnl)
                        # ===== LOGGING ARCHIVE ==========
                        remaining = db.conn.execute("SELECT COUNT(*) FROM current_matches").fetchone()[0]

                        logger.info(
                            "FINISH | %s | %s | FT=%s | result=%s | pnl=%s | strat=%s | remaining_current=%s",
                            event_id,
                            ev["event_name"],
                            ev["ft_score"],
                            result_val,
                            pnl,
                            ev["strategy"],
                            remaining - 1  # because we are about to remove it
                        )
                        # Archive match
                        db.archive_match(event_id)
            except Exception:
                pass
        
        # ========== DELETE IF UNUSEABLE ==========
        # Delete from current and do not archive if no data has been recorded or partially recorded and unuseable.
        
        # Check if 2 days after kickoff, ft_score = NULL, only partial or none of intervals recorded.
        try:
            ko_dt = datetime.fromisoformat(kickoff.replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - ko_dt > timedelta(days=1):
                print('TEST - DECIDE TO ARCHIVE: DELETE 1')
                if kickoff and (inplay_status not in ("Finished", "Cancelled", "Abandoned") or inplay_status == None) and not ev['ft_score'] and ((ev['time_elapsed'] and int(ev['time_elapsed']) < 90 ) or not ev['time_elapsed']):
                    print('TEST - DECIDE TO ARCHIVE: DELETE 2')
                    # ===== LOGGING ARCHIVE ==========
                    remaining = db.conn.execute("SELECT COUNT(*) FROM current_matches").fetchone()[0]

                    logger.info(
                        "DELETE | %s | %s | reason=NO DATA| remaining_current=%s",
                        event_id,
                        ev["event_name"],
                        remaining - 1  # because we are about to remove it
                    )
                    # Archive match
                    db.delete_from_current(event_id)
        except Exception:
            pass


if __name__ == "__main__":
    AutoTrader().start()
