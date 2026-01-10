"""
BaseStrategy — minimal, opinionated contract used by AutoTrader.

Strategies should:
- Decide if they apply to an event (assign_if_applicable)
- Run per-tick logic (on_tick)
- Use DBHelper for all state writes (no pandas required)
"""

from __future__ import annotations
from typing import Optional, Dict, Any
from core.settings import BOT_VERSION, PAPER_MODE
from core.db_helper import DBHelper


class BaseStrategy:
    name: str = "BASE"
    requires_api: bool = True  # flip to False if strategy never needs Betfair API

    # ---- Optional: override if strategy needs its own CSV config paths
    filtered_leagues_csv: Optional[str] = None
    late_goal_leagues_csv: Optional[str] = None

    def assign_if_applicable(self, db: DBHelper, ev: Dict[str, Any]) -> None:
        """
        Idempotently assign this strategy to current_matches row if eligible.
        Default impl: do nothing.
        """
        return

    def on_tick(self, db: DBHelper, ev: Dict[str, Any], api=None) -> None:
        """
        Called every loop for each event row in current_matches.
        Must be idempotent.
        """
        raise NotImplementedError

    # ---- Helpers to write to DB safely
    def _mark_strategy(self, db: DBHelper, ev_id: str, strategy: str, market: str) -> None:
        db.update_current(
            ev_id,
            strategy=strategy,
            market=market,
            bot_v=BOT_VERSION,
            paper=PAPER_MODE,
        )

    def _set_lay_prices(self, db: DBHelper, ev_id: str, h=None, a=None, d=None) -> None:
        updates = {}
        if h is not None: updates["h_lay_price"] = h
        if a is not None: updates["a_lay_price"] = a
        if d is not None: updates["d_lay_price"] = d
        if updates:
            db.update_current(ev_id, **updates)

    def _order_snapshot(self, db: DBHelper, ev_id: str, side: str, price: float, size: float,
                        status: str, matched: float = 0.0, remaining: float = 0.0, betid: str | None = None) -> None:
        """Store unified entry order fields for the strategy."""
        db.update_current(
            ev_id,
            e_ordered=1,
            e_side=side,
            e_price=price,
            e_stake=size,
            e_status=status,
            e_matched=matched,
            e_remaining=remaining,
            e_betid=betid,
        )

    def _log_stream(self, db: DBHelper, ev: Dict[str, Any], h=None, a=None, d=None, inplay_time: int | None = None):
        """Optional: append a row to match_stream_history for odds tracking."""
        fields = {
            "league": ev.get("comp"),
            "event_name": ev.get("event_name"),
            "event_id": ev.get("event_id"),
            "h_lay_price": h,
            "a_lay_price": a,
            "d_lay_price": d,
            "h_score": ev.get("h_score"),
            "a_score": ev.get("a_score"),
            "inplay_time": inplay_time,
            "h_red_cards": ev.get("h_red_cards"),
            "a_red_cards": ev.get("a_red_cards"),
        }
        db.log_stream(fields)

    def _sync_order_state(
        self,
        db: DBHelper,
        api,
        logger,
        ev: dict,
        market_id: str,
        prefix: str = "e",   # "e" for entry1, "x" for exit if you add later
    ) -> dict | None:
        """
        Sync an existing Betfair order with DB state.
        Returns a dict with latest order info, or None if no sync performed.
        """

        betid = ev.get(f"{prefix}_betid")
        stake = ev.get(f"{prefix}_stake") or 0.0
        matched = ev.get(f"{prefix}_matched") or 0.0
        price = ev.get(f"{prefix}_price") or 0.0
        cur_d_lay_price = ev.get("d_lay_price") or 0.0
        remaining = ev.get(f"{prefix}_remaining") or 0.0
        cur_liab = ev.get("liability") or 0.0
        status = ev.get("e_status") or 0.0
        

        if PAPER_MODE:
            # No order or already fully matched → nothing to do
            if stake <= 0 or matched == stake or stake == None:
                return None 
            # For LAY side Strats 
            if cur_d_lay_price <= price and status in ('PAPER_EXECUTED', 'PAPER_SECOND'):
                # Calculate liability
                liab = max(0.0, (float(price) - 1.0) * remaining) + cur_liab
                # Update current tables
                db.update_current(
                    ev["event_id"],
                    e_matched=stake,
                    e_remaining=0.0,
                    liability=liab)

        else:
            # No order or already fully matched → nothing to do
            if not betid or stake <= 0 or matched >= stake:
                return None

            try:
                resp = api.betting.list_current_orders(bet_ids=[betid])
                orders = resp.current_orders or []
            except Exception as e:
                self.log.error("ORDER_SYNC_FAIL | %s | betid=%s err=%s",
                            ev["event_id"], betid, e)
                return None

            # Order no longer exists at Betfair (fully settled/cancelled elsewhere)
            if not orders:
                db.update_current(
                    ev["event_id"],
                    **{
                        f"{prefix}_status": "MISSING",
                        f"{prefix}_remaining": 0.0,
                    }
                )
                return {
                    "status": "MISSING",
                    "matched": matched,
                    "remaining": 0.0,
                }

            o = orders[0]

            new_matched = float(o.size_matched or 0.0)
            new_remaining = float(o.size_remaining or 0.0)
            new_status = o.status  # EXECUTABLE, EXECUTION_COMPLETE, etc.

            # Only write if something actually changed
            if (
                new_matched != matched
                or new_remaining != ev.get(f"{prefix}_remaining")
                or new_status != ev.get(f"{prefix}_status")
            ):
                db.update_current(
                    ev["event_id"],
                    **{
                        f"{prefix}_matched": new_matched,
                        f"{prefix}_remaining": new_remaining,
                        f"{prefix}_status": new_status,
                    }
                )

                logger.info(
                    "ORDER_SYNC | %s | betid=%s status=%s matched=%.2f remaining=%.2f",
                    ev["event_id"], betid, new_status, new_matched, new_remaining
                )

            return {
                "status": new_status,
                "matched": new_matched,
                "remaining": new_remaining,
        }

    def calculate_pnl(self, logger, ev: Dict[str, Any], result_val):
        strat = ev.get("strategy")
        if strat is None:
            return None

        result = result_val
        stake_matched = ev.get("e_matched")
        liab = 0 - ev.get("liability")

        if result not in (0, 1) or stake_matched is None or liab is None:
            logger.error("PnL ERROR | %s | bad inputs result=%s stake=%s liability=%s",
                        ev.get("event_id"), result, stake_matched, liab)
            return None

        return stake_matched if result == 1 else liab

