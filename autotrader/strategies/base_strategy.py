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
            "league": ev.get("league"),
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
        db.insert_stream(fields)
