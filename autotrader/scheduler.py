"""
scheduler.py — Lightweight background scheduler for FootballTrader v0.3.3

Responsibilities:
  • Run MatchFinder automatically every X minutes
  • Run in its own background thread (daemon)
  • Log start/end + row count of each refresh
  • Thread-safe, no duplicate runs
"""

import logging
import threading
import time
from datetime import datetime, timedelta
import sqlite3

from core.settings import SCHEDULE_MATCHFINDER_MIN
from match_finder import MatchFinder

# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------
logger = logging.getLogger("scheduler")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)

# -----------------------------------------------------------------------------
# Scheduler state
# -----------------------------------------------------------------------------
_running = threading.Event()
_scheduler_thread: threading.Thread | None = None


def _run_matchfinder_job():
    """Execute a single MatchFinder run safely. Retry if failed."""
    for attempt in range(1, 6):
        try:
            logger.info("MatchFinder scheduled run starting...")
            mf = MatchFinder()
            rows = mf.run()
            logger.info("MatchFinder completed successfully (%s rows refreshed).", rows or 0)
            return
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                wait = 0.5 * attempt
                logger.warning("DB locked. Retry %s/5 in %.1fs", attempt, wait)
                time.sleep(wait)
                continue
            raise
        except Exception as e:
            logger.exception("MatchFinder job failed: %s", e)
            return


def _scheduler_loop():
    """Internal background loop."""
    next_run = datetime.now()

    while _running.is_set():
        now = datetime.now()
        if now >= next_run:
            _run_matchfinder_job()
            next_run = now + timedelta(minutes=SCHEDULE_MATCHFINDER_MIN)

        # Sleep with short interval for responsive shutdown
        for _ in range(60):
            if not _running.is_set():
                break
            time.sleep(1)


def start_scheduler():
    """
    Starts the scheduler thread if not already running.
    """
    global _scheduler_thread
    if _running.is_set():
        logger.warning("Scheduler already running, skipping duplicate start.")
        return

    _running.set()
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True)
    _scheduler_thread.start()
    logger.info(f"Scheduler started: MatchFinder every {SCHEDULE_MATCHFINDER_MIN} minutes.")


def stop_scheduler():
    """
    Stops the scheduler thread gracefully.
    """
    if _running.is_set():
        logger.info("Stopping scheduler thread...")
        _running.clear()
        if _scheduler_thread and _scheduler_thread.is_alive():
            _scheduler_thread.join(timeout=3)
        logger.info("Scheduler stopped.")
