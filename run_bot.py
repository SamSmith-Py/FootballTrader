"""
run_bot.py — Main entrypoint for FootballTrader v0.3.3

Responsibilities:
  • Load config
  • Start MatchFinder scheduler in background
  • Run AutoTrader in main thread
  • Handle graceful shutdown (Ctrl+C)
"""

import os
import logging
import time
import threading
from pathlib import Path

from core.config_loader import load_betfair_credentials
from core.settings import BOT_VERSION, SCHEDULE_MATCHFINDER_MIN
from autotrader.scheduler import start_scheduler, stop_scheduler
from autotrader.autotrader import AutoTrader

# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------
logger = logging.getLogger("run_bot")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)


# -----------------------------------------------------------------------------
# Guard to ensure scheduler only starts once
# -----------------------------------------------------------------------------
_scheduler_started = threading.Event()


def safe_start_scheduler():
    """Ensure scheduler starts only once globally."""
    if not _scheduler_started.is_set():
        start_scheduler()
        _scheduler_started.set()
    else:
        logger.warning("Scheduler already running, skipping duplicate start.")


# -----------------------------------------------------------------------------
# Main entrypoint
# -----------------------------------------------------------------------------
def main():
    logger.info("Bot starting... (FootballTrader %s)", BOT_VERSION)
    # cwd = os.getcwd()
    # logger.info("CWD = %s", cwd)

    # Prefer absolute path anchored to this file, not the shell working directory
    base_dir = Path(__file__).resolve().parent
    config_path = base_dir / "config" / "config.ini"
    logger.info("config.ini exists = %s", config_path.exists())
    if not config_path.exists():
        logger.error("Missing config.ini! Bot cannot start.")
        return

    # Load config/credentials
    load_betfair_credentials(str(config_path))

    # Start scheduler in the background
    safe_start_scheduler()
    logger.info("Scheduler active: MatchFinder every %s minutes.", SCHEDULE_MATCHFINDER_MIN)

    # Run AutoTrader in the foreground (this blocks forever)
    trader = AutoTrader()
    try:
        trader.start()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (CTRL+C).")
    except Exception as e:
        logger.exception("AutoTrader crashed: %s", e)
    finally:
        # Stop scheduler cleanly (optional, but tidy)
        try:
            stop_scheduler()
        except Exception:
            pass
        logger.info("FootballTrader shutting down.")


# -----------------------------------------------------------------------------
# Entrypoint guard
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
