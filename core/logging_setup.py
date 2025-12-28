import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from core.settings import (LOG_DIR, LOG_AUTOTRADER_FILE, LOG_STRATEGY_FILE, LOG_ROTATION_WHEN, LOG_ROTATION_BACKUPS)

def setup_bot_logging(log_dir: str, level=logging.INFO):
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s | %(message)s")

    root = logging.getLogger("AutoTrader")
    root.setLevel(level)

    # Console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(level)
    root.addHandler(ch)
    
    # Rotating file
    fh = TimedRotatingFileHandler(
        filename=str(Path(LOG_AUTOTRADER_FILE)),
        when=LOG_ROTATION_WHEN,
        backupCount=LOG_ROTATION_BACKUPS,
        encoding="utf-8"
    )
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)
    root.addHandler(fh)

    # Optional: debug file
    dh = RotatingFileHandler(
        filename=str(Path(log_dir) / "bot.debug.log"),
        maxBytes=10_000_000,
        backupCount=5,
        encoding="utf-8"
    )
    dh.setFormatter(fmt)
    dh.setLevel(logging.DEBUG)
    root.addHandler(dh)

    return root

def setup_LTD60_logging(log_dir: str, level=logging.INFO):
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s | %(message)s")

    root = logging.getLogger("LTD60")
    root.setLevel(level)

    # Console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(level)
    root.addHandler(ch)
    
    # Rotating file
    fh = TimedRotatingFileHandler(
        filename=str(Path(LOG_STRATEGY_FILE)),
        when=LOG_ROTATION_WHEN,
        backupCount=LOG_ROTATION_BACKUPS,
        encoding="utf-8"
    )
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)
    root.addHandler(fh)

    # Optional: debug file
    dh = RotatingFileHandler(
        filename=str(Path(log_dir) / "LTD60.debug.log"),
        maxBytes=10_000_000,
        backupCount=5,
        encoding="utf-8"
    )
    dh.setFormatter(fmt)
    dh.setLevel(logging.DEBUG)
    root.addHandler(dh)

    return root
