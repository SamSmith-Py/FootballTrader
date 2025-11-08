import logging
import time
from autotrader.scheduler import start_scheduler
# from autotrader import AutoTrader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")

logger = logging.getLogger("scheduler")

if __name__ == "__main__":
    logger.info("Bot starting...")

    # start scheduler
    start_scheduler()

    # keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down bot...")

    # Start AutoTrader (blocking loop)
    # trader = AutoTrader()
    # trader.start()