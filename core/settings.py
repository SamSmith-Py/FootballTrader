from pathlib import Path

# ================= BOT META =================
BOT_VERSION = "v0.3.3"     # Stored in DB for audit/history
PAPER_MODE  = 1            # 1 = Paper (simulate), 0 = Live (real bets)

# ================= PATHS ====================
BASE_DIR    = Path(__file__).resolve().parent.parent
DB_PATH     = BASE_DIR / "database" / "autotrader_data.db"
CONFIG_PATH =  BASE_DIR / "config" / "config.ini"


# Backtest outputs (used for strategy assignment)
BACKTEST_DIR          = BASE_DIR / "data_analysis"
FILTERED_LEAGUES_CSV  = BACKTEST_DIR / "filtered_leagues_2.csv"
LATE_GOAL_LEAGUES_CSV = BACKTEST_DIR / "late_goal_leagues_2.csv"
FILTERED_LEAGUES_CSV_V3  = BACKTEST_DIR / "filtered_leagues_3.csv"
LATE_GOAL_LEAGUES_CSV_V3 = BACKTEST_DIR / "late_goal_leagues_3.csv"

# ================= LOGGING ==================
LOG_DIR = BASE_DIR / "logs"
LOG_MATCHFINDER_FILE = LOG_DIR / "matchfinder.log"
LOG_AUTOTRADER_FILE = LOG_DIR / "autotrader.log"
LOG_STRATEGY_FILE = LOG_DIR / "strategy.log"
LOG_LEVEL = "INFO"          # DEBUG/INFO/WARNING/ERROR
LOG_ROTATION_WHEN = "midnight"   # daily rotation
LOG_ROTATION_BACKUPS = 10        # keep last 10 days

# ================= DB TABLE NAMES ===========
TABLE_ARCHIVE = "archive_v3"
TABLE_CURRENT = "current_matches"
TABLE_STREAM  = "match_stream_history"

# ================= MATCHFINDER / SCHEDULER ==
BETFAIR_HOURS_LOOKAHEAD = 12   # Fetch markets up to X hours from now
SCHEDULE_MATCHFINDER_MIN = 30  # How often to run MatchFinder
# We’ll fetch all three; if a specific market is missing in a run it stays NULL and will be updated on a later run
MARKETS_REQUIRED = ["MATCH_ODDS", "OVER_UNDER_45", "CORRECT_SCORE"]
# Optional: pagination cap for catalogue results
BETFAIR_CATALOGUE_MAX_RESULTS = 1000

# ================= AUTOTRADER ===============
SP_CAPTURE_WINDOW_SEC = 90   # take snapshot inside last 90s pre-KO
SP_FALLBACK_INPLAY = True    # if missed pre-KO, capture once at first in-play


# ================= STRATEGIES ===============
# Keep these strings — they’re used in DB rows
STRAT_LTD60 = "LTD60"

# Stakes (live/paper)
STAKE_LTD_PAPER = 100.0
STAKE_LTD_LIVE  = 4.0

# LTD60 config
LTD60_MAX_ODDS_ACCEPT = 4.5        # max draw odds for entry
LTD60_MAX_SECOND_ENTRY_ODDS = 2.5      # assumed odds for second lay at 60'
LTD60_FT_COMMISSION = 0.02         # 2% commission
LTD60_KO_WINDOW_MINUTES = 12 

LTD60_SECOND_ENTRY_TIME = 60  # Time in play to trigger second entry

# ================= STREAMING =================
# How often to poll prices/scores for history logging
STREAM_POLL_SECONDS = 10  # change here any time
PRICE_EPSILON = 1e-6      # float “changed” tolerance

# ==============SELECTION ID===================
DRAW_SELECTION_ID = 58805

# ============= BACK TEST ====================
BACKTEST_STAKE = 200
BACKTEST_MAX_PRICE = 4.5
BACKTEST_MAX_PRICE_ENTRY_2 = 2.5
BACKTEST_DECISIVE_COMP_MIN = 76
BACKTEST_LATE_GOAL_COMP_MIN = 70
MATCHES_MIN_PLAYED = 20