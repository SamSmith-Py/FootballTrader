# create_db_structure.py
import os
import sqlite3

DB_PATH = r"C:\Users\Sam\FootballTrader v0.3.3\database\autotrader_data.db"

SCHEMA = {
    "archive_v3": """
    CREATE TABLE IF NOT EXISTS archive_v3 (
        comp        TEXT NOT NULL,
        comp_id       INTEGER,
        country_code    TEXT,
        event_name    TEXT NOT NULL,
        event_id      TEXT NOT NULL UNIQUE,   -- Betfair event id (unique)
        kickoff       TEXT NOT NULL,          -- ISO8601 UTC
        inplay_status TEXT,                   -- 'Finished' expected here
        ft_score      TEXT NOT NULL,          -- e.g. '2-1' (ENFORCED per your choice)
        ht_score      TEXT,
        h_score       INTEGER,
        a_score       INTEGER,
        h_goals15     INTEGER, a_goals15 INTEGER,
        h_goals30     INTEGER, a_goals30 INTEGER,
        h_goals45     INTEGER, a_goals45 INTEGER,
        h_goals60     INTEGER, a_goals60 INTEGER,
        h_goals75     INTEGER, a_goals75 INTEGER,
        h_goals90     INTEGER, a_goals90 INTEGER,
        h_red_cards   INTEGER,
        a_red_cards   INTEGER,
        h_SP          REAL,
        a_SP          REAL,
        d_SP          REAL,
        fav           INTEGER,                -- 1=home, 2=away
        paper         INTEGER,                -- 0=live, 1=paper
        result        INTEGER,                -- 1=decisive, 0=draw
        pnl           REAL,                   -- realised PnL for this match
        bot_v         TEXT,                   -- bot version string
        h_team        TEXT,
        a_team        TEXT,
        strategy      TEXT,
        market        TEXT,
        created_ts    TEXT DEFAULT (datetime('now','utc'))
    );
    """,

    "current_matches": """
    CREATE TABLE IF NOT EXISTS current_matches (
        comp        TEXT NOT NULL,
        comp_id       INTEGER,
        country_code  TEXT,
        event_name    TEXT NOT NULL,
        event_id      TEXT NOT NULL UNIQUE,   -- Unique while match is live
        kickoff       TEXT NOT NULL,          -- ISO8601 UTC
        inplay_status TEXT,                   -- 'KickOff','FirstHalfEnd','SecondHalfKickOff','Finished',...
        time_elapsed  TEXT,
        ft_score      TEXT,
        ht_score      TEXT,
        h_score       INTEGER,
        a_score       INTEGER,
        h_goals15     INTEGER, a_goals15 INTEGER,
        h_goals30     INTEGER, a_goals30 INTEGER,
        h_goals45     INTEGER, a_goals45 INTEGER,
        h_goals60     INTEGER, a_goals60 INTEGER,
        h_goals75     INTEGER, a_goals75 INTEGER,
        h_goals90     INTEGER, a_goals90 INTEGER,
        h_red_cards   INTEGER,
        a_red_cards   INTEGER,
        h_SP          REAL,
        a_SP          REAL,
        d_SP          REAL,
        fav           INTEGER,
        paper         INTEGER,
        h_back_price       REAL,
        a_back_price       REAL,
        d_back_price       REAL,
        h_lay_price       REAL,
        a_lay_price       REAL,
        d_lay_price       REAL,
        result        INTEGER,
        pnl           REAL,
        e_ordered     INTEGER,
        e_price       REAL,
        e_matched     REAL,
        e_remaining   REAL,
        e_stake       REAL,
        e_betid       TEXT,
        e_side        TEXT,
        e_status      TEXT,
        liability     REAL,
        x_ordered     INTEGER,
        x_price       REAL,
        x_matched     REAL,
        x_remaining   REAL,
        x_stake       REAL,
        x_betid       TEXT,
        x_side        TEXT,
        x_status      TEXT,
        bot_v         TEXT,
        h_team        TEXT,
        a_team        TEXT,
        strategy      TEXT,
        market        TEXT,
        market_state  TEXT,
        market_id_MATCH_ODDS     TEXT,
        market_id_OU45     TEXT,
        market_id_CS     TEXT,
        created_ts    TEXT DEFAULT (datetime('now','utc')),
        updated_ts    TEXT
    );
    """,

    "match_stream_history": """
    CREATE TABLE IF NOT EXISTS match_stream_history (
        comp      TEXT,
        event_name  TEXT,
        event_id    TEXT NOT NULL,
        h_price     REAL,
        a_price     REAL,
        d_price     REAL,
        h_score     INTEGER,
        a_score     INTEGER,
        inplay_time INTEGER,       -- minutes
        h_red_cards INTEGER,
        a_red_cards INTEGER,
        timestamp   TEXT NOT NULL  DEFAULT (datetime('now','utc'))
    );
    """
}

INDEXES = [
    # Helpful read paths
    "CREATE INDEX IF NOT EXISTS idx_current_kickoff ON current_matches(kickoff);",
    "CREATE INDEX IF NOT EXISTS idx_current_comp ON current_matches(comp);",
    "CREATE INDEX IF NOT EXISTS idx_stream_event_ts ON match_stream_history(event_id, timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_archive_kickoff ON archive_v3(kickoff);",
]

PRAGMAS_BOOT = [
    "PRAGMA foreign_keys = ON;",
    "PRAGMA journal_mode = WAL;",     # better concurrency
    "PRAGMA synchronous = NORMAL;",   # speed vs durability tradeoff (fine for WAL)
    "PRAGMA wal_autocheckpoint = 1000;",
    "PRAGMA auto_vacuum = FULL;"      # reclaim space
]

def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def main():
    ensure_dir(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        for p in PRAGMAS_BOOT:
            cur.execute(p)

        # Create tables
        for name, ddl in SCHEMA.items():
            cur.execute(ddl)

        # Create indexes
        for idx in INDEXES:
            cur.execute(idx)

        conn.commit()

        # For auto_vacuum change to take effect immediately:
        cur.execute("VACUUM;")
        conn.commit()

        print("Database initialised:")
        print(" - WAL mode ON, FULL auto-vacuum set")
        print(" - Tables: archive_v3, current_matches, match_stream_history")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
