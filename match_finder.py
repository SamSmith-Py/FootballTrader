import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
import re


import pandas as pd
import sqlite3
import betfairlightweight
from betfairlightweight import filters

from core.settings import (
    BOT_VERSION, PAPER_MODE,
    DB_PATH, CONFIG_PATH,
    LOG_DIR, LOG_MATCHFINDER_FILE, LOG_LEVEL, LOG_ROTATION_WHEN, LOG_ROTATION_BACKUPS,
    BETFAIR_HOURS_LOOKAHEAD, MARKETS_REQUIRED, BETFAIR_CATALOGUE_MAX_RESULTS,
    TABLE_CURRENT
)
from core.betfair_session import BetfairSession
from core.config_loader import load_betfair_credentials

# ---- Use your existing DB helper (imported from your file) ----
from core.db_helper import DBHelper  # must be available in PYTHONPATH

# =========================================
# Logging setup (console + daily rotating file)
# =========================================
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("matchfinder")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# Console handler
ch = logging.StreamHandler()
ch.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

# Timed rotating file handler (midnight rollover)
fh = TimedRotatingFileHandler(
    filename=str(LOG_MATCHFINDER_FILE),
    when=LOG_ROTATION_WHEN,  # e.g., 'midnight'
    backupCount=LOG_ROTATION_BACKUPS,
    encoding="utf-8",
)
fh.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

# Avoid duplicate handlers on rerun
if not logger.handlers:
    logger.addHandler(ch)
    logger.addHandler(fh)

# =========================================
# MatchFinder: fetch & upsert into current_matches
# =========================================
class MatchFinder:
    def __init__(self, hours: int = BETFAIR_HOURS_LOOKAHEAD):
        self.hours = hours

    def run(self) -> None:
        logger.info("MatchFinder run started")
        
        username, password, app_key = load_betfair_credentials(CONFIG_PATH)
        
        
        with BetfairSession(username, password, app_key) as api:
            
            rows = self._fetch_catalogue(api)

            if not rows:
                logger.info("No catalogue rows returned.")
                return

            # Build market-specific frames
            df_mo = self._build_market_df(rows, target="MATCH_ODDS")
            df_ou45 = self._build_market_df(rows, target="OVER_UNDER_45",
                                            exact_market_name="Over/Under 4.5 Goals")
            df_cs = self._build_market_df(rows, target="CORRECT_SCORE")

            # Merge on event_id (preserve union of all events seen)
            base = self._build_base_df([df_mo, df_ou45, df_cs])

            # Upsert
            self._upsert_into_current_matches(base)

            logger.info("MatchFinder run finished.")

    # ---------- Fetch whole catalogue window ----------
    def _fetch_catalogue(self, api) -> List[Dict[str, Any]]:
        market_filter = filters.market_filter(
            event_type_ids=[1],  # 1 = Soccer
            market_type_codes=list(set(MARKETS_REQUIRED)),
            in_play_only=False,
            market_start_time={
                "from": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": (datetime.now(timezone.utc) + timedelta(hours=self.hours)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )

        logger.info(f"Fetching catalogue for next {self.hours}h | markets={MARKETS_REQUIRED}")
        cats = api.betting.list_market_catalogue(
            filter=market_filter,
            market_projection=["COMPETITION", "EVENT", "MARKET_START_TIME", "RUNNER_DESCRIPTION"],
            max_results=BETFAIR_CATALOGUE_MAX_RESULTS,
            lightweight=False,
        )
        # print(cats)
        # Normalize to list of dicts for robust downstream handling
        rows = []
        for mc in cats:
            # 'mc' is MarketCatalogue (obj); turn into dict shape we need
            try:
                raw_comp = getattr(mc.competition, "name", None)
                comp_name = self._clean_league_name(raw_comp)

                event_name = getattr(mc.event, "name", None)
                event_id = getattr(mc.event, "id", None)
                market_id = getattr(mc, "market_id", None)
                market_name = getattr(mc, "market_name", None)
                market_type = getattr(mc, "market_name", None)  # we still rely on name + type below
                mtype_code = getattr(mc, "market_type", None) if hasattr(mc, "market_type") else None
                mst = getattr(mc, "market_start_time", None)

                # Runners (for teams from MATCH_ODDS)
                home_team = None
                away_team = None
                try:
                    runners = getattr(mc, "runners", []) or []
                    if len(runners) >= 2:
                        home_team = getattr(runners[0], "runner_name", None)
                        away_team = getattr(runners[1], "runner_name", None)
                except Exception:
                    pass

                rows.append({
                    "competition": comp_name,
                    "event_name": event_name,
                    "event_id": event_id,
                    "market_id": market_id,
                    "market_name": market_name,
                    "market_type_code": mtype_code,   # may be None in lightweight; we’ll check name too
                    "market_start_time": mst,
                    "h_team_guess": home_team,
                    "a_team_guess": away_team,
                })
            except Exception as e:
                logger.debug(f"Skipping malformed catalogue row: {e}")
                continue

        logger.info(f"Catalogue rows fetched: {len(rows)}")
        return rows

    # ---------- Build a DataFrame for a specific market ----------
    def _build_market_df(self, rows: List[Dict[str, Any]], target: str,
                         exact_market_name: Optional[str] = None) -> pd.DataFrame:
        """
        target in {"MATCH_ODDS","OVER_UNDER_45","CORRECT_SCORE"}
        If exact_market_name is given, additionally require market_name to match e.g. 'Over/Under 4.5 Goals'.
        Returns columns: event_id, league, event_name, kickoff, (h_team, a_team only if MATCH_ODDS),
                         market_id_<TARGET>
        """
        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=["event_id"])

        # Identify rows for this market:
        # - Some installations don’t expose market_type_code in lightweight; we rely on market_name contains
        #   'Match Odds' / 'Over/Under 4.5 Goals' / 'Correct Score'
        target_name_map = {
            "MATCH_ODDS": "Match Odds",
            "OVER_UNDER_45": "Over/Under 4.5 Goals",
            "CORRECT_SCORE": "Correct Score",
        }
        wanted_name = exact_market_name or target_name_map[target]

        mdf = df[df["market_name"].fillna("").str.casefold() == wanted_name.casefold()].copy()
        if mdf.empty and target == "MATCH_ODDS":
            # Fallback: sometimes name casing differs; already handled with casefold, but keep guard.
            logger.warning("No MATCH_ODDS markets matched by name; leaving empty frame.")
        elif mdf.empty:
            logger.info(f"No {target} markets in this window; leaving empty frame.")

        # Normalize core columns
        mdf["league"] = mdf["competition"]
        mdf["kickoff"] = pd.to_datetime(mdf["market_start_time"], errors="coerce", utc=True)

        out_cols = ["event_id", "league", "event_name", "kickoff"]

        # Teams (only from MATCH_ODDS reliably)
        if target == "MATCH_ODDS":
            mdf["h_team"] = mdf.get("h_team_guess")
            mdf["a_team"] = mdf.get("a_team_guess")
            out_cols += ["h_team", "a_team"]

        # Market id column name
        market_col = {
            "MATCH_ODDS": "market_id_MATCH_ODDS",
            "OVER_UNDER_45": "market_id_OU45",
            "CORRECT_SCORE": "market_id_CS",
        }[target]
        mdf[market_col] = mdf["market_id"]

        return mdf[out_cols + [market_col]].dropna(subset=["event_id"]).drop_duplicates("event_id")

    # ---------- Make a base DF from any of the market DFs ----------
    def _build_base_df(self, frames: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Outer-merge the three market-specific DFs by event_id so that:
          - league / event_name / kickoff are preserved when available (prefer MATCH_ODDS source),
          - market_id_* columns are present; missing values are None,
          - h_team/a_team present if MATCH_ODDS available.
        """
        # Choose a priority order so that base has h_team/a_team if possible
        # Priority: MATCH_ODDS -> OU45 -> CS (for league/event_name/kickoff)
        by_name = {f.columns[-1]: f for f in frames if not f.empty}
        df_mo = by_name.get("market_id_MATCH_ODDS")
        df_ou = by_name.get("market_id_OU45")
        df_cs = by_name.get("market_id_CS")

        # Start from whatever exists
        base = df_mo if df_mo is not None else (df_ou if df_ou is not None else (df_cs if df_cs is not None else None))
        if base is None:
            return pd.DataFrame(columns=[
                "event_id", "league", "event_name", "kickoff", "h_team", "a_team",
                "market_id_MATCH_ODDS", "market_id_OU45", "market_id_CS"
            ])

        # Merge in the others
        out = base.copy()
        for other in [df_ou, df_cs]:
            if other is not None:
                out = out.merge(other, on="event_id", how="outer", suffixes=("", "_dup"))

                # Fill missing core fields from the newly merged chunk if base had NA
                for col in ["league", "event_name", "kickoff", "h_team", "a_team"]:
                    if col in out.columns and f"{col}_dup" in out.columns:
                        out[col] = out[col].where(out[col].notna(), out[f"{col}_dup"])
                        out.drop(columns=[f"{col}_dup"], inplace=True, errors="ignore")

                # Clean any *_dup market ids
                for mcol in ["market_id_MATCH_ODDS", "market_id_OU45", "market_id_CS"]:
                    if f"{mcol}_dup" in out.columns:
                        out[mcol] = out[mcol].where(out[mcol].notna(), out[f"{mcol}_dup"])
                        out.drop(columns=[f"{mcol}_dup"], inplace=True, errors="ignore")

        # Ensure all market_id columns present
        for mcol in ["market_id_MATCH_ODDS", "market_id_OU45", "market_id_CS"]:
            if mcol not in out.columns:
                out[mcol] = None

        # Ensure final column order
        out = out[[
            "event_id", "league", "event_name", "kickoff",
            "h_team", "a_team",
            "market_id_MATCH_ODDS", "market_id_OU45", "market_id_CS"
        ]].drop_duplicates("event_id")

        return out

    # ---------- Upsert into current_matches ----------
    def _upsert_into_current_matches(self, df: pd.DataFrame) -> None:
        if df.empty:
            logger.info("No rows to upsert into current_matches.")
            return
        df = df[df["league"].notna()].copy()

        logger.info(f"Upserting {len(df)} rows into {TABLE_CURRENT}...")
        with DBHelper(DB_PATH) as db:
            # (Optional) ensure table exists / columns known — assuming you already created
            for _, row in df.iterrows():
                payload = {
                    "event_id": str(row["event_id"]),
                    "league": (row.get("league") or None),
                    "event_name": (row.get("event_name") or None),
                    "kickoff": (row.get("kickoff").isoformat() if pd.notna(row.get("kickoff")) else None),
                    "h_team": (row.get("h_team") or None),
                    "a_team": (row.get("a_team") or None),
                    "market_id_MATCH_ODDS": (row.get("market_id_MATCH_ODDS") or None),
                    "market_id_OU45": (row.get("market_id_OU45") or None),
                    "market_id_CS": (row.get("market_id_CS") or None),

                    # Defaults on insert (won’t overwrite existing values thanks to ON CONFLICT update rule)
                    "paper": PAPER_MODE,         # 1 = paper, 0 = live
                    "bot_v": BOT_VERSION,

                    # Placeholders the bot fills later
                    "strategy": None,
                    "market": None,
                    "market_state": None,
                }
                
                db.upsert_current(payload)

        logger.info("Upsert complete.")
    
    def _clean_league_name(self, name: str) -> str:
        """
        Normalises Betfair competition names into 'Country, League' format.
        """
        if not name:
            return None

        name = name.replace("  ", " ").strip()

        # Split by first space or hyphen
        parts = re.split(r"\s+|-", name, maxsplit=1)

        if len(parts) == 2:
            country, league = parts
            return f"{country.strip()}, {league.strip()}"

        return name  # fallback: leave unchanged


# ========= Entry point =========
if __name__ == "__main__":
    try:
        print('test')
        mf = MatchFinder(hours=BETFAIR_HOURS_LOOKAHEAD)
        mf.run()
    except Exception as e:
        logger.exception(f"MatchFinder crashed: {e}")
        raise
