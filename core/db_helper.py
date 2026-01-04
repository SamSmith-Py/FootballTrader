# db_helper.py
import sqlite3
from typing import Dict, Any, Iterable, Optional
from contextlib import contextmanager
from typing import Any, Dict, Optional, Tuple, List
import sqlite3
from datetime import datetime, timezone
from core.settings import TABLE_CURRENT, TABLE_STREAM, PRICE_EPSILON

class DBHelper:
    """
    Lightweight DB helper tailored to your schema.
    - Opens with WAL + sensible PRAGMAs.
    - event_id is UNIQUE in current_matches and archive_v3.
    - archive_match() MOVES row from current_matches -> archive_v3.
    """
    def __init__(self, db_path: str, check_same_thread: bool = False, timeout: float = 30.0):
        self.db_path = db_path
        self.conn = sqlite3.connect(
            db_path,
            check_same_thread=check_same_thread,
            timeout=timeout,   # <-- IMPORTANT
        )
        self.conn.row_factory = sqlite3.Row
        self._boot()

    def _boot(self):
        cur = self.conn.cursor()

        # Safe per-connection PRAGMAs
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("PRAGMA busy_timeout = 30000;")      # <-- IMPORTANT (ms)
        cur.execute("PRAGMA synchronous = NORMAL;")

        # DO NOT do these here (remove them):
        # cur.execute("PRAGMA journal_mode = WAL;")
        # cur.execute("PRAGMA wal_autocheckpoint = 1000;")
        # cur.execute("PRAGMA auto_vacuum = FULL;")

        self.conn.commit()

    def close(self):
        self.conn.close()

    @contextmanager
    def tx(self):
        try:
            yield
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    # ---------- helpers ----------
    @staticmethod
    def _now_utc() -> str:
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _kv_sql(fragment_for: Iterable[str]) -> str:
        """
        Builds 'col1=?, col2=?, ...' strings for UPDATE SET clauses.
        """
        return ", ".join([f"{k}=?" for k in fragment_for])

    # ---------- CURRENT_MATCHES ----------
    def upsert_or_update_current(self, event_id: str, fields: dict):
        """
        Insert row if event_id does not exist,
        otherwise update only passed fields.
        """
        exists = self.conn.execute(
        "SELECT 1 FROM current_matches WHERE event_id=?", (event_id,)
        ).fetchone()

        if exists:
            self.update_current(event_id, **fields)
        else:
            fields["event_id"] = event_id
            self.upsert_current(fields)

    def upsert_current(self, fields: Dict[str, Any]) -> None:
        """
        Insert or update a row in current_matches based on event_id (UNIQUE).
        Any keys not in the table are ignored silently.
        """
        if "event_id" not in fields:
            raise ValueError("upsert_current requires 'event_id'")

        # Add/update timestamp
        fields = dict(fields)
        fields.setdefault("created_ts", self._now_utc())
        fields["updated_ts"] = self._now_utc()

        # Read table columns to filter incoming dict
        cols = self._table_columns("current_matches")
        clean = {k: v for k, v in fields.items() if k in cols}

        keys = list(clean.keys())
        placeholders = ", ".join(["?"] * len(keys))
        updates = ", ".join([f"{k}=excluded.{k}" for k in keys if k != "event_id"])

        sql = f"""
        INSERT INTO current_matches ({", ".join(keys)})
        VALUES ({placeholders})
        ON CONFLICT(event_id) DO UPDATE SET
            {updates}
        """
        self.conn.execute(sql, [clean[k] for k in keys])

    def update_current(self, event_id: str, **fields) -> None:
        if not fields:
            return
        cols = self._table_columns("current_matches")
        clean = {k: v for k, v in fields.items() if k in cols}
        clean["updated_ts"] = self._now_utc()
        keys = list(clean.keys())
        sql = f"UPDATE current_matches SET {self._kv_sql(keys)} WHERE event_id=?"
        self.conn.execute(sql, [clean[k] for k in keys] + [event_id])

    def fetch_current(self, event_id: str) -> Optional[sqlite3.Row]:
        cur = self.conn.execute("SELECT * FROM current_matches WHERE event_id=?", (event_id,))
        return cur.fetchone()

    def list_current(self, where_sql: str = "", params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        sql = "SELECT * FROM current_matches"
        if where_sql:
            sql += " WHERE " + where_sql
        return list(self.conn.execute(sql, params).fetchall())

    # ---------- STREAM HISTORY ----------
    def log_stream(self, fields: Dict[str, Any]) -> None:
        """
        Insert a tick into match_stream_history. 'event_id' is required.
        'timestamp' is auto if not supplied.
        """
        if "event_id" not in fields:
            raise ValueError("log_stream requires 'event_id'")
        fields = dict(fields)
        fields.setdefault("timestamp", self._now_utc())

        cols = self._table_columns("match_stream_history")
        clean = {k: v for k, v in fields.items() if k in cols}
        keys = list(clean.keys())
        placeholders = ", ".join(["?"] * len(keys))
        sql = f"INSERT INTO match_stream_history ({', '.join(keys)}) VALUES ({placeholders})"
        self.conn.execute(sql, [clean[k] for k in keys])

    # ---------- ARCHIVING ----------
    def archive_match(self, event_id: str) -> None:
        """
        Move the row from current_matches -> archive_v3.
        Enforces that ft_score is not NULL when archiving (per your rule).
        """
        with self.tx():
            row = self.fetch_current(event_id)
            if not row:
                raise ValueError(f"event_id {event_id} not found in current_matches")

            if not row["ft_score"]:
                raise ValueError("Cannot archive without ft_score (enforced).")

            # Prepare insert into archive_v3 (only columns that exist there)
            arc_cols = self._table_columns("archive_v3")
            row_dict = dict(row)
            to_arc = {k: row_dict.get(k) for k in arc_cols if k in row_dict}

            cols = list(to_arc.keys())
            placeholders = ", ".join(["?"] * len(cols))
            updates = ", ".join([f"{c}=excluded.{c}" for c in cols if c != "event_id"])

            # UPSERT into archive_v3 (keep unique event_id guarantee)
            sql_ins = f"""
            INSERT INTO archive_v3 ({", ".join(cols)})
            VALUES ({placeholders})
            ON CONFLICT(event_id) DO UPDATE SET
                {updates}
            """
            self.conn.execute(sql_ins, [to_arc[c] for c in cols])

            # Remove from current_matches (MOVE policy)
            self.conn.execute("DELETE FROM current_matches WHERE event_id=?", (event_id,))

    def delete_from_current(self, event_id: str):
        # Remove from current_matches (DELETE policy)
        self.conn.execute("DELETE FROM current_matches WHERE event_id=?", (event_id,))

    # ---------- QUERY: ARCHIVE ----------
    def fetch_archive(self, event_id: str) -> Optional[sqlite3.Row]:
        cur = self.conn.execute("SELECT * FROM archive_v3 WHERE event_id=?", (event_id,))
        return cur.fetchone()

    def list_archive_since(self, iso_start: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM archive_v3 WHERE kickoff >= ? ORDER BY kickoff",
            (iso_start,)
        ).fetchall())

    # ---------- internals ----------
    def _table_columns(self, table: str) -> set[str]:
        cur = self.conn.execute(f"PRAGMA table_info({table})")
        return {r[1] for r in cur.fetchall()}
    
        # --- context manager support ---
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            try:
                self.conn.commit()
            except Exception:
                pass
        else:
            try:
                self.conn.rollback()
            except Exception:
                pass
        try:
            self.conn.close()
        except Exception:
            pass

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

    def get_last_stream_prices(self, event_id: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        cur = self.conn.execute(
            f"SELECT last_h_price, last_d_price, last_a_price FROM {TABLE_CURRENT} WHERE event_id = ? LIMIT 1",
            (event_id,)
        )
        row = cur.fetchone()
        return (row[0], row[1], row[2]) if row else (None, None, None)

    def update_last_prices(self, event_id: str, h: Optional[float], d: Optional[float], a: Optional[float]) -> None:
        self.conn.execute(
            f"""UPDATE {TABLE_CURRENT}
                SET last_h_price = ?, last_d_price = ?, last_a_price = ?, updated_ts = ?
                WHERE event_id = ?""",
            (h, d, a, self._now_iso(), event_id)
        )

    def insert_stream_if_changed(
        self,
        event_id: str,
        h_price: Optional[float],
        d_price: Optional[float],
        a_price: Optional[float],
        h_score: Optional[int],
        a_score: Optional[int],
        inplay_time: Optional[int],
        h_red: Optional[int],
        a_red: Optional[int],
        ts_iso: Optional[str] = None,
    ) -> bool:
        """
        Insert a new row in match_stream_history only if any price changed
        compared to current_matches.last_*_price. Also updates the last_* prices.
        Returns True if a row was inserted.
        """
        ts = ts_iso or self._now_iso()

        last_h, last_d, last_a = self.get_last_stream_prices(event_id)

        def changed(old, new):
            if old is None and new is None:
                return False
            if old is None or new is None:
                return True
            return abs(float(old) - float(new)) > PRICE_EPSILON

        if not (changed(last_h, h_price) or changed(last_d, d_price) or changed(last_a, a_price)):
            # nothing to insert
            return False

        self.conn.execute(
            f"""INSERT INTO {TABLE_STREAM}
                (event_id, h_price, d_price, a_price, h_score, a_score, inplay_time, h_red_cards, a_red_cards, timestamp)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (event_id, h_price, d_price, a_price, h_score, a_score, inplay_time, h_red, a_red, ts)
        )
        self.update_last_prices(event_id, h_price, d_price, a_price)
        return True

    def get_stream_history(self, event_id: str, since_iso: Optional[str] = None, limit: Optional[int] = None) -> List[sqlite3.Row]:
        sql = f"SELECT * FROM {TABLE_STREAM} WHERE event_id = ?"
        params: List[Any] = [event_id]
        if since_iso:
            sql += " AND timestamp >= ?"
            params.append(since_iso)
        sql += " ORDER BY timestamp ASC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        cur = self.conn.execute(sql, params)
        return cur.fetchall()

