# migrate_league_to_comp.py
from __future__ import annotations

import sqlite3
from typing import List, Tuple

from core.settings import DB_PATH


def sqlite_version_tuple(conn: sqlite3.Connection) -> Tuple[int, int, int]:
    v = conn.execute("select sqlite_version()").fetchone()[0]
    parts = v.split(".")
    return (int(parts[0]), int(parts[1]), int(parts[2]))


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]  # name


def has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    return col in table_columns(conn, table)


def cleanup_comp_values(conn: sqlite3.Connection, table: str) -> None:
    # Remove commas anywhere, then trim extra spaces
    conn.execute(
        f"""
        UPDATE {table}
        SET comp = TRIM(REPLACE(comp, ',', ''))
        WHERE comp IS NOT NULL AND INSTR(comp, ',') > 0
        """
    )


def rename_league_to_comp_modern(conn: sqlite3.Connection, table: str) -> None:
    # Rename column and clean values
    conn.execute(f"ALTER TABLE {table} RENAME COLUMN league TO comp")
    cleanup_comp_values(conn, table)


def rebuild_table_rename(conn: sqlite3.Connection, table: str) -> None:
    """
    Fallback for older SQLite without RENAME COLUMN.
    Creates a new table with same schema except league->comp,
    copies data, drops old table, renames new table.
    """
    # Get current DDL
    ddl_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not ddl_row or not ddl_row[0]:
        raise RuntimeError(f"Could not read schema for table {table}")

    ddl = ddl_row[0]

    # Create new DDL by replacing only the column name token "league" -> "comp"
    # This is simplistic but OK given your schema (single column named league).
    new_table = f"{table}__new"
    new_ddl = ddl.replace(f"CREATE TABLE {table}", f"CREATE TABLE {new_table}")
    new_ddl = new_ddl.replace("league        ", "comp          ")
    new_ddl = new_ddl.replace("league ", "comp ")

    conn.execute(new_ddl)

    cols = table_columns(conn, table)
    # Build insert mapping: league -> comp in new table
    new_cols = ["comp" if c == "league" else c for c in cols]

    col_list_old = ", ".join(cols)
    col_list_new = ", ".join(new_cols)

    conn.execute(
        f"INSERT INTO {new_table} ({col_list_new}) SELECT {col_list_old} FROM {table}"
    )

    conn.execute(f"DROP TABLE {table}")
    conn.execute(f"ALTER TABLE {new_table} RENAME TO {table}")

    cleanup_comp_values(conn, table)


def fix_indexes(conn: sqlite3.Connection) -> None:
    # Drop old index if it exists
    conn.execute("DROP INDEX IF EXISTS idx_current_league;")
    # Create new index
    conn.execute("CREATE INDEX IF NOT EXISTS idx_current_comp ON current_matches(comp);")


def migrate(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        ver = sqlite_version_tuple(conn)

        # current_matches
        if has_column(conn, "current_matches", "league") and not has_column(conn, "current_matches", "comp"):
            if ver >= (3, 25, 0):
                rename_league_to_comp_modern(conn, "current_matches")
            else:
                rebuild_table_rename(conn, "current_matches")

        # archive_v3
        if has_column(conn, "archive_v3", "league") and not has_column(conn, "archive_v3", "comp"):
            if ver >= (3, 25, 0):
                rename_league_to_comp_modern(conn, "archive_v3")
            else:
                rebuild_table_rename(conn, "archive_v3")

        fix_indexes(conn)

        conn.commit()
        print("Migration complete: league -> comp, commas removed, indexes updated.")

    finally:
        conn.close()


if __name__ == "__main__":
    migrate(DB_PATH)
