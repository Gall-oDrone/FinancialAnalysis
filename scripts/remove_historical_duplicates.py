#!/usr/bin/env python3
"""
Remove duplicate rows from the PostgreSQL table `historical` (stocks).
Keeps one row per (book, date); optionally ensures UNIQUE(book, date) constraint exists.
Uses env vars: PGDBNAME, PGDBUSER, PGDBPASS, PGDBHOST, PGDBPORT (defaults: cryptostocks, postgres, localhost, 5432).
"""
import os
import sys

# Allow importing Storage from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2 import sql

TABLE = "historical"
DBNAME = os.getenv("PGDBNAME", "cryptostocks")
USER = os.getenv("PGDBUSER", "postgres")
PASSWORD = os.getenv("PGDBPASS", "")
HOST = os.getenv("PGDBHOST", "localhost")
PORT = os.getenv("PGDBPORT", "5432")


def get_conn():
    return psycopg2.connect(
        dbname=DBNAME,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
    )


def count_duplicates(conn):
    """Return number of duplicate (book, date) pairs and total duplicate rows."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS dup_pairs,
                   SUM(cnt - 1) AS extra_rows
            FROM (
                SELECT book, date, COUNT(*) AS cnt
                FROM historical
                GROUP BY book, date
                HAVING COUNT(*) > 1
            ) t
        """)
        row = cur.fetchone()
    return (row[0] or 0, row[1] or 0)


def count_total(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM historical")
        return cur.fetchone()[0]


def remove_duplicates(conn):
    """Keep one row per (book, date), delete the rest (keeps row with smallest ctid)."""
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM historical a
            USING historical b
            WHERE a.book = b.book AND a.date = b.date AND a.ctid < b.ctid
        """)
        deleted = cur.rowcount
    conn.commit()
    return deleted


def has_unique_book_date(conn):
    """Check if table has a unique constraint on (book, date)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE t.relname = %s
              AND c.contype IN ('u', 'p')
              AND pg_get_constraintdef(c.oid) LIKE '%%book%%' AND pg_get_constraintdef(c.oid) LIKE '%%date%%'
            LIMIT 1
        """, (TABLE,))
        return cur.fetchone() is not None


def add_unique_book_date_if_missing(conn):
    """Add UNIQUE(book, date) if not already present."""
    if has_unique_book_date(conn):
        return False
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE historical
            ADD CONSTRAINT historical_book_date_key UNIQUE (book, date)
        """)
    conn.commit()
    return True


def main():
    print(f"Connecting to DB: {DBNAME} @ {HOST}:{PORT} (user={USER})")
    try:
        conn = get_conn()
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    try:
        total_before = count_total(conn)
        dup_pairs, extra_rows = count_duplicates(conn)

        if extra_rows == 0 and dup_pairs == 0:
            print("No duplicates found in 'historical'.")
        else:
            print(f"Found {dup_pairs} (book, date) pairs with duplicates ({extra_rows} extra rows). Removing...")
            deleted = remove_duplicates(conn)
            print(f"Deleted {deleted} duplicate row(s).")

        total_after = count_total(conn)
        print(f"Total rows: before={total_before}, after={total_after}")

        if add_unique_book_date_if_missing(conn):
            print("Added UNIQUE(book, date) constraint to prevent future duplicates.")
        else:
            print("Table already has a unique constraint on (book, date).")

    finally:
        conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
