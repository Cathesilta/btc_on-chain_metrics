#!/usr/bin/env python3
"""
Count how many blocks exist per N-day window in public.block_header
for heights [H1, H2] (inclusive).

Assumes:
- block_header.time is Unix epoch seconds (UTC)
- height is unique per row

Example:
  python count_blocks_by_ndays.py --h1 920000 --h2 933106 --ndays 7
"""



# =========================
# IMPORTS
# =========================

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Tuple
import os
import psycopg2

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

print(str(Path(__file__).resolve().parents[2]))
from indexing.conf import settings

H1 = 0          # start height (inclusive)
H2 = 400000          # end height (inclusive)
N_DAYS = 30           # bucket size (N days)

ANCHOR = "first_day" # "first_day" | "utc_epoch"



# =========================
# DB CONFIG
# =========================

@dataclass
class DbConf:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def load_db_conf() -> DbConf:
    return DbConf(
        host=settings.PGHOST,
        port=settings.PGPORT,
        dbname=settings.PGDATABASE,
        user=settings.PGUSER,
        password=settings.PGPASSWORD,
    )

# =========================
# DATA ACCESS
# =========================

def fetch_daily_counts(
    conn,
    h1: int,
    h2: int,
) -> List[Tuple[datetime, int]]:
    """
    Return [(day_utc_midnight, block_count)] sorted by day.
    """
    sql = """
        SELECT
            date_trunc('day', to_timestamp(time) AT TIME ZONE 'UTC') AS day_utc,
            COUNT(*)::bigint AS blocks
        FROM public.block_header
        WHERE height BETWEEN %s AND %s
        GROUP BY 1
        ORDER BY 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (h1, h2))
        rows = cur.fetchall()

    out = []
    for day_utc, cnt in rows:
        if day_utc.tzinfo is None:
            day_utc = day_utc.replace(tzinfo=timezone.utc)
        out.append((day_utc, int(cnt)))
    return out

# =========================
# AGGREGATION
# =========================

def bucket_by_ndays(
    daily: List[Tuple[datetime, int]],
    ndays: int,
    anchor: str,
) -> List[Tuple[datetime, datetime, int]]:
    """
    Returns:
      [(window_start_utc, window_end_utc_exclusive, block_count)]
    """
    if not daily:
        return []

    if ndays <= 0:
        raise ValueError("N_DAYS must be >= 1")

    window = timedelta(days=ndays)

    if anchor == "first_day":
        anchor_day = daily[0][0]
    elif anchor == "utc_epoch":
        anchor_day = datetime(1970, 1, 1, tzinfo=timezone.utc)
    else:
        raise ValueError("ANCHOR must be 'first_day' or 'utc_epoch'")

    buckets = {}
    for day, cnt in daily:
        idx = int((day - anchor_day) // window)
        buckets[idx] = buckets.get(idx, 0) + cnt

    result = []
    for idx in sorted(buckets):
        start = anchor_day + idx * window
        end = start + window
        result.append((start, end, buckets[idx]))

    return result

# =========================
# MAIN
# =========================

def main() -> None:
    h1, h2 = (H1, H2) if H1 <= H2 else (H2, H1)

    conf = load_db_conf()
    conn = psycopg2.connect(
        host=conf.host,
        port=conf.port,
        dbname=conf.dbname,
        user=conf.user,
        password=conf.password,
    )

    try:
        daily = fetch_daily_counts(conn, h1, h2)
        buckets = bucket_by_ndays(daily, N_DAYS, ANCHOR)

        print("window_start_utc,window_end_utc_exclusive,blocks")
        for start, end, cnt in buckets:
            print(f"{start.isoformat()},{end.isoformat()},{cnt}")

        if daily:
            total_blocks = sum(cnt for _, cnt in daily)
            print()
            print(
                f"# heights=[{h1},{h2}] "
                f"days=[{daily[0][0].date()},{daily[-1][0].date()}] "
                f"total_blocks={total_blocks}"
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()





