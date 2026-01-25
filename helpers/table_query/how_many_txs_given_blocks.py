from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Tuple
import psycopg2

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from indexing.conf import settings


# =========================
# USER CONFIG
# =========================

H1 = 500000          # start height (inclusive)
H2 = 505000          # end height (inclusive)


# =========================
# SQL
# =========================

SQL_SUM_TX = """
SELECT
  COUNT(*) AS blocks_count,
  COALESCE(SUM(tx_count), 0)::bigint AS total_txs
FROM public.block_header
WHERE height BETWEEN %s AND %s;
"""


# =========================
# MAIN
# =========================

def main():
    if H1 > H2:
        raise ValueError("H1 must be <= H2")

    conn = psycopg2.connect(
        host=settings.PGHOST,
        port=settings.PGPORT,
        dbname=settings.PGDATABASE,
        user=settings.PGUSER,
        password=settings.PGPASSWORD,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SUM_TX, (H1, H2))
            blocks_count, total_txs = cur.fetchone()

        print("=================================")
        print(f"Height Range: [{H1}, {H2}]")
        print(f"Blocks Found: {blocks_count}")
        print(f"Total Transactions (SUM tx_count): {total_txs}")
        print("=================================")

        expected_blocks = H2 - H1 + 1
        if blocks_count != expected_blocks:
            print(f"WARNING: expected {expected_blocks} blocks, got {blocks_count}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()