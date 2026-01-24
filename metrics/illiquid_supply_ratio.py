
"""
Illiquidity Supply Ratio (address-level approximation)
- Tracks address inflow/outflow (received/spent) and balance (UTXO-style) from START_HEIGHT to latest.
- Classifies an address as "illiquid" if (spent / received) < ILLIQUID_SPEND_RATIO_THRESHOLD
- Illiquidity Supply Ratio = sum(balance of illiquid addresses) / sum(balance of all tracked addresses)

Notes / assumptions:
- Uses address from tx_outputs. Skips NULL/empty address.
- Spent value is derived by joining tx_inputs.prev_txid/prev_vout to tx_outputs.txid/vout.
- Only measures behavior since START_HEIGHT (920000). Older history is ignored.
"""

import os
import json
import time
from dataclasses import dataclass
from typing import Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

from tqdm import tqdm
from contextlib import contextmanager

import inspect

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from indexing.conf import settings

# =========================
# Global config
# =========================

DB_HOST = settings.PGHOST
DB_PORT = settings.PGPORT
DB_NAME = settings.PGDATABASE
DB_USER = settings.PGUSER
DB_PASSWORD = settings.PGPASSWORD

START_HEIGHT = 400000
BATCH_BLOCKS = 5000  # tune for performance
UPSERT_PAGE_SIZE = 50000     # execute_values page size
COMMIT_EVERY_BATCHES = 20     # commit less often (but risk bigger rollback on crash)
SNAPSHOT_EVERY_N_BLOCKS = 5000  # store one snapshot every N blocks (and always at latest)

# limit to first N blocks only (for experiment)
DRY_BLOCK_WINDOW = 100000   # e.g. only process 2000 blocks then exit

# "Illiquid" if spent/received < threshold.
# Commonly used heuristics are in the ~0.1-0.3 range; adjust for your needs.
ILLIQUID_SPEND_RATIO_THRESHOLD = 0.25

# If True, recreates working tables (drops prior run state)
RESET_WORK_TABLES = False

# Working schema/table names
WORK_SCHEMA = settings.PGSCHEMA
ADDR_STATE_TABLE = "addr_state_illiq"
RESULT_TABLE = "illiquid_supply_ratio"


# Optional: use UNLOGGED state table for speed (data is lost after crash/restart of Postgres)
#####******************################   Danger  Zone  ##############******************##########
USE_UNLOGGED_STATE_TABLE = True



# =========================
# SQL
# =========================

SQL_GET_LATEST_HEIGHT = "SELECT MAX(height) FROM public.block_header;"
SQL_GET_BLOCK_TIME = "SELECT time FROM public.block_header WHERE height = %s;"

SQL_DROP_STATE = f"DROP TABLE IF EXISTS {WORK_SCHEMA}.{ADDR_STATE_TABLE};"
SQL_DROP_RESULT = f"DROP TABLE IF EXISTS {WORK_SCHEMA}.{RESULT_TABLE};"

def sql_create_state(unlogged: bool) -> str:
    tbl_kind = "UNLOGGED TABLE" if unlogged else "TABLE"
    return f"""
    CREATE {tbl_kind} IF NOT EXISTS {WORK_SCHEMA}.{ADDR_STATE_TABLE} (
      address       text PRIMARY KEY,
      received_sats bigint NOT NULL DEFAULT 0,
      spent_sats    bigint NOT NULL DEFAULT 0,
      balance_sats  bigint NOT NULL DEFAULT 0
    );
    """

SQL_CREATE_RESULT = f"""
CREATE TABLE IF NOT EXISTS {WORK_SCHEMA}.{RESULT_TABLE} (
  height               integer PRIMARY KEY,
  time                 bigint NOT NULL,
  total_supply_sats    bigint NOT NULL,
  illiquid_supply_sats bigint NOT NULL,
  ratio                double precision NOT NULL,
  params               jsonb NOT NULL
);
"""

# resume from last snapshot
SQL_GET_LAST_DONE = f"SELECT COALESCE(MAX(height), {START_HEIGHT}-1) FROM {WORK_SCHEMA}.{RESULT_TABLE};"

# batch aggregates
SQL_BATCH_RECEIVED = """
SELECT address, SUM(value_sats)::bigint AS recv
FROM public.tx_outputs
WHERE created_height BETWEEN %s AND %s
  AND address IS NOT NULL AND address <> ''
GROUP BY address;
"""

SQL_BATCH_SPENT = """
SELECT o.address, SUM(o.value_sats)::bigint AS sp
FROM public.tx_inputs i
JOIN public.tx_outputs o
  ON o.txid = i.prev_txid AND o.vout = i.prev_vout
WHERE i.spent_height BETWEEN %s AND %s
  AND o.address IS NOT NULL AND o.address <> ''
GROUP BY o.address;
"""
# ######## Added to optimize SQL_BATCH_SPENT
# session-local tuning (applies only inside current transaction)
SQL_SET_LOCAL_TUNING = """
SET LOCAL work_mem = '512MB';
"""
# If your Postgres supports it (PG 13+ typically has hash_mem_multiplier),
# you can add it; if it errors, remove this line.
SQL_SET_LOCAL_HASH_MEM = "SET LOCAL hash_mem_multiplier = 2;"
# ######### Added to optimize SQL_BATCH_SPENT

SQL_UPSERT_RECEIVED = f"""
INSERT INTO {WORK_SCHEMA}.{ADDR_STATE_TABLE} (address, received_sats, balance_sats)
VALUES %s
ON CONFLICT (address) DO UPDATE
SET received_sats = {WORK_SCHEMA}.{ADDR_STATE_TABLE}.received_sats + EXCLUDED.received_sats,
    balance_sats  = {WORK_SCHEMA}.{ADDR_STATE_TABLE}.balance_sats  + EXCLUDED.received_sats;
"""

SQL_UPSERT_SPENT = f"""
INSERT INTO {WORK_SCHEMA}.{ADDR_STATE_TABLE} (address, spent_sats, balance_sats)
VALUES %s
ON CONFLICT (address) DO UPDATE
SET spent_sats   = {WORK_SCHEMA}.{ADDR_STATE_TABLE}.spent_sats   + EXCLUDED.spent_sats,
    balance_sats = {WORK_SCHEMA}.{ADDR_STATE_TABLE}.balance_sats - EXCLUDED.spent_sats;
"""


# NULLIF(received_sats, 0) means return received_sats or 0, it's a redundancy because "WHEN received_sats > 0", ignore this.
SQL_COMPUTE_SNAPSHOT = f"""
WITH s AS (
  SELECT
    SUM(balance_sats)::bigint AS total_supply_sats,
    SUM(
      CASE
        WHEN received_sats > 0
         AND (spent_sats::numeric / NULLIF(received_sats, 0)) < %s
        THEN balance_sats
        ELSE 0
      END
    )::bigint AS illiquid_supply_sats
  FROM {WORK_SCHEMA}.{ADDR_STATE_TABLE}
  WHERE balance_sats > 0
)
SELECT
  COALESCE(total_supply_sats, 0)::bigint,
  COALESCE(illiquid_supply_sats, 0)::bigint,
  CASE WHEN COALESCE(total_supply_sats, 0) > 0
       THEN (illiquid_supply_sats::double precision / total_supply_sats::double precision)
       ELSE 0.0
  END AS ratio
FROM s;
"""

SQL_UPSERT_RESULT = f"""
INSERT INTO {WORK_SCHEMA}.{RESULT_TABLE}
  (height, time, total_supply_sats, illiquid_supply_sats, ratio, params)
VALUES (%s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (height) DO UPDATE
SET time                 = EXCLUDED.time,
    total_supply_sats    = EXCLUDED.total_supply_sats,
    illiquid_supply_sats = EXCLUDED.illiquid_supply_sats,
    ratio                = EXCLUDED.ratio,
    params               = EXCLUDED.params;
"""


# =========================
# DB helpers
# =========================

def connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

def fetch_one_int(cur, sql: str, args=()) -> int:
    cur.execute(sql, args)
    v = cur.fetchone()[0]
    if v is None:
        raise RuntimeError("Query returned NULL unexpectedly.")
    return int(v)

def fetch_block_time(cur, height: int) -> int:
    cur.execute(SQL_GET_BLOCK_TIME, (height,))
    r = cur.fetchone()
    if not r:
        raise RuntimeError(f"Missing block time for height={height}")
    # ?Is the time really r[0]?
    return int(r[0])

def ensure_tables(cur):
    if RESET_WORK_TABLES:
        cur.execute(SQL_DROP_RESULT)
        cur.execute(SQL_DROP_STATE)

    # Create state table (optionally UNLOGGED)
    cur.execute(sql_create_state(USE_UNLOGGED_STATE_TABLE))
    cur.execute(SQL_CREATE_RESULT)

def upsert_addr_delta(cur, upsert_sql: str, rows: list[tuple]):
    """
    rows: [(address, delta_sats), ...]
    """
    if not rows:
        return
    # for received: (address, received_sats, balance_sats)
    # for spent:    (address, spent_sats, balance_sats_delta)  (same value used, SQL subtracts from balance)
    payload = [(addr, int(v), int(v)) for addr, v in rows]
    execute_values(cur, upsert_sql, payload, page_size=UPSERT_PAGE_SIZE)

# def upsert_addr_delta(cur, upsert_sql: str, rows: list[tuple], desc: str = "upsert"):
#     """
#     rows: [(address, delta_sats), ...]
#     """
#     n = len(rows)
#     if n == 0:
#         return

#     # build payload once (this can also be non-trivial time)
#     with timed(f"{desc}: build payload ({n})"):
#         payload = [(addr, int(v), int(v)) for addr, v in rows]

#     page = UPSERT_PAGE_SIZE
#     total_pages = (n + page - 1) // page

#     with tqdm(total=total_pages, desc=desc, unit="page", leave=False) as pbar:
#         # send in chunks so tqdm can update
#         for i in range(0, n, page):
#             chunk = payload[i:i + page]
#             with timed(f"{desc}: execute_values chunk {i//page + 1}/{total_pages} (rows={len(chunk)})"):
#                 execute_values(cur, upsert_sql, chunk, page_size=len(chunk))
#             pbar.update(1)

def compute_snapshot(cur) -> Tuple[int, int, float]:
    cur.execute(SQL_COMPUTE_SNAPSHOT, (ILLIQUID_SPEND_RATIO_THRESHOLD,))
    total, illiq, ratio = cur.fetchone()
    return int(total), int(illiq), float(ratio)

def should_snapshot(height: int, latest: int) -> bool:
    print("checking should_snapshot")
    if height >= latest:
        return True
    print(f"height {height} % SNAPSHOT_EVERY_N_BLOCKS {SNAPSHOT_EVERY_N_BLOCKS} == {height % SNAPSHOT_EVERY_N_BLOCKS}")
    
    # snapshot every N blocks by height boundary
    return (height % SNAPSHOT_EVERY_N_BLOCKS) == 0


# =========================
# Monitor helpers
# =========================

@contextmanager
def timed(step: str):
    t0 = time.time()
    yield
    dt = time.time() - t0
    tqdm.write(f"[{step}] {dt:.2f}s")


# =========================
# Main
# =========================

def main():
    t0 = time.time()

    with connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            ensure_tables(cur)
            cur.execute(SQL_SET_LOCAL_TUNING)
            cur.execute(SQL_SET_LOCAL_HASH_MEM)
            print(f"<{inspect.currentframe().f_code.co_name}> settings:")
            print(f"<{inspect.currentframe().f_code.co_name}> BATCH_BLOCKS to {BATCH_BLOCKS}")
            # Get the lastest height from block_head
            latest = fetch_one_int(cur, SQL_GET_LATEST_HEIGHT)
            if DRY_BLOCK_WINDOW is not None:
                latest = min(latest, START_HEIGHT + DRY_BLOCK_WINDOW - 1)
                print(f"<{inspect.currentframe().f_code.co_name}> you set DRY_BLOCK_WINDOW to {DRY_BLOCK_WINDOW}")

            if latest < START_HEIGHT:
                raise RuntimeError(f"latest={latest} < START_HEIGHT={START_HEIGHT}")
            
            print(f"<{inspect.currentframe().f_code.co_name}> latest is {latest}")

            # Resume from last snapshot, unless reset
            if RESET_WORK_TABLES:
                h = START_HEIGHT
                print(f"<{inspect.currentframe().f_code.co_name}> RESET_WORK_TABLES is {RESET_WORK_TABLES} .\
                      Will start at START_HEIGHT {START_HEIGHT}")
            else:
                last_done = fetch_one_int(cur, SQL_GET_LAST_DONE)
                h = max(START_HEIGHT, last_done + 1)
                
                print(f"<{inspect.currentframe().f_code.co_name}> even if you set START_HEIGHT to {START_HEIGHT},\
                      RESET_WORK_TABLES is {RESET_WORK_TABLES}. Will continue at {h}")
                
            print(f"<{inspect.currentframe().f_code.co_name}> \
                  range: {h}..{latest} (START_HEIGHT={START_HEIGHT}, BATCH_BLOCKS={BATCH_BLOCKS})")

            batches_since_commit = 0
            SQL_BATCH_SPENT_time_spent = 0

            total_blocks = latest - h + 1
            pbar = tqdm(total=total_blocks, desc="Processing blocks", unit="block")
            while h <= latest:
                h2 = min(h + BATCH_BLOCKS - 1, latest)
                batch_n = h2 - h + 1

                # print(f"<{inspect.currentframe().f_code.co_name}> processing from height {h} to height {h2}")
                # 1) received deltas in [h, h2]
                cur.execute(SQL_BATCH_RECEIVED, (h, h2))
                recv_rows = cur.fetchall()  # [(address, recv), ...]
                upsert_addr_delta(cur, SQL_UPSERT_RECEIVED, recv_rows)

                # 2) spent deltas in [h, h2]
                start_time = time.time()
                with timed(f"{h}-{h2} SQL_BATCH_SPENT execute"):
                    cur.execute(SQL_BATCH_SPENT, (h, h2))
                SQL_BATCH_SPENT_time_spent += time.time() - start_time
                spent_rows = cur.fetchall()
                upsert_addr_delta(cur, SQL_UPSERT_SPENT, spent_rows)

                batches_since_commit += 1

                # snapshot on boundary (or latest)
                if should_snapshot(h2+1, latest):
                    blk_time = fetch_block_time(cur, h2)
                    with timed(f"{h}-{h2} compute_snapshot"):
                        total, illiq, ratio = compute_snapshot(cur)
                    params = {
                        "start_height": START_HEIGHT,
                        "batch_blocks": BATCH_BLOCKS,
                        "snapshot_every_n_blocks": SNAPSHOT_EVERY_N_BLOCKS,
                        "illiquid_spend_ratio_threshold": ILLIQUID_SPEND_RATIO_THRESHOLD,
                        "definition": "address-level approx: illiquid if spent/received < threshold; supply=positive balances since start_height",
                    }
                    print(f"<{inspect.currentframe().f_code.co_name}> snapshot compute finished")

                    cur.execute(
                        SQL_UPSERT_RESULT,
                        (h2, blk_time, total, illiq, ratio, json.dumps(params)),
                    )
                    
                    print(
                        f"snapshot height={h2} ratio={ratio:.6f} total={total} illiq={illiq} "
                        f"(recv_addrs={len(recv_rows)} spent_addrs={len(spent_rows)})"
                    )

                # commit policy
                if batches_since_commit >= COMMIT_EVERY_BATCHES:
                    conn.commit()
                    batches_since_commit = 0

                h = h2 + 1
                pbar.update(batch_n)

            pbar.close()

            # final commit
            conn.commit()

            print(f"With BATCH_BLOCKS == {BATCH_BLOCKS} SQL_BATCH_SPENT_time_spent spent {SQL_BATCH_SPENT_time_spent}s")

    print(f"done in {time.time() - t0:.1f}s; results: {WORK_SCHEMA}.{RESULT_TABLE}")


if __name__ == "__main__":
    main()