"""
Create per-partition indexes for Bitcoin tx_inputs/tx_outputs partitions.

- Iterates partitions in [START_HEIGHT, END_HEIGHT) with STEP (default 20000)
- For each partition, creates (CONCURRENTLY IF NOT EXISTS):
  1) tx_outputs_pXXXXXX_YYYYYY: (txid, vout)
  2) tx_outputs_pXXXXXX_YYYYYY: (created_height)
  3) tx_inputs_pXXXXXX_YYYYYY : (spent_height)
  4) tx_inputs_pXXXXXX_YYYYYY : (spent_height, prev_txid, prev_vout)

IMPORTANT:
- CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
  So we force autocommit=True and execute one statement at a time.
"""

import os
import sys
import time
from dataclasses import dataclass

import psycopg2
from psycopg2 import sql
from psycopg2.errors import DuplicateObject, UndefinedTable

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))
from indexing.conf import settings


@dataclass(frozen=True)
class Config:

    schema: str = settings.PGSCHEMA

    start_height: int = int(os.getenv("START_HEIGHT", "880000"))
    end_height: int = int(os.getenv("END_HEIGHT", "920000"))  # end-exclusive
    step: int = settings.TABLE_PARTITION_STEP

    dry_run: bool = os.getenv("DRY_RUN", "0") == "1"
    sleep_sec: float = float(os.getenv("SLEEP_SEC", "0.05"))

    # Optional safety: fail fast if locks conflict too long (seconds)
    lock_timeout_ms: int = int(os.getenv("LOCK_TIMEOUT_MS", "0"))  # 0 = no change
    statement_timeout_ms: int = int(os.getenv("STATEMENT_TIMEOUT_MS", "0"))  # 0 = no change


def partition_name(prefix: str, lo: int, hi: int) -> str:
    # Your naming is p{lo}_{hi} where hi = lo+step-1 in the name, but the range is [lo, lo+step).
    return f"{prefix}_p{lo}_{hi}"


def expected_partitions(cfg: Config):
    lo = cfg.start_height
    while lo < cfg.end_height:
        hi_name = lo + cfg.step - 1
        yield lo, hi_name
        lo += cfg.step


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s);", [f"{schema}.{table}"])
    return cur.fetchone()[0] is not None


def run_one(cur, stmt: str, dry_run: bool):
    if dry_run:
        print(stmt + ";")
        return
    cur.execute(stmt)


def maybe_set_timeouts(cur, cfg: Config):
    # These apply per-session; harmless if 0.
    if cfg.lock_timeout_ms > 0:
        cur.execute("SET lock_timeout = %s;", [f"{cfg.lock_timeout_ms}ms"])
    if cfg.statement_timeout_ms > 0:
        cur.execute("SET statement_timeout = %s;", [f"{cfg.statement_timeout_ms}ms"])


def main():
    cfg = Config()

    conn = psycopg2.connect(
        host=settings.PGHOST,
        port=settings.PGPORT,
        dbname=settings.PGDATABASE,
        user=settings.PGUSER,
        password=settings.PGPASSWORD,
    )
    # Required for CREATE INDEX CONCURRENTLY
    conn.autocommit = True

    created = 0
    skipped_missing_tables = 0
    errors = 0

    with conn.cursor() as cur:
        maybe_set_timeouts(cur, cfg)

        for lo, hi_name in expected_partitions(cfg):
            txo = partition_name("tx_outputs", lo, hi_name)
            txi = partition_name("tx_inputs", lo, hi_name)

            txo_exists = table_exists(cur, cfg.schema, txo)
            txi_exists = table_exists(cur, cfg.schema, txi)



            if not txo_exists and not txi_exists:
                skipped_missing_tables += 2
                print(f"[SKIP] missing: {cfg.schema}.{txo} and {cfg.schema}.{txi}")
                continue

            if txo_exists:
                stmts = [
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txo_txid_vout_p{lo}_{hi_name} "
                    f"ON {cfg.schema}.{txo} (txid, vout)",
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txo_created_height_p{lo}_{hi_name} "
                    f"ON {cfg.schema}.{txo} (created_height)",
                ]
                for s in stmts:
                    try:
                        run_one(cur, s, cfg.dry_run)
                        created += 1
                    except DuplicateObject:
                        # Shouldn’t happen with IF NOT EXISTS, but safe.
                        print(f"[EXISTS] {s.split('IF NOT EXISTS',1)[1].strip()}")
                    except Exception as e:
                        errors += 1
                        print(f"[ERROR] {cfg.schema}.{txo}: {e.__class__.__name__}: {e}")
                    time.sleep(cfg.sleep_sec)

            if txi_exists:
                stmts = [
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txi_spent_height_p{lo}_{hi_name} "
                    f"ON {cfg.schema}.{txi} (spent_height)",
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txi_spent_height_prev_p{lo}_{hi_name} "
                    f"ON {cfg.schema}.{txi} (spent_height, prev_txid, prev_vout)",
                ]
                for s in stmts:
                    try:
                        run_one(cur, s, cfg.dry_run)
                        created += 1
                    except DuplicateObject:
                        print(f"[EXISTS] {s.split('IF NOT EXISTS',1)[1].strip()}")
                    except Exception as e:
                        errors += 1
                        print(f"[ERROR] {cfg.schema}.{txi}: {e.__class__.__name__}: {e}")
                    time.sleep(cfg.sleep_sec)

            print(f"[DONE] p{lo}_{hi_name}")

    conn.close()
    print(
        f"\nSummary: statements={'printed' if cfg.dry_run else 'executed'}={created}, "
        f"skipped_missing_tables={skipped_missing_tables}, errors={errors}"
    )
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    sys.exit(main())