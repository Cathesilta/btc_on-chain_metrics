"""
Create RANGE partitions for tx_inputs / tx_outputs and create required indexes on each new partition.

Partitions created:
  tx_inputs_p{lo}_{hi}   FOR VALUES FROM (lo) TO (to)
  tx_outputs_p{lo}_{hi}  FOR VALUES FROM (lo) TO (to)

Default behavior:
- start inclusive, end exclusive (END_HEIGHT not included)
- step default 20000 => names like 700000_719999, ..., 780000_799999
- last partition is allowed to be smaller if END_HEIGHT is not aligned

Index/constraints per NEW tx_inputs partition:
  {part}_pkey                                 PRIMARY KEY (prev_txid, prev_vout, spent_height)
  idx_txi_spent_height_p{lo}_{hi}             (spent_height)
  idx_txi_spent_height_prev_p{lo}_{hi}        (spent_height, prev_txid, prev_vout)

Index/constraints per NEW tx_outputs partition:
  {part}_pkey                                 PRIMARY KEY (created_height, txid, vout)
  idx_txo_created_height_p{lo}_{hi}           (created_height)
  idx_txo_p{lo}_{hi}_address                  (address)
  idx_txo_p{lo}_{hi}_created_time             (created_time)
  idx_txo_txid_vout_p{lo}_{hi}                (txid, vout)

Notes:
- Uses autocommit because you may later extend to CONCURRENTLY; also keeps behavior simple.
- PRIMARY KEY creation uses a named constraint; we check pg_constraint to avoid duplicates.
"""

import os
import sys
import time
from dataclasses import dataclass
from typing import Iterable, Tuple

import psycopg2


@dataclass(frozen=True)
class Config:
    host: str = os.getenv("PGHOST", "127.0.0.1")
    port: int = int(os.getenv("PGPORT", "5432"))
    dbname: str = os.getenv("PGDATABASE", "btc_index")
    user: str = os.getenv("PGUSER", "btcetl")
    password: str = os.getenv("PGPASSWORD", "strongpassword")
    schema: str = os.getenv("PGSCHEMA", "public")

    start_height: int = int(os.getenv("START_HEIGHT", "780000"))
    end_height: int = int(os.getenv("END_HEIGHT", "800000"))  # end-exclusive
    step: int = int(os.getenv("STEP", "20000"))

    dry_run: bool = os.getenv("DRY_RUN", "0") == "1"
    sleep_sec: float = float(os.getenv("SLEEP_SEC", "0.02"))


def qname(schema: str, name: str) -> str:
    # safe enough if you control names (they are deterministic). If you want full safety, switch to psycopg2.sql.
    return f"{schema}.{name}"


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s);", [f"{schema}.{table}"])
    return cur.fetchone()[0] is not None


def constraint_exists(cur, schema: str, table: str, constraint_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = %s AND t.relname = %s AND c.conname = %s
        LIMIT 1;
        """,
        [schema, table, constraint_name],
    )
    return cur.fetchone() is not None


def index_exists(cur, schema: str, index_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s);", [f"{schema}.{index_name}"])
    return cur.fetchone()[0] is not None


def run(cur, sql_stmt: str, dry_run: bool):
    if dry_run:
        print(sql_stmt + ";")
        return
    cur.execute(sql_stmt)


def iter_ranges(start: int, end_exclusive: int, step: int) -> Iterable[Tuple[int, int, int]]:
    """
    yields (lo, to, hi_name) where range is [lo, to), and hi_name = to-1
    """
    lo = start
    while lo < end_exclusive:
        to = min(lo + step, end_exclusive)
        hi_name = to - 1
        yield lo, to, hi_name
        lo = to


def main() -> int:
    cfg = Config()

    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    conn.autocommit = True

    created_tables = 0
    created_constraints = 0
    created_indexes = 0
    skipped_existing = 0
    errors = 0

    with conn.cursor() as cur:
        # Sanity: parent tables exist
        for parent in ("tx_inputs", "tx_outputs"):
            if not table_exists(cur, cfg.schema, parent):
                print(f"[FATAL] missing parent table: {qname(cfg.schema, parent)}", file=sys.stderr)
                return 2

        for lo, to, hi in iter_ranges(cfg.start_height, cfg.end_height, cfg.step):
            txi_part = f"tx_inputs_p{lo}_{hi}"
            txo_part = f"tx_outputs_p{lo}_{hi}"

            # ---- Create partitions (if missing) ----
            if not table_exists(cur, cfg.schema, txi_part):
                stmt = (
                    f"CREATE TABLE {qname(cfg.schema, txi_part)} "
                    f"PARTITION OF {qname(cfg.schema, 'tx_inputs')} "
                    f"FOR VALUES FROM ({lo}) TO ({to})"
                )
                try:
                    run(cur, stmt, cfg.dry_run)
                    created_tables += 1
                except Exception as e:
                    errors += 1
                    print(f"[ERROR] create {qname(cfg.schema, txi_part)}: {e.__class__.__name__}: {e}")
            else:
                skipped_existing += 1

            if not table_exists(cur, cfg.schema, txo_part):
                stmt = (
                    f"CREATE TABLE {qname(cfg.schema, txo_part)} "
                    f"PARTITION OF {qname(cfg.schema, 'tx_outputs')} "
                    f"FOR VALUES FROM ({lo}) TO ({to})"
                )
                try:
                    run(cur, stmt, cfg.dry_run)
                    created_tables += 1
                except Exception as e:
                    errors += 1
                    print(f"[ERROR] create {qname(cfg.schema, txo_part)}: {e.__class__.__name__}: {e}")
            else:
                skipped_existing += 1

            # If creation failed and table still doesn't exist, skip indexes for that table.
            txi_ok = table_exists(cur, cfg.schema, txi_part)
            txo_ok = table_exists(cur, cfg.schema, txo_part)

            # ---- tx_inputs constraints/indexes ----
            if txi_ok:
                pk_name = f"{txi_part}_pkey"
                if not constraint_exists(cur, cfg.schema, txi_part, pk_name):
                    stmt = (
                        f"ALTER TABLE {qname(cfg.schema, txi_part)} "
                        f"ADD CONSTRAINT {pk_name} PRIMARY KEY (prev_txid, prev_vout, spent_height)"
                    )
                    try:
                        run(cur, stmt, cfg.dry_run)
                        created_constraints += 1
                    except Exception as e:
                        errors += 1
                        print(f"[ERROR] PK {qname(cfg.schema, txi_part)}: {e.__class__.__name__}: {e}")

                idx1 = f"idx_txi_spent_height_p{lo}_{hi}"
                if not index_exists(cur, cfg.schema, idx1):
                    stmt = (
                        f"CREATE INDEX IF NOT EXISTS {idx1} "
                        f"ON {qname(cfg.schema, txi_part)} (spent_height)"
                    )
                    try:
                        run(cur, stmt, cfg.dry_run)
                        created_indexes += 1
                    except Exception as e:
                        errors += 1
                        print(f"[ERROR] {idx1}: {e.__class__.__name__}: {e}")

                idx2 = f"idx_txi_spent_height_prev_p{lo}_{hi}"
                if not index_exists(cur, cfg.schema, idx2):
                    stmt = (
                        f"CREATE INDEX IF NOT EXISTS {idx2} "
                        f"ON {qname(cfg.schema, txi_part)} (spent_height, prev_txid, prev_vout)"
                    )
                    try:
                        run(cur, stmt, cfg.dry_run)
                        created_indexes += 1
                    except Exception as e:
                        errors += 1
                        print(f"[ERROR] {idx2}: {e.__class__.__name__}: {e}")

            # ---- tx_outputs constraints/indexes ----
            if txo_ok:
                pk_name = f"{txo_part}_pkey"
                if not constraint_exists(cur, cfg.schema, txo_part, pk_name):
                    stmt = (
                        f"ALTER TABLE {qname(cfg.schema, txo_part)} "
                        f"ADD CONSTRAINT {pk_name} PRIMARY KEY (created_height, txid, vout)"
                    )
                    try:
                        run(cur, stmt, cfg.dry_run)
                        created_constraints += 1
                    except Exception as e:
                        errors += 1
                        print(f"[ERROR] PK {qname(cfg.schema, txo_part)}: {e.__class__.__name__}: {e}")

                idx_ch = f"idx_txo_created_height_p{lo}_{hi}"
                if not index_exists(cur, cfg.schema, idx_ch):
                    stmt = (
                        f"CREATE INDEX IF NOT EXISTS {idx_ch} "
                        f"ON {qname(cfg.schema, txo_part)} (created_height)"
                    )
                    try:
                        run(cur, stmt, cfg.dry_run)
                        created_indexes += 1
                    except Exception as e:
                        errors += 1
                        print(f"[ERROR] {idx_ch}: {e.__class__.__name__}: {e}")

                idx_addr = f"idx_txo_p{lo}_{hi}_address"
                if not index_exists(cur, cfg.schema, idx_addr):
                    stmt = (
                        f"CREATE INDEX IF NOT EXISTS {idx_addr} "
                        f"ON {qname(cfg.schema, txo_part)} (address)"
                    )
                    try:
                        run(cur, stmt, cfg.dry_run)
                        created_indexes += 1
                    except Exception as e:
                        errors += 1
                        print(f"[ERROR] {idx_addr}: {e.__class__.__name__}: {e}")

                idx_ct = f"idx_txo_p{lo}_{hi}_created_time"
                if not index_exists(cur, cfg.schema, idx_ct):
                    stmt = (
                        f"CREATE INDEX IF NOT EXISTS {idx_ct} "
                        f"ON {qname(cfg.schema, txo_part)} (created_time)"
                    )
                    try:
                        run(cur, stmt, cfg.dry_run)
                        created_indexes += 1
                    except Exception as e:
                        errors += 1
                        print(f"[ERROR] {idx_ct}: {e.__class__.__name__}: {e}")

                idx_tv = f"idx_txo_txid_vout_p{lo}_{hi}"
                if not index_exists(cur, cfg.schema, idx_tv):
                    stmt = (
                        f"CREATE INDEX IF NOT EXISTS {idx_tv} "
                        f"ON {qname(cfg.schema, txo_part)} (txid, vout)"
                    )
                    try:
                        run(cur, stmt, cfg.dry_run)
                        created_indexes += 1
                    except Exception as e:
                        errors += 1
                        print(f"[ERROR] {idx_tv}: {e.__class__.__name__}: {e}")

            print(f"[DONE] range [{lo},{to}) -> p{lo}_{hi}")
            time.sleep(cfg.sleep_sec)

    conn.close()
    print(
        f"\nSummary: tables={created_tables}, constraints={created_constraints}, "
        f"indexes={created_indexes}, skipped_existing={skipped_existing}, errors={errors}"
    )
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    sys.exit(main())