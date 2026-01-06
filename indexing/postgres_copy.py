# Second Version
# This time, we write into stg_tx_outputs_*, making sure to write the whole rows in a csv file, and load it into the final tx_outputs_* table

import os
import sys
import subprocess
from pathlib import Path

from tqdm import tqdm

PGHOST = "127.0.0.1"
PGPORT = "5432"
PGDATABASE = "btc_index"
PGUSER = "btcetl"
PGPASSWORD = "strongpassword"

BASE_FOLDER = "/data/index/btc/csv/"
CHOSEN_FOLDER = "From_800000/from_840000"
OPERATION_FOLDER = os.path.join(BASE_FOLDER,CHOSEN_FOLDER)

# Partition size (you said 20,000 blocks per partition)
PART_STEP = 20000

# Tables
BLOCK_TABLE = "public.block_header"


# If you re-run and want a clean reload: Set this parameter to True

RELOAD_PARTITION_BEFORE_LOAD = True
# =========================


# =========================
# INTERNAL HELPERS
# =========================
def run_psql(sql: str) -> None:
    cmd = [
        "psql",
        "-h", PGHOST,
        "-p", PGPORT,
        "-U", PGUSER,
        "-d", PGDATABASE,
        "-v", "ON_ERROR_STOP=1",
        "-q",
        "-c", sql,
    ]
    r = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        env={"PGPASSWORD": PGPASSWORD},
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or "psql failed")


def psql_copy(table: str, csv_path: Path) -> None:
    # CSV must be readable by your Linux user (because \copy reads on client side).
    sql = f"\\copy {table} FROM '{csv_path}' WITH (FORMAT csv, HEADER true)"
    run_psql(sql)


def parse_range_dirname(name: str) -> tuple[int, int]:
    # "900000-900999" -> (900000, 900999)
    a, b = name.split("-", 1)
    return int(a), int(b)


def partition_table_for_height(h: int) -> str:
    p_start = (h // PART_STEP) * PART_STEP
    # p_start = h
    p_end = p_start + PART_STEP - 1
    return f"public.tx_outputs_p{p_start}_{p_end}", p_start, p_end


# =========================
# MAIN LOGIC
# =========================

def main():
    root = Path(OPERATION_FOLDER)
    if not root.exists() or not root.is_dir():
        print(f"MAIN_FOLDER not found or not a directory: {root}", file=sys.stderr)
        sys.exit(1)

    subdirs = sorted([p for p in root.iterdir() if p.is_dir()])

    # only keep dirs that match "N-N"
    jobs = []
    for d in subdirs:
        try:
            start_h, end_h = parse_range_dirname(d.name)
        except Exception:
            continue
        bh_csv = d / "block_header.pg.csv"
        txo_csv = d / "tx_outputs.pg.csv"
        if bh_csv.exists() and txo_csv.exists():
            jobs.append((d, start_h, end_h, bh_csv, txo_csv))

    if not jobs:
        print(f"No valid data folders under: {root}", file=sys.stderr)
        sys.exit(1)

    truncated_parts: set[str] = set()
    ok = 0
    failed = 0

    pbar = tqdm(jobs, unit="folder", desc="Importing")
    for d, start_h, end_h, bh_csv, txo_csv in pbar:
        part_table, part_start, part_end = partition_table_for_height(start_h)

        # update tqdm line
        pbar.set_postfix_str(f"{d.name} -> {part_table}")

        try:
            # This move is to delete all rows and write in the sub table again,
            # if you want to update the sub table.
            # If you don't want to do so, set RELOAD_PARTITION_BEFORE_LOAD to False.
            if RELOAD_PARTITION_BEFORE_LOAD and part_table not in truncated_parts:
                run_psql(f"TRUNCATE TABLE {part_table};")
                run_psql(
                    f"DELETE FROM {BLOCK_TABLE} "
                    f"WHERE height >= {part_start} AND height <= {part_end};"
                )
                truncated_parts.add(part_table)

            psql_copy(BLOCK_TABLE, bh_csv)
            psql_copy(part_table, txo_csv)

            ok += 1
        except Exception as e:
            failed += 1
            tqdm.write(f"FAILED {d.name}: {e}")
            # continue

        pbar.set_postfix({"ok": ok, "failed": failed})

    print(f"\nDONE. ok={ok}, failed={failed}")
    if failed > 0:
        print(
            "If failures are due to duplicate keys, set RELOAD_PARTITION_BEFORE_LOAD=True\n"
            "or TRUNCATE the specific partition and rerun.",
            file=sys.stderr,
        )



if __name__ == "__main__":
    main()
