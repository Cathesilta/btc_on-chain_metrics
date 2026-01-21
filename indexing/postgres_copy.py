# Second Version
# This time, we write into stg_tx_outputs_*, making sure to write the whole rows in a csv file, and load it into the final tx_outputs_* table

import os
import sys
import subprocess
from pathlib import Path

from tqdm import tqdm

from conf import settings


CHOSEN_FOLDER = ["from_380000", "from_360000","from_340000","from_320000","from_300000",
                 "from_280000", "from_260000","from_240000","from_220000","from_200000",
                 "from_180000", "from_160000","from_140000","from_120000","from_100000",
                 "from_80000",  "from_60000", "from_40000","from_20000","from_0"]

OPERATION_PATHS = [os.path.join(settings.CSV_DIR, r) for r in CHOSEN_FOLDER]


# Tables
BLOCK_TABLE = "public.block_header"


# If you re-run and want a clean reload: Set this parameter to True

RELOAD_PARTITION_BEFORE_LOAD = False
# =========================



# =========================
# INTERNAL HELPERS
# =========================
def run_psql(sql: str) -> None:


    cmd = [
        "psql",
        "-h", settings.PGHOST,
        "-p", settings.PGPORT,
        "-U", settings.PGUSER,
        "-d", settings.PGDATABASE,
        "-v", "ON_ERROR_STOP=1",
        "-q",
        "-c", sql,
    ]
    r = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        env={"PGPASSWORD": settings.PGPASSWORD},
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip() or "psql failed")

def sql_quote_literal(s: str) -> str:
    # safe SQL string literal quoting
    return "'" + s.replace("'", "''") + "'"


def psql_copy(table: str, csv_path: Path) -> None:
    # Server-side COPY: postgres server reads the file.
    path_lit = sql_quote_literal(str(csv_path))
    sql = (
        f"COPY {table} FROM {path_lit} "
        f"WITH (FORMAT csv, HEADER true);"
    )
    run_psql(sql)


def parse_range_dirname(name: str) -> tuple[int, int]:
    # "900000-900999" -> (900000, 900999)
    a, b = name.split("-", 1)
    return int(a), int(b)


def partition_tables_for_height(h: int) -> str:
    p_start = (h // settings.TABLE_PARTITION_STEP) * settings.TABLE_PARTITION_STEP
    # p_start = h
    p_end = p_start + settings.TABLE_PARTITION_STEP - 1
    txo_table = f"public.tx_outputs_p{p_start:0{settings.HEIGHT_DIGITS}d}_{p_end:0{settings.HEIGHT_DIGITS}d}"
    txi_table = f"public.tx_inputs_p{p_start:0{settings.HEIGHT_DIGITS}d}_{p_end:0{settings.HEIGHT_DIGITS}d}"
    return txo_table, txi_table, p_start, p_end



# =========================
# MAIN LOGIC
# =========================

def main():


    for path in OPERATION_PATHS:
        root = Path(path)
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
            txi_csv = d / "tx_inputs.pg.csv"
            if bh_csv.exists() and txo_csv.exists() and txi_csv.exists():
                jobs.append((d, start_h, end_h, bh_csv, txo_csv, txi_csv))

        if not jobs:
            print(f"No valid data folders under: {root}", file=sys.stderr)
            sys.exit(1)

        truncated_parts_for_txo: set[str] = set()
        truncated_parts_for_txi: set[str] = set()
        ok = 0
        failed = 0

        pbar = tqdm(jobs, unit="folder", desc="Importing")
        for d, start_h, end_h, bh_csv, txo_csv, txi_csv in pbar:
            txo_table, txi_table, part_start, part_end = partition_tables_for_height(start_h)

            # update tqdm line
            pbar.set_postfix_str(f"{d.name}")

            try:
                # This move is to delete all rows and write in the sub table again,
                # if you want to update the sub table.
                # If you don't want to do so, set RELOAD_PARTITION_BEFORE_LOAD to False.
                if RELOAD_PARTITION_BEFORE_LOAD and ((txo_table not in truncated_parts_for_txo) or (txi_table not in truncated_parts_for_txi)):
                    run_psql(f"TRUNCATE TABLE {txo_table};")
                    run_psql(f"TRUNCATE TABLE {txi_table};")
                    run_psql(
                        f"DELETE FROM {BLOCK_TABLE} "
                        f"WHERE height >= {part_start} AND height <= {part_end};"
                    )
                    truncated_parts_for_txo.add(txo_table)
                    truncated_parts_for_txi.add(txi_table)

                psql_copy(BLOCK_TABLE, bh_csv)
                psql_copy(txo_table, txo_csv)
                psql_copy(txi_table, txi_csv)

                ok += 1
            except Exception as e:
                failed += 1
                tqdm.write(f"FAILED {d.name}: {e}")
                continue


            # print("d:",d)
            # print("start_h:",start_h)
            # print("end_h:",end_h)
            # print("bh_csv:",bh_csv)
            # print("txo_csv:",txo_csv)
            # print("txi_csv:",txi_csv)

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