import os
import psycopg2
import pandas as pd
from pathlib import Path
from tqdm import tqdm

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))
from indexing.conf import settings


FRACTION = 40000

BLOCK_TABLE = "block_header"
INPUTS_TABLE = f"tx_inputs_p{FRACTION:0{settings.HEIGHT_DIGITS}d}_{(FRACTION+settings.TABLE_PARTITION_STEP-1):06d}"
OUTPUTS_TABLE = f"tx_outputs_p{FRACTION:0{settings.HEIGHT_DIGITS}d}_{(FRACTION+settings.TABLE_PARTITION_STEP-1):06d}"
CSV_CHECKING_PATH = f"/data/index/btc/csv/from_{FRACTION}"

block_row = 0
inputs_row = 0
outputs_row = 0


subdirs = [p for p in Path(CSV_CHECKING_PATH).iterdir() if p.is_dir()]

for subdir in tqdm(subdirs, desc="Scanning directories"):
    print(f"checking directory {subdir}...")
    for file in subdir.iterdir():
        if file.is_file() and file.name == 'block_header.pg.csv':
            # print(f"counting for {subdir.name}/{file.name}")
            df_block = pd.read_csv(os.path.join(subdir, 'block_header.pg.csv'))
            block_row += len(df_block)
            # print("outputs_row:",block_row)

        elif file.is_file() and file.name == 'tx_inputs.pg.csv':
            # print(f"counting for {subdir.name}/{file.name}")
            df_inputs = pd.read_csv(os.path.join(subdir, 'tx_inputs.pg.csv'))
            inputs_row += len(df_inputs)
            # print("outputs_row:",inputs_row)

        elif file.is_file() and file.name == 'tx_outputs.pg.csv':
            # print(f"counting for {subdir.name}/{file.name}")
            df_outputs = pd.read_csv(os.path.join(subdir, 'tx_outputs.pg.csv'))
            outputs_row += len(df_outputs)
            # print("outputs_row:",outputs_row)

print("Final block_row:",block_row)
print("Final inputs_row:",inputs_row)
print("Final outputs_row:",outputs_row)


                    






conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="btc_index",
    user="btcetl",
    password="strongpassword",
)

with conn:
    with conn.cursor() as cur:

        cur.execute(
            """
            SELECT COUNT(*)
            FROM block_header
            WHERE height BETWEEN %s AND %s;
            """,
            (FRACTION, FRACTION+19999)
        )
        block_rows = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(*) FROM {INPUTS_TABLE};")
        inputs_rows = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(*) FROM {OUTPUTS_TABLE};")
        outputs_rows = cur.fetchone()[0]


conn.close()


print(f"In Postgres, you have {block_rows} rows for table {BLOCK_TABLE} in range {FRACTION} to {FRACTION+19999} height")
print(f"And {inputs_rows} rows for table {INPUTS_TABLE}")
print(f"And {outputs_rows} rows for table {OUTPUTS_TABLE}")


if block_row == block_rows and inputs_row == inputs_rows and outputs_row == outputs_rows:
    print("They perfectly matches!")


