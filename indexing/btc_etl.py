"""
Bitcoin → Postgres ETL (raw index for later metrics).

- Reads blocks from Bitcoin Core via RPC (JSON)
- Stores:
    - btc_blocks(height, block_hash, time, tx_count)
    - btc_tx_outputs(txid, vout, address, value_sats,
                     created_height, created_time,
                     spent_height, spent_time)
    - btc_metadata(key, value)  for ETL progress

Incremental:
    - tracks last_processed_height in btc_metadata
    - on each run, imports from (last_processed_height + 1) to (tip - CONFIRMATIONS)
"""




import os
import sys
import datetime as dt
from typing import Optional

import psycopg2
from psycopg2.extensions import connection as PgConnection
from bitcoinrpc.authproxy import AuthServiceProxy

import pandas as pd
import decimal
import time


# ----------------------------
# Configuration
# ----------------------------

# Bitcoin Core RPC
RPC_USER = os.environ.get("BTC_RPC_USER", "feiy_btc")
RPC_PASSWORD = os.environ.get("BTC_RPC_PASSWORD", "v&xI1r&qa@=xi=lcroyl")
RPC_HOST = os.environ.get("BTC_RPC_HOST", "127.0.0.1")
RPC_PORT = int(os.environ.get("BTC_RPC_PORT", "8332"))

# Postgres (database=btc_index)
PGHOST = os.environ.get("PGHOST", "127.0.0.1")
PGDATABASE = os.environ.get("PGDATABASE", "btc_index")
PGUSER = os.environ.get("PGUSER", "btcetl")
PGPASSWORD = os.environ.get("PGPASSWORD", "strongpassword")

# Safety margin for reorgs
# 1 block is roughly 10 mins
# 6 blocks deep can be considered hard to reorg
CONFIRMATIONS = 6

# Checkpoint
RESTARTING_BLOCK = 369990   

# Commit every N blocks for performance (tune later if needed)
COMMIT_INTERVAL = 1000


# ----------------------------
# Helpers: connections
# ----------------------------

def get_rpc() -> AuthServiceProxy:
    """Return RPC client for Bitcoin Core."""
    uri = f"http://{RPC_USER}:{RPC_PASSWORD}@{RPC_HOST}:{RPC_PORT}"
    return AuthServiceProxy(uri)


def get_db() -> PgConnection:
    """Return Postgres connection to btc_index as btcetl."""
    dsn = (
        f"dbname={PGDATABASE} user={PGUSER} "
        f"password={PGPASSWORD} host={PGHOST}"
    )
    return psycopg2.connect(dsn)


# ----------------------------
# Schema & metadata
# ----------------------------

def ensure_schema(conn: PgConnection) -> None:
    """Create tables if they do not exist."""
    with conn.cursor() as cur:
        # Blocks
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS btc_blocks (
                height        INTEGER PRIMARY KEY,
                block_hash          TEXT NOT NULL,
                time          TIMESTAMPTZ NOT NULL,
                tx_count      INTEGER NOT NULL
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS btc_blocks_time_idx "
            "ON btc_blocks(time);"
        )

        # Outputs (UTXO-level)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS btc_tx_outputs (
                txid           TEXT NOT NULL,
                vout           INTEGER NOT NULL,
                address        TEXT,
                value_sats     BIGINT NOT NULL,
                created_height INTEGER NOT NULL,
                created_time   TIMESTAMPTZ NOT NULL,
                spent_height   INTEGER,
                spent_time     TIMESTAMPTZ,
                PRIMARY KEY (txid, vout)
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS btc_out_created_height_idx "
            "ON btc_tx_outputs(created_height);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS btc_out_spent_height_idx "
            "ON btc_tx_outputs(spent_height);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS btc_out_address_idx "
            "ON btc_tx_outputs(address);"
        )

        # Metadata
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS btc_metadata (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )

        # Initialize last_processed_height if missing
        cur.execute(
            """
            INSERT INTO btc_metadata(key, value)
            VALUES ('last_processed_height', '-1')
            ON CONFLICT (key) DO NOTHING;
            """
        )

    conn.commit()


def get_metadata(conn: PgConnection, key: str) -> Optional[str]:
    """Return metadata value for key, or None if missing."""
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM btc_metadata WHERE key = %s", (key,))
        row = cur.fetchone()
        return row[0] if row else None


def set_metadata(conn: PgConnection, key: str, value: str) -> None:
    """Set metadata key to value (upsert)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO btc_metadata(key, value)
            VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value;
            """,
            (key, value),
        )
    conn.commit()


# ----------------------------
# ETL core
# ----------------------------

def import_new_blocks(conn: PgConnection, rpc: AuthServiceProxy) -> int:
    """
    Import new blocks from Bitcoin Core into Postgres.

    Data flow:
      Bitcoin Core (RPC JSON) -> parse -> INSERT/UPDATE into Postgres tables

    Returns:
      last processed block height after this run (ETL checkpoint).
    """


    # 1) Read the ETL checkpoint from Postgres.
    #    This is NOT Bitcoin metadata; it's your own state so ETL can resume incrementally.
    last_h_str = get_metadata(conn, "last_processed_height")


    # 2) If checkpoint is missing, stop.
    #    This should never happen if ensure_schema() inserted it.
    if last_h_str is None:
        raise RuntimeError("last_processed_height not initialized")

    # 3) Convert checkpoint to int: last imported block height.
    last_h = int(last_h_str)

    # 4) Ask Bitcoin Core for the current chain tip height.
    #    This is the best-known block height at the moment.
    tip_height = rpc.getblockcount()


    # 5) Define the maximum height we will import in this run:
    #    tip_height - CONFIRMATIONS is a safety margin against short reorgs.
    max_height = tip_height - CONFIRMATIONS

    # 6) If the database already has everything up to max_height, nothing to do.
    if max_height <= last_h:
        print("No new blocks to import.")
        return last_h

    # 7) Log the planned import range.
    #    We will import from last_h+1 up to max_height inclusive.
    print(f"Importing blocks from {last_h + 1} to {max_height} "
          f"(tip={tip_height}, confirmations={CONFIRMATIONS})")

    # 8) Track the highest height that we have durably committed to Postgres.
    #    This is important for correct checkpointing.
    current_committed = last_h


    # 9) Main loop: iterate over each height we need to import.
    # for height in range(last_h + 1, max_height + 1):


    
    for height in range(RESTARTING_BLOCK, max_height + 1):

        print("Processing the {} block out of {} blocks".format(height, max_height))

        # 9.1) Convert height -> block hash.
        #      Bitcoin Core indexes main-chain blocks by height, but fetch APIs use hash.
        block_hash = rpc.getblockhash(height)

        # 9.2) Fetch the full decoded block (verbosity=2).
        #      This includes:
        #        - block header fields
        #        - "tx": full decoded transactions
        block = rpc.getblock(block_hash, 2)

        # 9.3) Convert block["time"] (unix seconds) into a timezone-aware UTC datetime.
        #      (Replace utcfromtimestamp to avoid naive datetime.)
        block_time = dt.datetime.fromtimestamp(block["time"], tz=dt.timezone.utc
)
        
        # 9.4) Extract decoded transactions list from the block JSON.
        txs = block["tx"]

        # 9.5) Open a DB cursor. All SQL in this "with" block uses the same cursor.
        # This does not auto-commit; commit is controlled below.
        with conn.cursor() as cur:
            # 9.5.1) Insert the block row into btc_blocks.
            #        ON CONFLICT DO NOTHING prevents duplicates if rerun.
            # start_time = time.time()
            cur.execute(
                """
                INSERT INTO btc_blocks(height, block_hash, time, tx_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (height) DO NOTHING;
                """,
                (height, block_hash, block_time, len(txs)),
            )
            # end_time = time.time()
            # execution_time = end_time - start_time
            # print(f"Time taken for block inserting: {execution_time} seconds")
            # exit(0)
            

            # 9.5.2) Process each transaction in this block.
            tx_outputs_data = []
            tx_inputs_data = []
            for tx in txs:
                txid = tx["txid"]

                # --------------------
                # Outputs: create UTXO records
                # --------------------

                # 9.5.2.1) For each vout, we create a row in btc_tx_outputs.
                #          This becomes "UTXO-like" state in SQL:
                #            - created_height/time are set now
                #            - spent_height/time are NULL until later when spent
                # start_time = time.time()
                for vout in tx.get("vout", []):
                    # This is actually the vout index
                    n = vout["n"]
                    value_btc = vout["value"]
                    value_btc = decimal.Decimal(value_btc)
                    value_sats = int(round(value_btc * decimal.Decimal('1e8')))
                    

                    # 9.5.2.1.1) Extract an address if available.
                    #          Many script types won't present a simple "addresses" list.
                    #          We store the first address only (lossy) or None.
                    scriptpubkey = vout.get("scriptPubKey", {})
                    address = scriptpubkey.get("address") or None

                    print("address:",address)
                    exit(0)

                    tx_outputs_data.append({
                        'txid': txid,
                        'vout': n,
                        'address': address,
                        'value_sats': value_sats,
                        'created_height': height,
                        'created_time': block_time
                    })
                # end_time = time.time()
                # execution_time = end_time - start_time

                # print(f"Time taken for vout reading: {execution_time} seconds")
                # exit(0)
                

                # 9.5.2.2) For each vin (input), mark the referenced previous output as spent.
                #          Spending means:
                #            the referenced (prev_txid, prev_vout) now has spent_height/time.
                # start_time = time.time()
                for vin in tx.get("vin", []):

                    if "txid" not in vin:
                        continue

                    prev_txid = vin["txid"]
                    prev_vout = vin["vout"]

                    tx_inputs_data.append({
                        'spent_height': height,
                        'spent_time': block_time,
                        'txid': prev_txid,
                        'vout': prev_vout
                    })
                # end_time = time.time()
                # execution_time = end_time - start_time
                # print(f"Time taken for vin reading: {execution_time} seconds")
                # exit(0)

            tx_outputs_df = pd.DataFrame(tx_outputs_data)
            tx_inputs_df = pd.DataFrame(tx_inputs_data)

            # 9.5.3) Bulk insert/update
            with conn.cursor() as cur:
                # --------------------
                # Outputs: create UTXO records
                # --------------------
                # Bulk insert tx outputs
                # start_time = time.time()
                for _, row in tx_outputs_df.iterrows():
                    cur.execute(
                        # On Conflict do nothing is a safe upsert, meaning if conflic, skip
                        """
                        INSERT INTO btc_tx_outputs(
                            txid, vout, address, 
                            value_sats, created_height, created_time, 
                            spent_height, spent_time
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL)
                        ON CONFLICT (txid, vout) DO NOTHING;
                        """, 
                        (row.iloc[0], row.iloc[1], row.iloc[2], row.iloc[3], row.iloc[4], row.iloc[5]))
                # end_time = time.time()
                # execution_time = end_time - start_time
                # print(f"Time taken for vout inserting: {execution_time} seconds")
                # exit(0)
                # -------------------
                # Inputs: mark spends
                # --------------------
                # Bulk update tx inputs
                # start_time = time.time()
                # for _, row in tx_inputs_df.iterrows():
                #     cur.execute(
                #         """
                #             UPDATE btc_tx_outputs
                #             SET spent_height = %s, 
                #                 spent_time   = %s
                #             WHERE txid = %s 
                #               AND vout = %s;
                #         """, 
                #         (row.iloc[0], row.iloc[1], row.iloc[2], row.iloc[3]))
                # end_time = time.time()
                # execution_time = end_time - start_time
                # print(f"Time taken for vin updating: {execution_time} seconds")
                # exit(0)




        # 9.6) Commit periodically (every COMMIT_INTERVAL blocks) to reduce overhead.
        #      This makes ETL restartable at block boundaries near the last commit.
        if height % COMMIT_INTERVAL == 0:
            conn.commit()
            
            current_committed = height
            print(f"... committed up to height {height}")

    # 10) After loop, commit any remaining work not committed by the periodic rule.
    conn.commit()
    current_committed = max_height

    # 11) Update the ETL checkpoint in btc_metadata to the last committed height.
    #     This controls the next run’s start height.
    set_metadata(conn, "last_processed_height", str(current_committed))

    # 12) Log and return the checkpoint.
    print(f"Import complete. last_processed_height = {current_committed}")

    return current_committed


# ----------------------------
# Main entrypoint
# ----------------------------

def main() -> int:
    print("Connecting to Bitcoin Core RPC...")
    
    rpc = get_rpc()


    print("Connecting to Postgres...")
    conn = get_db()


    try:
        print("Ensuring schema...")
        ensure_schema(conn)

        print("Running ETL (incremental)...")
        last_height = import_new_blocks(conn, rpc)
        print(f"Done. Last processed height: {last_height}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())