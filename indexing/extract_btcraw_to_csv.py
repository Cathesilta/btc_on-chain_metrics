# Third Version
# Use multi-processing, exploit multi core in CPU
# Monitor the RAM usage
# Use tqdm bar to monitor the process

import os
import sys
import pandas as pd
import decimal
import datetime as dt
from bitcoinrpc.authproxy import AuthServiceProxy

import inspect
from collections.abc import Mapping, Sequence
import time
import multiprocessing as mp
from multiprocessing import Pool

import threading
import psutil
from tqdm import tqdm

import ast

# ----------------------------
# Configuration
# ----------------------------

# Bitcoin Core RPC
RPC_USER = os.environ.get("BTC_RPC_USER", "feiy_btc")
RPC_PASSWORD = os.environ.get("BTC_RPC_PASSWORD", "v&xI1r&qa@=xi=lcroyl")
RPC_HOST = os.environ.get("BTC_RPC_HOST", "127.0.0.1")
RPC_PORT = int(os.environ.get("BTC_RPC_PORT", "8332"))

START_BLOCK = 931000
END_BLOCK = -1
CONFIRMATIONS = 6
TRUNK = 1000

# Do not use more than 3 if RAM 16GB
# Or decrease TRUNK to use more cores
USING_CORE = 2

# Define where to save the CSV files
DIR = '/data/index/btc/csv/from_920000'
FOLDER_NAME = f"{START_BLOCK}-{END_BLOCK}"

# ----------------------------
# Helpers: connections
# ----------------------------

def get_rpc() -> AuthServiceProxy:
    """Return RPC client for Bitcoin Core."""
    uri = f"http://{RPC_USER}:{RPC_PASSWORD}@{RPC_HOST}:{RPC_PORT}"
    return AuthServiceProxy(uri)


# ----------------------------
# Helpers
# ----------------------------


def deep_getsizeof(obj, seen=None):
    if seen is None:
        seen = set()
    oid = id(obj)
    if oid in seen:
        return 0
    seen.add(oid)

    size = sys.getsizeof(obj)

    # dict
    if isinstance(obj, Mapping):
        size += sum(deep_getsizeof(k, seen) + deep_getsizeof(v, seen) for k, v in obj.items())
    # list/tuple/set (but not strings/bytes)
    elif isinstance(obj, (list, tuple, set, frozenset)):
        size += sum(deep_getsizeof(i, seen) for i in obj)

    return size



def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

# ----------------------------
# Build Multi-process
# ----------------------------

def process_chunk(args):
    chunk_start, chunk_end, chunk_path = args

    # Each process creates its own RPC client (IMPORTANT)
    rpc = get_rpc()

    ensure_dir(chunk_path)

    start_time = time.time()
    extract_and_write_to_csv(rpc, chunk_start, chunk_end, chunk_path)
    dt_s = time.time() - start_time

    return (chunk_start, chunk_end, dt_s)


# ----------------------------
# Monitor
# ----------------------------

def ram_monitor_tqdm(position: int, interval=10):
    proc = psutil.Process(os.getpid())

    bar = tqdm(
        total=1,
        position=position,
        bar_format="{desc}",
        leave=True,
    )

    while True:
        rss = proc.memory_info().rss / (1024 ** 2)

        # VMS = how much virtual address space the process has reserved/mapped (includes:
        #   1)loaded libraries,
        #   2)memory-mapped files,
        #   3)reserved-but-not-used regions,
        #   4)anonymous mappings, etc.)
        vms = proc.memory_info().vms / (1024 ** 2)
        sys_mem = psutil.virtual_memory()
        total = sys_mem.total / (1024 ** 2)
        used = sys_mem.used / (1024 ** 2)

        bar.set_description_str(
            f"RAM | proc_rss={rss:.2f}MB | proc_vms={vms:.2f}MB  | sys={used:.1f}/{total:.1f}MB"
        )

        time.sleep(interval)

# ----------------------------
# Helpers: encode binary for csv
# ----------------------------

def bytes_to_pg_bytea(b: bytes) -> str:
    """Postgres bytea hex input format."""
    return "\\x" + b.hex()

def encode_for_csv(v):
    """
    Ensure values written by pandas are Postgres-friendly.
    - bytes -> \\x... (bytea hex)
    - keep others unchanged
    """
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes_to_pg_bytea(bytes(v))
    return v

def write_to_csv_pandas(file_path, data, header=None, bytea_cols=None):
    """
    Write rows to CSV using Pandas.
    Only convert specified columns (by name) to Postgres bytea hex: \\x...
    """
    if not data:
        print(f"No data to write for {file_path}")
        return

    df = pd.DataFrame(data)

    if header:
        df.columns = header

    if bytea_cols:
        for col in bytea_cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda v: ("\\x" + bytes(v).hex())
                    if isinstance(v, (bytes, bytearray, memoryview)) else v
                )

    df.to_csv(file_path, mode='a', header=not os.path.exists(file_path), index=False)

# ----------------------------
# Core Function
# ----------------------------

def extract_and_write_to_csv(rpc: AuthServiceProxy, start_height: int, end_height: int, chunk_path: str):

    proc = mp.current_process()
    # block_hash I save as 32 byte "bin" form, you'll have to "hex" it to 64 byte text form for btc client usage. 
    blocks_header = ['height', 'block_hash', 'time', 'tx_count']
    tx_outputs_header = ['txid', 'vout', 'address', 'value_sats', 'created_height', 'created_time']
    tx_inputs_header = ['prev_txid', 'prev_vout', 'spent_txid', 'spent_height', 'spent_time']

    tx_outputs_data = []
    tx_inputs_data = []

    # Worker index (0-based). Works for Pool workers.
    p = mp.current_process()
    wid = (p._identity[0] - 1) if p._identity else 0  # main process -> 0

    desc = f"W{wid} {start_height}-{end_height}"

    for height in tqdm(
        range(start_height, end_height + 1),
        total=(end_height - start_height + 1),
        position=wid,
        desc=desc,
        dynamic_ncols=True,
        leave=True,
    ):
        
        # tqdm.write(f"[{desc}] \
        #     <<[{proc.name}|pid={os.getpid()}] >> \
        #     <{inspect.currentframe().f_code.co_name}> Processing block {height} of {end_height}")

        block_hash_hex = rpc.getblockhash(height)
        block_hash_bin = bytes.fromhex(block_hash_hex)
        block = rpc.getblock(block_hash_hex, 2)  # verbosity=2 for full transactions

        # Extract block-level data
        # block_time = dt.datetime.fromtimestamp(block["time"], tz=dt.timezone.utc)
        block_time = block["time"]
        tx_count = len(block["tx"])

        # Write block data to CSV (btc_blocks)
        ##### temp commenting

        block_data = [(height, block_hash_bin, block_time, tx_count)]

        write_to_csv_pandas(os.path.join(chunk_path,f"block_header.pg.csv"), 
                            block_data, 
                            header = blocks_header,
                            bytea_cols = ["block_hash"],)
        ##### temp commenting

        for tx in block["tx"]:
            txid = tx["txid"]
            txid_bin = bytes.fromhex(txid)

            # --------------------
            # Outputs: create UTXO records
            # --------------------
            ##### temp commenting

            for vout in tx.get("vout", []):
                n = vout["n"]
                value_btc = decimal.Decimal(vout["value"])
                value_sats = int(round(value_btc * decimal.Decimal('1e8')))  # Convert to satoshis

                # Extract address if available
                scriptpubkey = vout.get("scriptPubKey", {})
                address = scriptpubkey.get("address") or None

                # Prepare output data for CSV
                tx_outputs_data.append([
                    txid_bin, n, address, value_sats, height, block_time
                ])
            ##### temp commenting
            # --------------------
            # Inputs (vin): create spend mapping records
            # --------------------
            if not os.path.exists(os.path.join(chunk_path, "tx_inputs.pg.csv")):
                for vin in tx.get("vin", []):
                    # Coinbase vin has no "txid"/"vout"
                    prev_txid_hex = vin.get("txid")
                    prev_vout = vin.get("vout")

                    if prev_txid_hex is None or prev_vout is None:
                        continue

                    prev_txid_bin = bytes.fromhex(prev_txid_hex)

                    tx_inputs_data.append([
                        prev_txid_bin,          # prev_txid (bytea)
                        int(prev_vout),         # prev_vout (int)
                        txid_bin,               # spent_txid (bytea) - optional but useful
                        int(height),            # spent_height
                        int(block_time),        # spent_time (block header time)
                    ])

    ##### temp commenting

    write_to_csv_pandas(os.path.join(chunk_path,f"tx_outputs.pg.csv"), 
                        tx_outputs_data, 
                        header=tx_outputs_header,
                        bytea_cols=["txid"])
    ##### temp commenting

    write_to_csv_pandas(os.path.join(chunk_path, "tx_inputs.pg.csv"),
                        tx_inputs_data,
                        header=tx_inputs_header,
                        bytea_cols=["prev_txid", "spent_txid"])

    # tqdm.write(f"[{desc}] \
    #            <<[{proc.name}|pid={os.getpid()}] >> \
    #             <{inspect.currentframe().f_code.co_name}> Finished processing blocks from {start_height} to {end_height})")


def main():

    # This thread monitor the RAM usage, printing the usage every 10 sec
    ram_thread = threading.Thread(
        target=ram_monitor_tqdm,
        args=(USING_CORE,),
        daemon=True
    )
    ram_thread.start()

    rpc = get_rpc()
    end_block = END_BLOCK
    if -1 == end_block:
        end_block = rpc.getblockcount()
        end_block = end_block - CONFIRMATIONS

    print(f"<{inspect.currentframe().f_code.co_name}> Starting from block {START_BLOCK} to block {end_block}")

    tasks = []

    for i_block in range(START_BLOCK, end_block, TRUNK):

        chunk_start = i_block
        chunk_end = min(i_block + TRUNK - 1, end_block)
        chunk_path = f"{DIR}/{chunk_start}-{chunk_end}"

        print(f"<{inspect.currentframe().f_code.co_name}> Currently processing from {chunk_start} to {chunk_end} blocks")
        tasks.append((chunk_start, chunk_end, chunk_path))

    n_workers = int(os.environ.get("N_WORKERS", str(USING_CORE)))
    print(f"workers={n_workers}, chunks={len(tasks)}")

    with Pool(processes=n_workers) as pool:
        for chunk_start, chunk_end, dt_s in pool.imap_unordered(process_chunk, tasks):
            print(f"done {chunk_start}-{chunk_end}, time={dt_s:.2f}s")



if __name__ == "__main__":
    sys.exit(main())