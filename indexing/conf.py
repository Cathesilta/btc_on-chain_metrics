# This conf file works for scope in the indexing folder's script


import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:

    # Bitcoin Core Client RPC Login Information
    RPC_USER: str = os.environ.get("BTC_RPC_USER", "feiy_btc")
    RPC_PASSWORD: str = os.environ.get("BTC_RPC_PASSWORD", "v&xI1r&qa@=xi=lcroyl")
    RPC_HOST: str = os.environ.get("BTC_RPC_HOST", "127.0.0.1")
    RPC_PORT: int = int(os.environ.get("BTC_RPC_PORT", "8332"))

    # Block Management
    HEIGHT_DIGITS: int = int(os.environ.get("HEIGHT_DIGITS", "6"))

    # Postgres Login Information
    PGHOST: str = os.environ.get("PGHOST", "127.0.0.1")
    PGPORT: int = int(os.environ.get("PGPORT", "5432"))
    PGDATABASE: str = os.environ.get("PGDATABASE", "btc_index")
    PGUSER: str = os.environ.get("PGUSER", "btcetl")
    PGPASSWORD: str = os.environ.get("PGPASSWORD", "strongpassword")

    # Postgres table settings
    TABLE_PARTITION_STEP: int = int(os.environ.get("TABLE_PARTITION_STEP", "20000"))

    # CSV settings
    CSV_DIR: str = str(os.environ.get("CSV_DIR", "/data/index/btc/csv/"))


settings = Settings()