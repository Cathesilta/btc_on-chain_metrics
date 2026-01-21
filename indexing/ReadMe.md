## Env: 

### Hardware

- Motherboard: Gigabyte B450M (PCIe 3.0)
- CPU: AMD R5 5500 (PCIe 3.0)
- RAM: 16 GB
- Disk: 3.6 TB Samsung 990 PRO NVMe (PCIe 4.0)

### System
- OS: Ubuntu 24.04
- kernel: 6.14.0-37-generic

### Runtime
- Python: 3.12.3
- PostgreSQL: psql (PostgreSQL) 16.11 (Ubuntu 16.11-0ubuntu0.24.04.1)
- bitcoin-cli: Bitcoin Core RPC client version v30.0.0


### Python Dependencies

- Python3 version 3.12.3
- numpy==2.4.0
- pandas==2.3.3
- psutil==7.2.1
- psycopg2-binary==2.9.11
- python-bitcoinrpc==1.0
- python-dateutil==2.9.0.post0
- pytz==2025.2
- six==1.17.0
- tqdm==4.67.1
- tzdata==2025.3
- tqdm





## Usage:

### Workflow

#### 1. Data Extraction (Bitcoin Core → CSV)
- Retrieve block and transaction data via Bitcoin Core RPC.
- GSerialize selected fields into JSON-compatible structures[^corresponding fields] a
- Save data as `.pg.csv` files[^.pg.csv].

#### 2. Database ingestion (CSV → PostgreSQL)
- Load generated `.pg.csv` files into PostgreSQL using `COPY`[^COPY].   

### Scripts

#### 1. "extract_btcraw_to_csv.py". 
- User can assign for what block (height) range to execute with "START_BLOCK" and "END_BLOCK".
- "TRUNK" respects to how many heights data will be saved as a batch of .pg.csv.
#### 2. "postgres_copy.py".
- Manually[^Mannually] assign "CHOSEN_FOLDER", in each folder, the script will find all sub-folders and execute COPY with those .pg.csv in each.
- "Part_STEP" should be correspond to how you "partition"[^partition] the transaction tables.

### Helpers

#### 1. Postgres Helpers
- "helpers/table_operation/create_tables.py". Because I use partition tables, it will be good to have this script to batch-create sub tables.
Use variables "start_height", "end_height" to assign a range. Use variable "step" to assign height partitioning. 
- "helpers/table_operation/check_tables.py". This can check if the rows in sub tales matches to the rows in corresponding csv files. Use variables "FRACTION" and "STEP" to assign the chosen table.
- "helpers/table_operation/create_table_indexes.py". For those transaction sub tables haven't created indexes, this is a supplement.

#### 2. CSV Helpers
- "helpers/csv_check.py". Check csv here.


[^partition]: A single transaction table will be too huge (2 TB) for random writing. Partition the table into ones within 50 GB would be better.
[^Mannually]: I use this fashion becaues this process will be slow in a cunsumer-level computer, it sometimes needs to restart the device to regain efficiency. Many consumer-grade NVMe drives use a portion of their flash memory as a "pseudo-SLC cache" to accelerate write speeds. During continuous large writes, once the cache is exhausted, the drive reverts to the actual TLC/QLC speed, and the throughput drops significantly (sometimes from 2–6 GB/s to a few hundred MB/s). After a period of inactivity, the controller's background process "organizes/moves" the cached data to the TLC/QLC memory, the cache is restored, and therefore the speed increases again. 
[^COPY]: Because writing row by row is way slower. \copy also works, but COPY is a server-level command, will be a little bit faster (depending on the network, since my Postgres in local, the network wouldn't be a problem, so I chose the word "a little bit").
[^corresponding fields]: According to what metrics need, currently for illiquid supply ratio.
[^.pg.csv]: Because .csv will transfer binary data to string, which spends more storage.

