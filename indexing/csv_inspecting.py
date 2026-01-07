import os
import pandas as pd

CSV_BASE_PATH = '/data/index/btc/csv'
CHECKING_PATH = 'From_800000/from_920000/920000-920999'

# Load the CSV files
df = pd.read_csv(os.path.join(CSV_BASE_PATH,CHECKING_PATH,'block_header.pg.csv'))

# Check for duplicates in 'block_hash' and 'height'
block_hash_duplicates = df[df.duplicated('block_hash')]
height_duplicates = df[df.duplicated('height')]

# Print duplicates if any
print("Duplicate block_hashes:\n", block_hash_duplicates)
print("Duplicate heights:\n", height_duplicates)