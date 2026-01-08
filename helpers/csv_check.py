import os
import pandas as pd

CSV_BASE_PATH = '/data/index/btc/csv'
CHECKING_PATH = 'from_920000/920000-920999'


if __name__ == "__main__":

    df_block = pd.read_csv(os.path.join(CSV_BASE_PATH, CHECKING_PATH, 'block_header.pg.csv'))
    df_outputs = pd.read_csv(os.path.join(CSV_BASE_PATH, CHECKING_PATH, 'tx_outputs.pg.csv'))
    df_inputs = pd.read_csv(os.path.join(CSV_BASE_PATH, CHECKING_PATH, 'tx_inputs.pg.csv'))

    print("df_block rows:",len(df_block))
    print("df_block columns:",list(df_block.columns))
    print(df_block.head().to_string(index=False))
    # print(df_block.head())



    print("df_outputs rows",len(df_outputs))
    # print("df_outputs columns:",list(df_outputs.columns))
    # print(df_outputs.head().to_string(index=False))
    # print(df_outputs.head())

    print("tx_inputs rows",len(df_inputs))
    # print("tx_inputs columns:",list(df_inputs.columns))
    # print(df_inputs.head().to_string(index=False))
    # print(df_outputs.head())








    # # Check for duplicates in 'block_hash' and 'height'
    # block_hash_duplicates = df_block[df_block.duplicated('block_hash')]
    # height_duplicates = df_block[df_block.duplicated('height')]

    # # Print duplicates if any
    # print("Duplicate block_hashes:\n", block_hash_duplicates)
    # print("Duplicate heights:\n", height_duplicates)