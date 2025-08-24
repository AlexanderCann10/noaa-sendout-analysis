"""
clean_mr_stations.py

cleans m&r stations sheet from gsd report workbooks
"""

import pandas as pd
import os

print(f"running: {__file__}")

def clean_mr_stations(input_file, output_dir):
    """
    cleans m&r stations sheet from a gsd report workbook

    args:
        input_file (str): path to raw workbook
        output_dir (str): directory to save cleaned csv

    returns:
        pd.DataFrame: cleaned dataframe
    """
    filename = os.path.basename(input_file)
    year = ''.join([c for c in filename if c.isdigit()])

    # load m&r stations sheet
    df = pd.read_excel(input_file, sheet_name='M&R Stations', skiprows=6, engine='xlrd')

    # cut off at row 385
    df = df.iloc[:385]

    # drop rows where date is NaN (monthly borders and blank rows)
    df = df[df['DATE'].notna()]

    # drop rows where any cell contains 'TOTAL' (grand totals)
    df = df[~df.apply(lambda row: row.astype(str).str.contains('TOTAL', case=False).any(), axis=1)]

    # drop blank and check number columns
    cols_to_drop = ['Unnamed: 1', 'CHECK NUMBER', 'CHECK NUMBER.1']
    df.drop(columns=[col for col in cols_to_drop if col in df.columns], inplace=True, errors='ignore')

    # reset index
    df.reset_index(drop=True, inplace=True)

    # melt tetco stations
    tetco_stations = ['0-30', '0-34', 'PENROSE', '0-60']
    tetco = df.melt(id_vars=['DATE'], value_vars=tetco_stations,
                    var_name='attribute', value_name='value')
    tetco['pipeline'] = 'tetco'

    # add tetco btu as attribute
    tetco_btu = df[['DATE', 'BTU']].rename(columns={'BTU': 'value'})
    tetco_btu['attribute'] = 'BTU'
    tetco_btu['pipeline'] = 'tetco'

    # melt transco stations
    transco_stations = ['RICHMOND', 'WHITMAN', 'ASHMEAD', 'SOMERTON', 'IVYHILL']
    transco = df.melt(id_vars=['DATE'], value_vars=transco_stations,
                      var_name='attribute', value_name='value')
    transco['pipeline'] = 'transco'

    # add transco btu as attribute
    transco_btu = df[['DATE', 'BTU.1']].rename(columns={'BTU.1': 'value'})
    transco_btu['attribute'] = 'BTU'
    transco_btu['pipeline'] = 'transco'

    # combine all
    clean_df = pd.concat([tetco, tetco_btu, transco, transco_btu], ignore_index=True)
    clean_df.rename(columns={'DATE': 'date'}, inplace=True)

    # build output filename
    output_file = os.path.join(output_dir, f"MR_Stations_{year}_clean.csv")
    os.makedirs(output_dir, exist_ok=True)
    clean_df.to_csv(output_file, index=False)

    print(f"saved cleaned file: {output_file}")
    return clean_df

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(base_dir, "..", "data", "raw_excel")
    output_dir = os.path.join(base_dir, "..", "data", "cleaned")
    os.makedirs(output_dir, exist_ok=True)

    # get all GSD REPORT FY *.xls files
    files = sorted([f for f in os.listdir(raw_dir) if f.startswith("GSD REPORT FY") and f.endswith(".xls")])

    for f in files:
        input_path = os.path.join(raw_dir, f)
        print(f"\nprocessing: {input_path}")
        try:
            clean_mr_stations(input_path, output_dir)
        except Exception as e:
            print(f"failed to clean {f}: {e}")




