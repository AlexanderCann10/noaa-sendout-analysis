import pandas as pd
import os

# === CONFIG ===
INPUT_DIR = "data/raw_excel"
OUTPUT_FILE = "data/cleaned/MR_Stations_2012_2024.xlsx"
YEARS = list(range(2012, 2025))

# === KNOWN ROWS TO DROP (monthly totals etc.) ===
ROWS_TO_DROP = [38, 70, 101, 133, 165, 195, 227, 258, 290, 321, 353, 385]

# === FUNCTION TO CLEAN A SINGLE FILE ===
def clean_single_file(filepath, fiscal_year):
    print(f"Processing {filepath}")
    try:
        df = pd.read_excel(filepath, sheet_name='M&R Stations', skiprows=6)

        # Drop irrelevant columns
        df = df.drop(columns=['Unnamed: 1', 'CHECK NUMBER'], errors='ignore')

        # Drop summary rows
        df.drop(index=ROWS_TO_DROP, inplace=True, errors='ignore')

        # Forward-fill the date column
        df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
        df['DATE'] = df['DATE'].ffill()

        # Melt the table to long format
        df_melted = df.melt(id_vars=['DATE'], var_name='attribute', value_name='value')
        df_melted.dropna(subset=['value'], inplace=True)
        print(df_melted.head())
        print(f"{len(df_melted)} rows loaded for FY {fiscal_year}")

        # Assign pipeline based on station name
        def infer_pipeline(attr):
            attr = str(attr).upper()
            if 'TRANSCO' in attr:
                return 'transco'
            elif 'TETCO' in attr:
                return 'tetco'
            else:
                return 'unknown'

        df_melted['pipeline'] = df_melted['attribute'].apply(infer_pipeline)
        df_melted['fy'] = fiscal_year

        return df_melted[['DATE', 'attribute', 'value', 'pipeline', 'fy']]

    except Exception as e:
        print(f"Failed to process {filepath}: {e}")
        return pd.DataFrame()  # Return empty on failure


# === MAIN LOOP ===
def main():
    all_data = []

    for year in YEARS:
        filename = f"GSD REPORT FY {year}.xls"
        filepath = os.path.join(INPUT_DIR, filename)
        if os.path.exists(filepath):
            df_cleaned = clean_single_file(filepath, year)
            all_data.append(df_cleaned)
        else:
            print(f"File not found: {filepath}")

    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_excel(OUTPUT_FILE, index=False)
    print(f"Total rows to save: {len(final_df)}")
    print(f"Saved cleaned data to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()