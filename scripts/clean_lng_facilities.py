import os
import re
import pandas as pd

# ----- paths -----
RAW_DIR = r"C:\Users\Alex.CannPgwork.com\OneDrive - Philadelphia Gas Works\noaa_sendout_projections\data\raw_excel"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "data", "cleaned")
os.makedirs(OUTPUT_DIR, exist_ok=True)

SHEET_NAME = "LNG Facilities"

# rows to keep (Excel absolute rows), inclusive window 8..384
ROW_START = 8
ROW_END = 384

# explicit monthly divider rows to drop (Excel absolute row numbers)
BORDER_ROWS = {38, 70, 101, 133, 165, 195, 227, 258, 290, 321, 353}

# we’ll read by letters and assign names manually (no Excel headers)
USECOLS_LETTERS = ["A", "E", "H", "I", "K", "L", "U", "V", "X", "Y"]

COL_NAMES = [
    "date",
    "RICHMOND__LIQUEFACTION",
    "RICHMOND__BOILOFF",
    "RICHMOND__VAPORIZATION",
    "RICHMOND__TRUCKING",
    "RICHMOND__INVENTORY",
    "PASSYUNK__BOILOFF",
    "PASSYUNK__VAPORIZATION",
    "PASSYUNK__RESERVED",
    "PASSYUNK__INVENTORY",
]


def compute_fy(d):
    """
    Fiscal year: Sept 1 → Aug 31
    FY = calendar year if month <= 8, else year + 1
    """
    if pd.isna(d):
        return pd.NA
    if not isinstance(d, (pd.Timestamp, )):
        d = pd.to_datetime(d, errors="coerce")
    if pd.isna(d):
        return pd.NA
    return int(d.year + 1) if d.month >= 9 else int(d.year)


def longify(df_wide, source_year):
    """
    Wide -> long using the temporary headers "PLANT__MEASURE"
    Returns: date, plant, measure, value, fy
    """
    id_cols = ["date"]
    value_cols = [c for c in df_wide.columns if c not in id_cols]

    long_df = df_wide.melt(
        id_vars=id_cols,
        value_vars=value_cols,
        var_name="plant_measure",
        value_name="value"
    )

    # split "PLANT__MEASURE"
    plant_measure_split = long_df["plant_measure"].str.split("__", n=1, expand=True)
    long_df["plant"] = plant_measure_split[0]
    long_df["measure"] = plant_measure_split[1]
    long_df.drop(columns=["plant_measure"], inplace=True)

    # coerce date
    long_df["date"] = pd.to_datetime(long_df["date"], errors="coerce").dt.date

    # numeric cleaning: allow commas/whitespace; drop non-numeric gracefully
    def _to_num(x):
        if pd.isna(x):
            return pd.NA
        s = str(x).strip()
        # remove commas and spaces
        s = re.sub(r"[,\s]", "", s)
        try:
            return float(s)
        except Exception:
            return pd.NA

    long_df["value"] = long_df["value"].map(_to_num)

    # derive fy from date (handles leap years/extra days naturally)
    long_df["fy"] = long_df["date"].map(
        lambda d: compute_fy(pd.to_datetime(d)) if pd.notna(d) else pd.NA
    )

    # add hint of which FY workbook this came from
    long_df["source_year_hint"] = source_year

    # order columns
    long_df = long_df[["date", "plant", "measure", "value", "fy", "source_year_hint"]]
    return long_df


def read_one_workbook(path):
    """
    Read rows 8..384 inclusive, drop monthly border rows,
    select columns by letter A/E/H/I/K/L/U/V/X/Y, assign names, return wide DataFrame.
    """
    # compute how many rows to read after skipping first 7
    nrows = ROW_END - (ROW_START - 1)  # 384 - 7 = 377

    df = pd.read_excel(
        path,
        sheet_name=SHEET_NAME,
        engine="xlrd",
        usecols=",".join(USECOLS_LETTERS),
        skiprows=ROW_START - 1,  # skip first 7 rows to start at row 8
        nrows=nrows,
        header=None,             # treat row 8 as data, not headers
    )

    # assign our own column names in the same order as USECOLS_LETTERS
    df.columns = COL_NAMES

    # rebuild the original excel row number for filtering borders:
    # first row after skip corresponds to Excel row 8.
    df["_excel_row"] = range(ROW_START, ROW_END + 1)

    # drop monthly border rows
    df = df[~df["_excel_row"].isin(BORDER_ROWS)].copy()

    # drop rows where date is NaN (safety)
    df = df[df["date"].notna()].copy()

    # done with helper column
    df.drop(columns=["_excel_row"], inplace=True)

    return df


def main():
    all_long = []

    # restrict to FY2012..FY2024
    files = []
    for fname in os.listdir(RAW_DIR):
        if fname.startswith("GSD REPORT FY") and fname.endswith(".xls"):
            digits = "".join(ch for ch in fname if ch.isdigit())
            if digits:
                yr = int(digits)
                if 2012 <= yr <= 2024:
                    files.append((yr, fname))

    files.sort()

    for yr, fname in files:
        fpath = os.path.join(RAW_DIR, fname)
        print(f"\nprocessing: {fpath}")
        try:
            wide_df = read_one_workbook(fpath)
            long_df = longify(wide_df, source_year=yr)
            all_long.append(long_df)
            print(f"  rows added: {len(long_df)}")
        except Exception as e:
            print(f"  failed to clean {fname}: {e}")

    if not all_long:
        print("no data extracted.")
        return

    final_df = pd.concat(all_long, ignore_index=True)

    # final sort for convenience
    final_df = final_df.sort_values(["fy", "date", "plant", "measure"]).reset_index(drop=True)

    # save CSV + Excel
    csv_path = os.path.join(OUTPUT_DIR, "LNG_Facilities_long_consolidated.csv")
    xlsx_path = os.path.join(OUTPUT_DIR, "LNG_Facilities_long_consolidated.xlsx")
    final_df.to_csv(csv_path, index=False)
    try:
        final_df.to_excel(xlsx_path, index=False)
    except Exception as e:
        print(f"excel save failed ({e}); csv was saved successfully.")

    print(f"\nsaved: {csv_path}")
    print(f"saved: {xlsx_path}")


if __name__ == "__main__":
    print(f"running: {__file__}")
    main()
