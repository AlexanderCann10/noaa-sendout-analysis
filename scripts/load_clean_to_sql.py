import os
import pandas as pd
from sqlalchemy import create_engine

# db config
DB_NAME = "noaa_weather"
DB_USER = "postgres"
DB_PASSWORD = "$oN413800"
DB_HOST = "localhost"
DB_PORT = "5432"
SCHEMA = "gsd"
TABLE = "cleaned_sendout_data"

# path to cleaned csv files
CLEANED_DIR = os.path.join("data", "cleaned")

# connect to postgres
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def load_csv_to_sql(file_path):
    print(f"loading {file_path}...")
    try:
        df = pd.read_csv(file_path)

        # rename columns and fix dates
        df = df.rename(columns={"date": "gas_day"})
        df["gas_day"] = pd.to_datetime(df["gas_day"]).dt.date

        # append to sql table
        df.to_sql(
            TABLE,
            engine,
            schema=SCHEMA,
            if_exists="append",
            index=False,
            method="multi",
        )
        print(f"loaded {file_path}")
    except Exception as e:
        print(f"error with {file_path}: {e}")

def main():
    print(f"looking in {CLEANED_DIR}")
    for filename in os.listdir(CLEANED_DIR):
        print(f"found file: {filename}")
        if filename.endswith(".csv"):
            file_path = os.path.join(CLEANED_DIR, filename)
            load_csv_to_sql(file_path)

if __name__ == "__main__":
    main()

