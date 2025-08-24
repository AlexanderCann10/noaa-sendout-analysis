import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# db config
DB_NAME = "noaa_weather"
DB_USER = "postgres"
DB_PASSWORD = "$oN413800"
DB_HOST = "localhost"
DB_PORT = "5432"
SCHEMA = "gsd"
TABLE = "sendout_weather_join"

# connect to postgres
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# load data
query = f"select * from {SCHEMA}.{TABLE}"
df = pd.read_sql(query, engine)

# drop unused columns
df = df.drop(columns=["wdir", "wpgt"], errors="ignore")

# check nulls
print("null counts after dropping wdir/wpgt:")
print(df.isnull().sum())

# scatter: sendout vs avg temperature
plt.scatter(df["tavg"], df["value"], alpha=0.3)
plt.title("Sendout vs Avg Temperature")
plt.xlabel("Avg Temperature (F)")
plt.ylabel("Sendout")
plt.show()

# scatter: sendout vs heating degree days
plt.scatter(df["hdd"], df["value"], alpha=0.3, color="orange")
plt.title("Sendout vs Heating Degree Days")
plt.xlabel("Heating Degree Days")
plt.ylabel("Sendout")
plt.show()

import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# db config
DB_NAME = "noaa_weather"
DB_USER = "postgres"
DB_PASSWORD = "$oN413800"
DB_HOST = "localhost"
DB_PORT = "5432"
SCHEMA = "gsd"
TABLE = "sendout_weather_join"

# connect to postgres
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# load data
query = f"select * from {SCHEMA}.{TABLE}"
df = pd.read_sql(query, engine)

# drop unused columns
df = df.drop(columns=["wdir", "wpgt"], errors="ignore")

# filter to only station sendout (exclude BTU, ODOR, TEMP)
sendout_df = df[~df["attribute"].str.contains("BTU|ODOR|TEMP", case=False, na=False)].copy()

# check nulls
print("null counts in station sendout data:")
print(sendout_df.isnull().sum())

# scatter: sendout vs avg temperature
plt.scatter(sendout_df["tavg"], sendout_df["value"], alpha=0.3)
plt.title("Sendout vs Avg Temperature (stations only)")
plt.xlabel("Avg Temperature (F)")
plt.ylabel("Sendout")
plt.show()

# scatter: sendout vs heating degree days
plt.scatter(sendout_df["hdd"], sendout_df["value"], alpha=0.3, color="orange")
plt.title("Sendout vs Heating Degree Days (stations only)")
plt.xlabel("Heating Degree Days")
plt.ylabel("Sendout")
plt.show()
