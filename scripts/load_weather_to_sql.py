import pandas as pd
from datetime import datetime
from meteostat import Point, Daily, units
from sqlalchemy import create_engine

# db config
DB_NAME = "noaa_weather"
DB_USER = "postgres"
DB_PASSWORD = "$oN413800"
DB_HOST = "localhost"
DB_PORT = "5432"
SCHEMA = "gsd"
TABLE = "daily_weather_phl"

# connect to postgres
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# phl airport location
phl = Point(39.87326, -75.22681)

# date range
start = datetime(2005, 9, 1)
end = datetime(2025, 8, 31)

# fetch daily data
data = Daily(phl, start, end)
data = data.convert(units.imperial)  # fahrenheit, inches, mph
df = data.fetch().reset_index()

# rename columns to match table
df = df.rename(columns={
    'time': 'date',
    'tavg': 'tavg',
    'tmin': 'tmin',
    'tmax': 'tmax',
    'prcp': 'prcp',
    'wdir': 'wdir',
    'wspd': 'wspd',
    'wpgt': 'wpgt',
    'rhum': 'rhum'  # relative humidity may be null
})

# insert into postgres
df.to_sql(
    TABLE,
    engine,
    schema=SCHEMA,
    if_exists='replace',  # replace if already exists
    index=False,
    method='multi'
)

print(f"loaded {len(df)} rows into {SCHEMA}.{TABLE}")
