import pandas as pd
import plotly.express as px
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

# plot: Sendout vs Avg Temperature, colored by station, hover shows date + station
fig_temp = px.scatter(
    df,
    x="tavg",
    y="value",
    color="attribute",
    hover_data=["gas_day", "attribute", "value"],
    title="Sendout vs Avg Temperature (Interactive)"
)
fig_temp.show()

# plot: Sendout vs Heating Degree Days
fig_hdd = px.scatter(
    df,
    x="hdd",
    y="value",
    color="attribute",
    hover_data=["gas_day", "attribute", "value"],
    title="Sendout vs Heating Degree Days (Interactive)"
)
fig_hdd.show()




