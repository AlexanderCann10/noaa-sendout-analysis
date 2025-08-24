import requests
import psycopg2
from collections import defaultdict

# PHI National Aiport 
lat, lon = 39.87326, -75.22681

# User-Agent
headers = {'User-Agent': 'PGW Sendout Model (alex.cann@pgworks.com)'}
point_url = f"https://api.weather.gov/points/{lat},{lon}"
resp = requests.get(point_url, headers=headers)
if resp.status_code != 200:
    print(f"Error fetching point data: {resp.status_code}")
    exit()

forecast_url = resp.json()['properties']['forecast']
forecast_resp = requests.get(forecast_url, headers=headers)
if forecast_resp.status_code != 200:
    print(f"Error fetching forecast data: {forecast_resp.status_code}")
    exit()

forecast_data = forecast_resp.json()
periods = forecast_data['properties']['periods']

# daily summaries 
daily_data = defaultdict(list)
for period in periods:
    day_name = period['name'].split()[0]
    daily_data[day_name].append(period)

# postgres
try:
    conn = psycopg2.connect(
        dbname="noaa_weather",
        user="postgres",       
        password="$oN413800",   
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    for day, entries in daily_data.items():
        temps = [p['temperature'] for p in entries]
        avg_temp = sum(temps) / len(temps)
        high_temp = max(temps)
        low_temp = min(temps)
        conditions = [p['shortForecast'] for p in entries]
        most_common_condition = max(set(conditions), key=conditions.count)
        winds = []
        for p in entries:
            speed_parts = p['windSpeed'].split()
            for s in speed_parts:
                if s.isdigit():
                    winds.append(int(s))
        max_wind = max(winds) if winds else 0

        # forecasts table
        cur.execute("""
            INSERT INTO forecasts (forecast_date, high_temp, low_temp, avg_temp, max_wind, conditions)
            VALUES (CURRENT_DATE, %s, %s, %s, %s, %s)
        """, (high_temp, low_temp, avg_temp, max_wind, most_common_condition))

    conn.commit()
    print("\n✅ Forecasts saved to PostgreSQL successfully.")

except Exception as e:
    print("❌ Error connecting to PostgreSQL:", e)

finally:
    if conn:
        cur.close()
        conn.close()
