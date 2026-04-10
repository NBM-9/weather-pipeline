import requests
import json
import pandas as pd
import sqlite3
from datetime import datetime

# API
url = "https://api.open-meteo.com/v1/forecast?latitude=38.63&longitude=-90.2&current_weather=true"

# STEP 1: Ingest
response = requests.get(url)
data = response.json()
data['ingested_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

with open("raw_data.json", "a") as f:
    f.write(json.dumps(data) + "\n")

# STEP 2: Transform
records = []
with open("raw_data.json", "r") as f:
    for line in f:
        d = json.loads(line)
        weather = d.get("current_weather", {})

        records.append({
            "temperature": weather.get("temperature"),
            "windspeed": weather.get("windspeed"),
            "time": weather.get("time"),
            "ingested_at": d.get("ingested_at")
        })

df = pd.DataFrame(records)
df.to_csv("clean_data.csv", index=False)

# STEP 3: Load
conn = sqlite3.connect("weather.db")
df.to_sql("weather", conn, if_exists="append", index=False)
conn.commit()
conn.close()

print("Pipeline ran successfully")