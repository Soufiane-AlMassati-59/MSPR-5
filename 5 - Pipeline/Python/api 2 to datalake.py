import requests
import psycopg2
from datetime import datetime

# Connexion PostgreSQL
conn = psycopg2.connect(
    dbname="goodair",
    user="postgres",
    password="Formation",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

OWM_KEY = "fa328cd88b0933b1bdb3d91b77473ff3"
cities = ["Strasbourg", "Paris", "Lyon", "Toulouse", "Marseille"]
country = "FR"

for city in cities:
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city},{country}&appid={OWM_KEY}"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()

        lat = str(data["coord"].get("lat", ""))
        lon = str(data["coord"].get("lon", ""))

        weather = data["weather"][0]
        weather_main = weather.get("main", "")
        description = weather.get("description", "").title()
        icon = weather.get("icon", "")

        main = data["main"]
        temp         = str(main.get("temp", ""))
        feels_like   = str(main.get("feels_like", ""))
        temp_min     = str(main.get("temp_min", ""))
        temp_max     = str(main.get("temp_max", ""))
        pressure     = str(main.get("pressure", ""))
        humidity     = str(main.get("humidity", ""))
        sea_level    = str(main.get("sea_level", ""))
        ground_level = str(main.get("grnd_level", ""))

        visibility = str(data.get("visibility", ""))
        wind       = data.get("wind", {})
        wind_speed = str(wind.get("speed", ""))
        wind_deg   = str(wind.get("deg", ""))
        clouds     = str(data.get("clouds", {}).get("all", ""))

        sys = data.get("sys", {})
        sunrise  = datetime.utcfromtimestamp(sys.get("sunrise", 0)).strftime("%Y-%m-%d %H:%M:%S")
        sunset   = datetime.utcfromtimestamp(sys.get("sunset", 0)).strftime("%Y-%m-%d %H:%M:%S")
        measured = datetime.utcfromtimestamp(data.get("dt", 0)).strftime("%Y-%m-%d %H:%M:%S")

        # Vérification si les données existent déjà pour la ville et la date mesurée
        cursor.execute("""
            SELECT 1
            FROM raw_weather_data
            WHERE city = %s AND measured_at_utc = %s
        """, (city, measured))
        exists_weather = cursor.fetchone()

        if not exists_weather:
            cursor.execute("""
                INSERT INTO raw_weather_data (
                    city, country, lat, lon,
                    weather_main, weather_description, weather_icon,
                    temp, feels_like, temp_min, temp_max,
                    pressure, humidity, sea_level, ground_level,
                    visibility, wind_speed, wind_deg, clouds,
                    sunrise, sunset, measured_at_utc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                city, country, lat, lon,
                weather_main, description, icon,
                temp, feels_like, temp_min, temp_max,
                pressure, humidity, sea_level, ground_level,
                visibility, wind_speed, wind_deg, clouds,
                sunrise, sunset, measured
            ))
            print(f"[✅] Météo insérée pour {city}")
        else:
            print(f"[⚠️] Données météo déjà existantes pour {city} ({measured})")

    except requests.exceptions.RequestException as e:
        print(f"[❌] Erreur pour {city} : {e}")

conn.commit()
cursor.close()
conn.close()
