import requests
import psycopg2
from datetime import datetime, timezone

# Connexion à PostgreSQL
conn = psycopg2.connect(
    dbname="goodair",
    user="postgres",
    password="Formation",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Liste des villes à interroger
cities = ["paris", "lyon", "marseille", "toulouse", "strasbourg"]
API_TOKEN = "10155aae3dc65617791b3b1cf5a13492adbd18f5"

# Fonction utilitaire pour IAQI
def get_iaqi_value(iaqi_dict, key):
    return str(iaqi_dict.get(key, {}).get("v")) if key in iaqi_dict else None

# Fonction utilitaire pour forecast
def get_forecast_value(forecast_dict, i, pollutant, field):
    try:
        return str(forecast_dict.get(pollutant, [])[i].get(field))
    except:
        return None

# Fonction pour nettoyer le nom de la ville
def clean_city_name(city_name_raw, cities):
    city_name_raw_lower = city_name_raw.lower()
    for city in cities:
        if city in city_name_raw_lower:
            return city.capitalize()
    return city_name_raw.split(',')[0].split('-')[0].strip().capitalize()

# Collecte des données
for city in cities:
    url = f"https://api.waqi.info/feed/{city}/?token={API_TOKEN}"
    response = requests.get(url)
    data = response.json()

    if data["status"] == "ok":
        d = data["data"]
        city_name_raw = d.get("city", {}).get("name", "")
        city_name = clean_city_name(city_name_raw, cities)
        country = d.get("city", {}).get("country", "France")
        geo = d.get("city", {}).get("geo", [None, None])
        lat, lon = str(geo[0]), str(geo[1])
        aqi = str(d.get("aqi"))
        dominentpol = d.get("dominentpol")
        source_url = d.get("city", {}).get("url")

        time_info = d.get("time", {})
        time_utc = None
        if "iso" in time_info:
            try:
                time_utc = datetime.fromisoformat(time_info["iso"]).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        if not time_utc and "s" in time_info:
            try:
                time_utc = datetime.strptime(time_info["s"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        if not time_utc and "v" in time_info:
            try:
                time_utc = datetime.utcfromtimestamp(time_info["v"]).strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass

        iaqi = d.get("iaqi", {})
        iaqi_pm25 = get_iaqi_value(iaqi, "pm25")
        iaqi_pm10 = get_iaqi_value(iaqi, "pm10")
        iaqi_o3 = get_iaqi_value(iaqi, "o3")
        iaqi_no2 = get_iaqi_value(iaqi, "no2")
        iaqi_so2 = get_iaqi_value(iaqi, "so2")
        iaqi_co = get_iaqi_value(iaqi, "co")
        iaqi_t = get_iaqi_value(iaqi, "t")
        iaqi_h = get_iaqi_value(iaqi, "h")
        iaqi_w = get_iaqi_value(iaqi, "w")
        iaqi_p = get_iaqi_value(iaqi, "p")

        attribution = d.get("attributions", [])
        attribution_names = "; ".join(a.get("name", "") for a in attribution)
        attribution_urls = "; ".join(a.get("url", "") for a in attribution)

        data_quality_inserted = False
        data_forecast_inserted = False
        forecast_already_exists = True  # Par défaut on suppose que tout existe

        # Vérifier si la donnée pour air_quality existe déjà
        cursor.execute("""
            SELECT 1
            FROM raw_air_quality
            WHERE city = %s AND time_utc = %s
        """, (city_name, time_utc))
        exists_quality = cursor.fetchone()

        if not exists_quality:
            cursor.execute("""
                INSERT INTO raw_air_quality (
                    city, country, lat, lon, aqi, dominentpol, time_utc,
                    iaqi_pm25, iaqi_pm10, iaqi_o3, iaqi_no2, iaqi_so2,
                    iaqi_co, iaqi_t, iaqi_h, iaqi_w, iaqi_p,
                    source_url, attribution_names, attribution_urls
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                city_name, country, lat, lon, aqi, dominentpol, time_utc,
                iaqi_pm25, iaqi_pm10, iaqi_o3, iaqi_no2, iaqi_so2,
                iaqi_co, iaqi_t, iaqi_h, iaqi_w, iaqi_p,
                source_url, attribution_names, attribution_urls
            ))
            data_quality_inserted = True

        forecast = d.get("forecast", {}).get("daily", {})
        nb_days = len(forecast.get("pm25", []))

        for i in range(nb_days):
            day = forecast.get("pm25", [])[i].get("day")
            cursor.execute("""
                SELECT 1
                FROM raw_air_forecast
                WHERE city = %s AND day = %s
            """, (city_name, day))
            exists_forecast = cursor.fetchone()

            if not exists_forecast:
                cursor.execute("""
                    INSERT INTO raw_air_forecast (
                        city, day,
                        pm25_avg, pm25_min, pm25_max,
                        pm10_avg, pm10_min, pm10_max,
                        o3_avg,   o3_min,   o3_max,
                        uvi_avg,  uvi_min,  uvi_max
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    city_name, day,
                    get_forecast_value(forecast, i, "pm25", "avg"),
                    get_forecast_value(forecast, i, "pm25", "min"),
                    get_forecast_value(forecast, i, "pm25", "max"),

                    get_forecast_value(forecast, i, "pm10", "avg"),
                    get_forecast_value(forecast, i, "pm10", "min"),
                    get_forecast_value(forecast, i, "pm10", "max"),

                    get_forecast_value(forecast, i, "o3", "avg"),
                    get_forecast_value(forecast, i, "o3", "min"),
                    get_forecast_value(forecast, i, "o3", "max"),

                    get_forecast_value(forecast, i, "uvi", "avg"),
                    get_forecast_value(forecast, i, "uvi", "min"),
                    get_forecast_value(forecast, i, "uvi", "max")
                ))
                data_forecast_inserted = True
                forecast_already_exists = False

        # Affichage final pour la ville
        if data_quality_inserted or data_forecast_inserted:
            print(f"[✅] Données insérées pour {city_name}")
        elif exists_quality and forecast_already_exists:
            print(f"[⚠️] Données déjà existantes pour {city_name}")

    else:
        print(f"[❌] API erreur pour {city}")

# Finalisation
conn.commit()
cursor.close()
conn.close()
