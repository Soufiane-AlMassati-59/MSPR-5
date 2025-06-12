BEGIN;

-- Dimension Location
CREATE TABLE DIM_LOCATION (
    location_id     SERIAL PRIMARY KEY,
    city            TEXT    NOT NULL,
    country         TEXT    NOT NULL,
    lat             NUMERIC(9,6),
    lon             NUMERIC(9,6),
    source_url      TEXT
);

-- Dimension Time
CREATE TABLE DIM_TIME (
    time_id         SERIAL PRIMARY KEY,
    timestamp_utc   TIMESTAMP WITH TIME ZONE NOT NULL,
    hour            SMALLINT NOT NULL,
    minute          SMALLINT NOT NULL,
    second          SMALLINT NOT NULL
);

-- Dimension Date
CREATE TABLE DIM_DATE (
    date_id         SERIAL PRIMARY KEY,
    date_value      DATE    NOT NULL,
    day             SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    year            SMALLINT NOT NULL,
    weekday         TEXT    NOT NULL
);

-- Dimension Pollutant
CREATE TABLE DIM_POLLUTANT (
    pollutant_id    SERIAL PRIMARY KEY,
    code            TEXT    NOT NULL,
    libelle         TEXT    NOT NULL
);

-- Dimension Attribution
CREATE TABLE DIM_ATTRIBUTION (
    attribution_id  SERIAL PRIMARY KEY,
    name            TEXT    NOT NULL,
    url             TEXT
);

-- Dimension Weather Condition
CREATE TABLE DIM_WEATHER_CONDITION (
    condition_id        SERIAL PRIMARY KEY,
    weather_main        TEXT    NOT NULL,
    weather_description TEXT,
    weather_icon        TEXT
);

-- Table de faits Air Quality
CREATE TABLE FACT_AIR_QUALITY (
    measurement_id  SERIAL PRIMARY KEY,
    location_id     INTEGER NOT NULL REFERENCES DIM_LOCATION(location_id),
    time_id         INTEGER NOT NULL REFERENCES DIM_TIME(time_id),
    pollutant_id    INTEGER NOT NULL REFERENCES DIM_POLLUTANT(pollutant_id),
    attribution_id  INTEGER NOT NULL REFERENCES DIM_ATTRIBUTION(attribution_id),
    aqi             INTEGER,
    iaqi_pm25       NUMERIC,
    iaqi_pm10       NUMERIC,
    iaqi_o3         NUMERIC,
    iaqi_no2        NUMERIC,
    iaqi_so2        NUMERIC,
    iaqi_co         NUMERIC,
    iaqi_t          NUMERIC,
    iaqi_h          NUMERIC,
    iaqi_w          NUMERIC,
    iaqi_p          NUMERIC
);

-- Table de faits Air Forecast
CREATE TABLE FACT_AIR_FORECAST (
    forecast_id     SERIAL PRIMARY KEY,
    location_id     INTEGER NOT NULL REFERENCES DIM_LOCATION(location_id),
    date_id         INTEGER NOT NULL REFERENCES DIM_DATE(date_id),
    pm25_avg        NUMERIC,
    pm25_min        NUMERIC,
    pm25_max        NUMERIC,
    pm10_avg        NUMERIC,
    pm10_min        NUMERIC,
    pm10_max        NUMERIC,
    o3_avg          NUMERIC,
    o3_min          NUMERIC,
    o3_max          NUMERIC,
    uvi_avg         NUMERIC,
    uvi_min         NUMERIC,
    uvi_max         NUMERIC
);

-- Table de faits Weather
CREATE TABLE FACT_WEATHER (
    weather_id      SERIAL PRIMARY KEY,
    location_id     INTEGER NOT NULL REFERENCES DIM_LOCATION(location_id),
    time_id         INTEGER NOT NULL REFERENCES DIM_TIME(time_id),
    condition_id    INTEGER NOT NULL REFERENCES DIM_WEATHER_CONDITION(condition_id),
    temp            NUMERIC,
    feels_like      NUMERIC,
    temp_min        NUMERIC,
    temp_max        NUMERIC,
    pressure        NUMERIC,
    humidity        NUMERIC,
    sea_level       NUMERIC,
    ground_level    NUMERIC,
    visibility      NUMERIC,
    wind_speed      NUMERIC,
    wind_deg        NUMERIC,
    sunrise         TIME,
    sunset          TIME
);

COMMIT;
