-- weather_schema.sql

-- 10. 일별 기온 (최저/최고)
CREATE TABLE IF NOT EXISTS Daily_Temperature (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    base_date TEXT NOT NULL, -- YYYYMMDD
    min_temp REAL,
    max_temp REAL,
    UNIQUE(base_date)
);

-- 11. 시간대별 기상정보
CREATE TABLE IF NOT EXISTS Hourly_Weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    base_date TEXT NOT NULL, -- YYYYMMDD
    hour INTEGER NOT NULL,
    temperature REAL,
    rain_prob REAL,
    rain_type REAL,
    UNIQUE(base_date, hour)
);
