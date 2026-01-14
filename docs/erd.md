# Database ERD

## Main Database (schema.sql)

```mermaid
erDiagram
    %% ===== 역/노선 관련 테이블 =====
    Stations {
        INTEGER station_id PK
        TEXT station_name_kr
    }

    Lines {
        INTEGER line_id PK
        TEXT line_name UK
        TEXT operator
        TEXT color_hex
    }

    Station_Routes {
        INTEGER route_id PK
        INTEGER station_id FK
        INTEGER line_id FK
        TEXT station_code UK
        TEXT road_address
        TEXT admin_dong_code
        TEXT admin_dong_name
        REAL lat
        REAL lon
    }

    Station_Congestion {
        INTEGER congestion_id PK
        TEXT quarter_code
        TEXT station_code FK
        INTEGER day_of_week "0:평일 1:토 2:일"
        INTEGER is_upline
        INTEGER time_slot
        REAL congestion_level
    }

    Station_Catchment_Buildings {
        INTEGER id PK
        INTEGER station_id FK
        TEXT building_name
        TEXT building_detail_name
        TEXT usage_type
        TEXT structure_type
        TEXT approval_date
        REAL height
        REAL floor_area
        INTEGER households
        INTEGER families
    }

    %% ===== 행정동 관련 테이블 =====
    Admin_Dong_Mapping {
        TEXT admin_dong_code PK
        TEXT admin_dong_name
    }

    Dong_Workplace_Population {
        INTEGER id PK
        TEXT quarter_code
        TEXT admin_dong_code
        TEXT admin_dong_name
        INTEGER total_pop
        INTEGER male_pop
        INTEGER female_pop
        INTEGER age_10_pop
        INTEGER age_20_pop
        INTEGER age_30_pop
        INTEGER age_40_pop
        INTEGER age_50_pop
        INTEGER age_60_over_pop
    }

    Dong_Floating_Population {
        INTEGER id PK
        TEXT quarter_code
        TEXT admin_dong_code
        TEXT admin_dong_name
        INTEGER total_floating_pop
        INTEGER male_floating_pop
        INTEGER female_floating_pop
        INTEGER time_00_06_floating_pop
        INTEGER time_06_11_floating_pop
        INTEGER time_11_14_floating_pop
        INTEGER time_14_17_floating_pop
        INTEGER time_17_21_floating_pop
        INTEGER time_21_24_floating_pop
    }

    Dong_Estimated_Revenue {
        INTEGER id PK
        TEXT quarter_code
        TEXT admin_dong_code
        TEXT admin_dong_name
        TEXT service_type_code
        TEXT service_type_name
        REAL month_sales_amt
        INTEGER month_sales_cnt
        REAL weekday_sales_amt
        REAL weekend_sales_amt
    }

    Dong_Living_Population {
        INTEGER id PK
        TEXT base_date "YYYYMMDD"
        TEXT time_slot "HH"
        TEXT admin_dong_code
        REAL local_total_living_pop
        REAL long_term_chinese_stay_pop
        REAL long_term_non_chinese_stay_pop
        REAL short_term_chinese_stay_pop
        REAL short_term_non_chinese_stay_pop
    }

    %% ===== 열차/분석 테이블 =====
    Subway_Timetable {
        INTEGER id PK
        INTEGER source_id
        INTEGER line_id FK
        TEXT station_code FK
        TEXT station_name
        TEXT day_type "DAY SAT END"
        TEXT direction "UP DOWN IN OUT"
        INTEGER is_express
        TEXT train_number
        TEXT arrival_time
        TEXT departure_time
        TEXT origin_station
        TEXT destination_station
    }

    Impact_Analysis_OptionA {
        INTEGER id PK
        TEXT base_date "YYYY-MM-DD"
        TEXT line_name
        TEXT station_name
        TEXT station_name_normalized
        TEXT day_of_week
        TEXT category
        INTEGER boarding_count
        INTEGER alighting_count
        INTEGER total_count
        REAL increase_rate
        TEXT increase_status
    }

    %% ===== 관계 정의 =====
    Stations ||--o{ Station_Routes : "has"
    Lines ||--o{ Station_Routes : "contains"
    Station_Routes ||--o{ Station_Congestion : "station_code"
    Stations ||--o{ Station_Catchment_Buildings : "station_id"

    %% Subway_Timetable 연결
    Station_Routes ||--o{ Subway_Timetable : "station_code"
    Lines ||--o{ Subway_Timetable : "line_id"

    %% Admin_Dong_Mapping 연결
    Admin_Dong_Mapping ||--o{ Station_Routes : "admin_dong_code"
    Admin_Dong_Mapping ||--o{ Dong_Workplace_Population : "admin_dong_code"
    Admin_Dong_Mapping ||--o{ Dong_Floating_Population : "admin_dong_code"
    Admin_Dong_Mapping ||--o{ Dong_Estimated_Revenue : "admin_dong_code"
    Admin_Dong_Mapping ||--o{ Dong_Living_Population : "admin_dong_code"

    %% Dong 테이블 간 논리적 연결
    Dong_Workplace_Population }o..o{ Dong_Floating_Population : "admin_dong_code"
    Dong_Floating_Population }o..o{ Dong_Estimated_Revenue : "admin_dong_code"
    Dong_Estimated_Revenue }o..o{ Dong_Living_Population : "admin_dong_code"
```

## Weather Database (weather_schema.sql)

```mermaid
erDiagram
    Daily_Temperature {
        INTEGER id PK
        TEXT base_date UK "YYYYMMDD"
        REAL min_temp
        REAL max_temp
    }

    Hourly_Weather {
        INTEGER id PK
        TEXT base_date "YYYYMMDD"
        INTEGER hour
        REAL temperature
        REAL rain_prob
        REAL rain_type
    }

    %% 날짜 기반 논리적 연결
    Daily_Temperature ||--o{ Hourly_Weather : "base_date"
```

## 관계 설명

### Main Database

| 관계 | 설명 |
|------|------|
| Stations → Station_Routes | 하나의 역이 여러 노선에 존재 가능 (1:N) |
| Lines → Station_Routes | 하나의 노선에 여러 역 존재 (1:N) |
| Station_Routes → Station_Congestion | 노선별 역에 시간대/요일별 혼잡도 데이터 (1:N) |
| Stations → Station_Catchment_Buildings | 역별 역세권 건물 정보 (1:N) |
| Station_Routes → Subway_Timetable | `station_code`로 역별 열차 시간표 연결 (1:N) |
| Lines → Subway_Timetable | `line_id`로 호선별 열차 시간표 연결 (1:N) |
| Admin_Dong_Mapping → Dong_* | 행정동 코드 마스터 테이블 (1:N) |
| Admin_Dong_Mapping → Station_Routes | 역의 행정동 정보 연결 (1:N) |

### Weather Database

| 관계 | 설명 |
|------|------|
| Daily_Temperature → Hourly_Weather | 일별 기온과 시간대별 기상정보 (1:N, `base_date` 기준) |

### Cross-Database 연결

| Main DB | Weather DB | 연결 키 |
|---------|------------|---------|
| Dong_Living_Population | Daily_Temperature | `base_date` |
| Dong_Living_Population | Hourly_Weather | `base_date` + `time_slot` |
| Impact_Analysis_OptionA | Daily_Temperature | `base_date` |
