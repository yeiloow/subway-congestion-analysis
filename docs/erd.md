# Database ERD

## Main Database (schema.sql)

```mermaid
erDiagram
    %% ===== 역/노선 관련 테이블 =====
    역_정보 {
        INTEGER station_id PK
        TEXT station_name_kr
    }

    노선_정보 {
        INTEGER line_id PK
        TEXT line_name UK
        TEXT operator
        TEXT color_hex
    }

    역_노선_매핑 {
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

    역_혼잡도 {
        INTEGER congestion_id PK
        TEXT quarter_code
        TEXT station_code FK
        INTEGER day_of_week "0:평일 1:토 2:일"
        INTEGER is_upline
        INTEGER time_slot
        REAL congestion_level
    }

    역세권_건물_정보 {
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
    행정동_매핑 {
        TEXT admin_dong_code PK
        TEXT admin_dong_name
    }

    행정동_직장인구 {
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

    행정동_유동인구 {
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

    행정동_추정매출 {
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

    행정동_생활인구 {
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
    지하철_시간표 {
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

    일별_기온 {
        INTEGER id PK
        TEXT base_date UK "YYYYMMDD"
        REAL min_temp
        REAL max_temp
    }

    시간별_날씨 {
        INTEGER id PK
        TEXT base_date "YYYYMMDD"
        INTEGER hour
        REAL temperature
        REAL rain_prob
        REAL rain_type
    }

    %% 날짜 기반 논리적 연결
    일별_기온 ||--o{ 시간별_날씨 : "base_date"

    %% ===== 관계 정의 =====
    역_정보 ||--o{ 역_노선_매핑 : "has"
    노선_정보 ||--o{ 역_노선_매핑 : "contains"
    역_노선_매핑 ||--o{ 역_혼잡도 : "station_code"
    역_정보 ||--o{ 역세권_건물_정보 : "station_id"

    %% Subway_Timetable 연결
    역_노선_매핑 ||--o{ 지하철_시간표 : "station_code"
    노선_정보 ||--o{ 지하철_시간표 : "line_id"

    %% Admin_Dong_Mapping 연결
    행정동_매핑 ||--o{ 역_노선_매핑 : "admin_dong_code"
    행정동_매핑 ||--o{ 행정동_직장인구 : "admin_dong_code"
    행정동_매핑 ||--o{ 행정동_유동인구 : "admin_dong_code"
    행정동_매핑 ||--o{ 행정동_추정매출 : "admin_dong_code"
    행정동_매핑 ||--o{ 행정동_생활인구 : "admin_dong_code"

    %% Dong 테이블 간 논리적 연결
    행정동_직장인구 }o..o{ 행정동_유동인구 : "admin_dong_code"
    행정동_유동인구 }o..o{ 행정동_추정매출 : "admin_dong_code"
    행정동_추정매출 }o..o{ 행정동_생활인구 : "admin_dong_code"
```

## Weather Database (weather_schema.sql)

```mermaid

```

## 관계 설명

### Main Database

| 관계 | 설명 |
|------|------|
| 역_정보 → 역_노선_매핑 | 하나의 역이 여러 노선에 존재 가능 (1:N) |
| 노선_정보 → 역_노선_매핑 | 하나의 노선에 여러 역 존재 (1:N) |
| 역_노선_매핑 → 역_혼잡도 | 노선별 역에 시간대/요일별 혼잡도 데이터 (1:N) |
| 역_정보 → 역세권_건물_정보 | 역별 역세권 건물 정보 (1:N) |
| 역_노선_매핑 → 지하철_시간표 | `station_code`로 역별 열차 시간표 연결 (1:N) |
| 노선_정보 → 지하철_시간표 | `line_id`로 호선별 열차 시간표 연결 (1:N) |
| 행정동_매핑 → 행정동_* | 행정동 코드 마스터 테이블 (1:N) |
| 행정동_매핑 → 역_노선_매핑 | 역의 행정동 정보 연결 (1:N) |

### Weather Database

| 관계 | 설명 |
|------|------|
| 일별_기온 → 시간별_날씨 | 일별 기온과 시간대별 기상정보 (1:N, `base_date` 기준) |

### Cross-Database 연결

| Main DB | Weather DB | 연결 키 |
|---------|------------|---------|
| 행정동_생활인구 | 일별_기온 | `base_date` |
| 행정동_생활인구 | 시간별_날씨 | `base_date` + `time_slot` |
| Impact_Analysis_OptionA | 일별_기온 | `base_date` |
