# 지하철 혼잡도 분석 프로젝트

## 개요
이 프로젝트는 서울시 지하철 혼잡도와 유동인구, 상주인구, 직장인구, 주변 상권의 추정 매출, 건물 정보 등 다양한 요인 간의 상관관계를 분석합니다.

## 프로젝트 구조
소스 코드는 다음과 같이 구성되어 있습니다:

-   `src/01_acquire`: 데이터 수집, 데이터베이스 초기화 및 데이터 삽입 스크립트.
-   `src/02_process`: 데이터 전처리 및 랭글링 스크립트.
-   `src/03_analyze`: 통계 분석 및 시각화 스크립트.
-   `src/05_visualize`: 시각화 전용 스크립트.
-   `db`: SQLite 데이터베이스 및 스키마 파일.
-   `output`: 생성된 플롯 및 데이터 파일 저장 디렉토리.

## 사전 요구 사항 (Prerequisites)
-   Python 3.x
-   `uv` 패키지 매니저 (권장) 또는 `pip`
    -   `uv` 설치 방법은 [공식 문서](https://github.com/astral-sh/uv)를 참고하세요.
-   서울열린데이터광장 인증키
    -   데이터를 수집하기 위해서는 API 키가 필요합니다. `.env` 파일을 프로젝트 루트에 생성하고 키를 입력하세요.
    ```env
    SEOUL_DATA_OPEN_API_KEY="서울열린데이터광장 인증키"
    ```

## 사용 방법 (Usage)

이 프로젝트는 `uv`를 사용하여 의존성을 관리하고 스크립트를 실행하는 것을 권장합니다.

### 1. 데이터베이스 초기화 (Database Initialization)
SQLite 데이터베이스 스키마를 초기화합니다:
```bash
uv run python src/01_acquire/01_init_db.py
```

### 2. 데이터 삽입 (Data Insertion)
데이터베이스에 데이터를 채웁니다. 아래 스크립트들을 순서대로 실행하세요:
```bash
# 지하철 역 정보 삽입
uv run python src/01_acquire/02_insert_subway.py

# 혼잡도 데이터 삽입
uv run python src/01_acquire/03_insert_congestion.py

# 인구 데이터 삽입
uv run python src/01_acquire/04_insert_floating_population.py
uv run python src/01_acquire/04_insert_living_population.py

# 추정 매출 데이터 삽입
uv run python src/01_acquire/05_insert_estimated_revenue.py
```

### 3. 분석 및 시각화 (Analysis & Visualization)
분석 스크립트를 실행하여 인사이트를 도출하고 플롯을 생성합니다:

-   **혼잡도 및 매출 분석**:
    ```bash
    uv run python src/03_analyze/analyze_revenue_congestion.py
    ```
-   **혼잡도 및 인구 상관관계 분석**:
    ```bash
    uv run python src/03_analyze/analysis_congestion_population_correlation.py
    ```
-   **탐색적 데이터 분석 (EDA)**:
    ```bash
    uv run python src/03_analyze/eda_living_population.py
    uv run python src/03_analyze/eda_revenue.py
    uv run python src/03_analyze/eda_workforce.py
    ```
-   **건물 및 혼잡도 상관관계 분석**:
    ```bash
    uv run python src/03_analyze/analyze_congestion_building_correlation.py
    ```

## Hugging Face 데이터셋 연결

이 프로젝트는 `data` 디렉토리를 Hugging Face Hub의 [alrq/subway](https://huggingface.co/datasets/alrq/subway) 데이터셋과 동기화하여 관리합니다.

### 1. 인증

먼저 Hugging Face 계정으로 로그인해야 합니다:

```bash
huggingface-cli login
```

### 2. 데이터 업로드 (Upload)

로컬의 `data` 폴더 내용을 Hugging Face 저장소에 업로드합니다:

```bash
huggingface-cli upload alrq/subway data . --repo-type dataset
```

### 3. 데이터 다운로드 (Download)

Hugging Face 저장소의 데이터를 로컬 `data` 폴더로 다운로드합니다:

```bash
huggingface-cli download alrq/subway --local-dir data --repo-type dataset
```

## 데이터 출처 (Data Sources)
-   **서울 열린데이터 광장**:
    -   서울시 생활인구 (행정동)
    -   서울시 유동인구 (행정동)
    -   서울시 추정매출 (행정동)
    -   서울시 지하철 혼잡도 통계
-   **V-World GIS**: 건물 공간 데이터.
-   **국토교통부**: 건축물대장 기반 건물 속성 정보.
