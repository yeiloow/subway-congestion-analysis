import sqlite3
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from statsmodels.tsa.stattools import adfuller
import os
import sys

# 프로젝트 루트 경로 추가
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(project_root)

# DB 파일 경로
DB_PATH = os.path.join(project_root, "db", "subway.db")


def load_top_congested_data(limit=5):
    """
    혼잡도가 가장 높은 상위 N개의 (역, 시간대, 상하행) 조합에 대한 시계열 데이터를 로드합니다.
    """
    conn = sqlite3.connect(DB_PATH)

    # 1. 상위 N개 조합 찾기 (2, 4, 5호선 대상)
    top_query = f"""
    SELECT 
        sc.station_number,
        sc.time_slot,
        sc.is_upline,
        AVG(sc.congestion_level) as avg_cong
    FROM Station_Congestion sc
    JOIN Station_Routes sr ON sc.station_number = sr.station_number
    JOIN Lines l ON sr.line_id = l.line_id
    WHERE sc.is_weekend = 0 -- 평일 기준
      AND l.line_name IN ('2호선', '4호선', '5호선')
    GROUP BY sc.station_number, sc.time_slot, sc.is_upline
    ORDER BY avg_cong DESC
    LIMIT {limit}
    """
    top_df = pd.read_sql(top_query, conn)

    results = []

    # 2. 각 조합별 시계열 데이터 조회
    for _, row in top_df.iterrows():
        s_num = row["station_number"]
        t_slot = row["time_slot"]
        upline = row["is_upline"]

        # 역 이름 등 상세 정보 조회
        info_query = """
        SELECT 
            st.station_name_kr,
            l.line_name
        FROM Station_Routes sr
        JOIN Stations st ON sr.station_id = st.station_id
        JOIN Lines l ON sr.line_id = l.line_id
        WHERE sr.station_number = ?
        """
        cursor = conn.cursor()
        cursor.execute(info_query, (s_num,))
        info = cursor.fetchone()
        station_name = info[0] if info else "Unknown"
        line_name = info[1] if info else "Unknown"

        ts_query = """
        SELECT 
            quarter_code,
            congestion_level
        FROM Station_Congestion
        WHERE station_number = ? 
          AND time_slot = ? 
          AND is_upline = ?
          AND is_weekend = 0
        ORDER BY quarter_code
        """
        ts_df = pd.read_sql(ts_query, conn, params=(s_num, t_slot, upline))

        direction = "상행" if upline else "하행"
        label = f"{line_name} {station_name} ({t_slot}시 {direction})"
        results.append({"label": label, "data": ts_df, "avg_cong": row["avg_cong"]})

    conn.close()
    return results


def perform_adf_test(series, name):
    """ADF 테스트를 수행하고 결과를 반환합니다."""
    if len(series) < 3:
        return {"error": "Not enough data"}

    try:
        # maxlag=0 enforced for very short series
        result = adfuller(series, autolag=None, maxlag=0)
        return {
            "statistic": result[0],
            "p-value": result[1],
            "is_stationary": result[1] < 0.05,
        }
    except Exception as e:
        return {"error": str(e)}


def print_test_result(name, result, prefix=""):
    """테스트 결과를 출력합니다."""
    if "error" in result:
        print(f"   {prefix}[{name}] -> 테스트 불가 ({result['error']})")
    else:
        status = (
            "정상 (Stationary)"
            if result["is_stationary"]
            else "비정상 (Non-stationary)"
        )
        print(f"   {prefix}[{name}]")
        print(
            f"     - ADF Stat: {result['statistic']:.4f}, p-value: {result['p-value']:.4f}"
        )
        print(f"     - 결과: {status}")


def visualize_comparison(data_list):
    """원본 데이터와 차분 데이터를 비교 시각화합니다."""
    # 상위 1개 케이스만 대표로 시각화하거나, subplot으로 그림
    # 여기서는 가독성을 위해 상위 3개만 Subplot 3x2 로 시각화

    target_count = min(len(data_list), 3)
    fig = make_subplots(
        rows=target_count,
        cols=2,
        subplot_titles=[
            f"Original" if i % 2 == 0 else f"Differenced"
            for i in range(2 * target_count)
        ],
        horizontal_spacing=0.1,
        vertical_spacing=0.15,
    )

    for i in range(target_count):
        item = data_list[i]
        df = item["data"]
        diff_series = df["congestion_level"].diff().dropna()
        label = item["label"]

        # Subplot Titles update manual hack (Plotly subplot titles are static list)
        # Instead, try adding title annotation or just rely on row grouping.

        # Original
        fig.add_trace(
            go.Scatter(
                x=df["quarter_code"],
                y=df["congestion_level"],
                mode="lines+markers",
                name=f"{label} (Org)",
                marker=dict(size=8, color="blue"),
                showlegend=True,
            ),
            row=i + 1,
            col=1,
        )

        # Differenced
        # x축은 차분으로 인해 1개 줄어듦 - quarter 코드도 1부터 슬라이싱
        fig.add_trace(
            go.Scatter(
                x=df["quarter_code"][1:],
                y=diff_series,
                mode="lines+markers",
                name=f"{label} (Diff)",
                marker=dict(size=8, color="red"),
                showlegend=True,
            ),
            row=i + 1,
            col=2,
        )

    fig.update_layout(
        title="<b>원본 시계열 vs 차분(Differenced) 시계열 비교</b> (상위 3개 혼잡 구간, 2/4/5호선)",
        height=300 * target_count,
        template="plotly_white",
        font=dict(family="Malgun Gothic", size=12),
    )

    return fig


def main():
    print("1. 데이터 로드 로드 (Top 5)...")
    try:
        data_list = load_top_congested_data(limit=5)
    except Exception as e:
        print(f"   [Error] {e}")
        return

    print("\n2. Stationarity 분석 (Original vs Differenced)")
    print("   * 표본이 매우 적으므로(5개) 해석에 주의 필요 *")

    for item in data_list:
        series = item["data"]["congestion_level"]
        label = item["label"]

        print(f"\n   == {label} ==")

        # 1. Original
        res_org = perform_adf_test(series, "Original")
        print_test_result("Original", res_org, prefix="")

        # 2. Differenced
        diff_series = series.diff().dropna()
        res_diff = perform_adf_test(diff_series, "Differenced")
        print_test_result("Differenced 1st Order", res_diff, prefix="")

    print("\n3. 비교 시각화 생성 중 (상위 3개)...")
    fig = visualize_comparison(data_list)
    fig.show()


if __name__ == "__main__":
    main()
