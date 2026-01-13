import sqlite3
import pandas as pd
import plotly.graph_objects as go
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

    # 1. 상위 N개 조합 찾기
    top_query = f"""
    SELECT 
        station_number,
        time_slot,
        is_upline,
        AVG(congestion_level) as avg_cong
    FROM Station_Congestion
    WHERE is_weekend = 0 -- 평일 기준
    GROUP BY station_number, time_slot, is_upline
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

        # 시계열 데이터 조회 (모든 요일 포함? 아니면 평일만? -> 보통 혼잡도 분석은 평일 위주이므로 평일(day_of_week=0)만 필터)
        # 쿼터별 평균으로 갈지, 특정 요일 데이터인지 확인 필요.
        # Station_Congestion은 요일별로 구분되어 있음.
        # 따라서 day_of_week=0 (평일) 인 데이터만 가져와서 시계열 구성.
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
    """ADF 테스트를 수행하고 결과를 출력합니다."""
    print(f"\n   [{name}]")
    if len(series) < 3:
        print("     -> 데이터 부족으로 테스트 불가")
        return

    try:
        # maxlag=0 enforced for very short series
        # autolag=None을 해야 maxlag가 적용됨
        result = adfuller(series, autolag=None, maxlag=0)

        p_value = result[1]
        is_stationary = p_value < 0.05

        print(f"     - ADF Statistic: {result[0]:.4f}")
        print(f"     - p-value: {p_value:.4f}")
        print(
            f"     - 결과: {'정상 시계열 (Stationary)' if is_stationary else '비정상 시계열 (Non-stationary)'}"
        )

    except Exception as e:
        print(f"     -> 테스트 오류: {str(e)}")


def plot_top_trends(data_list):
    """상위 혼잡 케이스의 시계열 추세를 시각화합니다."""
    fig = go.Figure()

    for item in data_list:
        df = item["data"]
        label = item["label"]

        fig.add_trace(
            go.Scatter(
                x=df["quarter_code"],
                y=df["congestion_level"],
                mode="lines+markers",
                name=label,
                marker=dict(size=8),
                line=dict(width=2),
            )
        )

    fig.update_layout(
        title="상위 5개 혼잡 구간(평일)의 분기별 혼잡도 추이 (Stationarity Check)",
        xaxis_title="분기",
        yaxis_title="혼잡도 (%)",
        template="plotly_white",
        font=dict(family="Malgun Gothic", size=12),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=1.05),
    )

    return fig


def main():
    print("1. 상위 5개 혼잡 데이터 로드 중...")
    try:
        data_list = load_top_congested_data(limit=5)
    except Exception as e:
        print(f"   [Error] 데이터 로드 실패: {e}")
        return

    print(f"   - {len(data_list)}개 케이스 로드 완료")

    print("\n2. ADF(Augmented Dickey-Fuller) 테스트 수행 (개별)")
    print("   * 주의: 데이터 포인트가 적어(최대 5개) 통계적 신뢰도가 낮습니다.")

    for item in data_list:
        if not item["data"].empty:
            perform_adf_test(item["data"]["congestion_level"], item["label"])
        else:
            print(f"\n   [{item['label']}] -> 데이터 없음")

    print("\n3. 시각화 생성 중...")
    fig = plot_top_trends(data_list)
    fig.show()


if __name__ == "__main__":
    main()
