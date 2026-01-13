# -*- coding: utf-8 -*-
"""
분기별 지하철 혼잡도 추세 분석

각 역별로 분기가 증가함에 따라 혼잡도가 증가/감소하는 추세인지 분석합니다.
선형 회귀를 사용하여 통계적으로 유의미한 추세를 가진 역들을 식별합니다.
"""

import sqlite3
import pandas as pd
import numpy as np
from scipy import stats
from pathlib import Path


def get_db_connection():
    """데이터베이스 연결"""
    db_path = Path(__file__).parent.parent.parent / "db" / "subway.db"
    return sqlite3.connect(db_path)


def load_congestion_data(conn):
    """역별 분기별 평균 혼잡도 데이터 로드"""
    query = """
        SELECT sc.station_number, sr.station_id, s.station_name_kr,
               l.line_name,
               sc.quarter_code, AVG(sc.congestion_level) as avg_congestion
        FROM Station_Congestion sc
        JOIN Station_Routes sr ON sc.station_number = sr.station_number
        JOIN Stations s ON sr.station_id = s.station_id
        JOIN Lines l ON sr.line_id = l.line_id
        GROUP BY sc.station_number, sc.quarter_code
        ORDER BY sc.station_number, sc.quarter_code
    """
    return pd.read_sql_query(query, conn)


def analyze_trend(df):
    """각 역별 혼잡도 추세 분석 (선형 회귀)"""
    # 분기 코드를 순서 숫자로 변환
    quarters = sorted(df['quarter_code'].unique())
    quarter_order = {q: i + 1 for i, q in enumerate(quarters)}
    df['quarter_num'] = df['quarter_code'].map(quarter_order)

    results = []
    for station_num in df['station_number'].unique():
        station_data = df[df['station_number'] == station_num].dropna()

        if len(station_data) >= 3:  # 최소 3개 분기 데이터 필요
            x = station_data['quarter_num'].values
            y = station_data['avg_congestion'].values
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

            station_info = station_data.iloc[0]
            first_q = station_data[station_data['quarter_num'] == station_data['quarter_num'].min()]['avg_congestion'].values[0]
            last_q = station_data[station_data['quarter_num'] == station_data['quarter_num'].max()]['avg_congestion'].values[0]

            results.append({
                'station_number': station_num,
                'station_name_kr': station_info['station_name_kr'],
                'line_name': station_info['line_name'],
                'slope': slope,
                'r_squared': r_value ** 2,
                'p_value': p_value,
                'data_points': len(station_data),
                'start_congestion': first_q,
                'end_congestion': last_q,
                'change': last_q - first_q
            })

    return pd.DataFrame(results), quarters


def print_data_overview(conn):
    """데이터 현황 출력"""
    query = """
        SELECT quarter_code,
               COUNT(DISTINCT station_number) as station_count,
               COUNT(*) as record_count,
               ROUND(AVG(congestion_level), 2) as avg_congestion
        FROM Station_Congestion
        GROUP BY quarter_code
        ORDER BY quarter_code
    """
    df = pd.read_sql_query(query, conn)

    print("=" * 70)
    print("                    데이터 현황")
    print("=" * 70)
    print(f"{'분기코드':<12} {'역 수':>10} {'레코드 수':>12} {'평균 혼잡도':>12}")
    print("-" * 70)
    for _, row in df.iterrows():
        print(f"{row['quarter_code']:<12} {row['station_count']:>10} {row['record_count']:>12} {row['avg_congestion']:>12.2f}")
    print()


def print_analysis_results(result_df, quarters):
    """분석 결과 출력"""
    # 분류
    increasing = result_df[(result_df['slope'] > 0) & (result_df['p_value'] < 0.1)].sort_values('slope', ascending=False)
    decreasing = result_df[(result_df['slope'] < 0) & (result_df['p_value'] < 0.1)].sort_values('slope')
    no_trend = result_df[result_df['p_value'] >= 0.1]

    # 분기 정보
    first_quarter = quarters[0]
    last_quarter = quarters[-1]

    print("=" * 70)
    print("            분기별 지하철 혼잡도 추세 분석 결과")
    print("=" * 70)
    print()
    print("[ 분석 개요 ]")
    print(f"  - 분석 기간: {first_quarter[:4]}년 {first_quarter[4]}분기 ~ {last_quarter[:4]}년 {last_quarter[4]}분기 (총 {len(quarters)}개 분기)")
    print(f"  - 분석 대상: {len(result_df)}개 역")
    print(f"  - 분석 방법: 선형 회귀 (시간에 따른 평균 혼잡도 추세)")
    print()

    print("[ 추세 분류 요약 (통계적 유의수준 p<0.1 기준) ]")
    print(f"  - 증가 추세: {len(increasing)}개 ({len(increasing) / len(result_df) * 100:.1f}%)")
    print(f"  - 감소 추세: {len(decreasing)}개 ({len(decreasing) / len(result_df) * 100:.1f}%)")
    print(f"  - 명확한 추세 없음: {len(no_trend)}개 ({len(no_trend) / len(result_df) * 100:.1f}%)")
    print()

    # 증가 추세 역 상세
    print("=" * 70)
    print("        [통계적으로 유의미한 증가 추세 역] (p < 0.1)")
    print("=" * 70)
    if len(increasing) == 0:
        print("  해당 역 없음")
    else:
        for _, row in increasing.iterrows():
            print(f"  {row['station_name_kr']} ({row['line_name']}, {row['station_number']})")
            print(f"    - 기울기: +{row['slope']:.2f} (분기당 혼잡도 증가량)")
            print(f"    - R²: {row['r_squared']:.3f}, p-value: {row['p_value']:.4f}")
            print(f"    - {first_quarter[:4]}Q{first_quarter[4]}: {row['start_congestion']:.1f} -> {last_quarter[:4]}Q{last_quarter[4]}: {row['end_congestion']:.1f} (변화: +{row['change']:.1f})")
            print()

    # 감소 추세 역 상세
    print("=" * 70)
    print("        [통계적으로 유의미한 감소 추세 역] (p < 0.1)")
    print("=" * 70)
    if len(decreasing) == 0:
        print("  해당 역 없음")
    else:
        for _, row in decreasing.iterrows():
            print(f"  {row['station_name_kr']} ({row['line_name']}, {row['station_number']})")
            print(f"    - 기울기: {row['slope']:.2f} (분기당 혼잡도 감소량)")
            print(f"    - R²: {row['r_squared']:.3f}, p-value: {row['p_value']:.4f}")
            print(f"    - {first_quarter[:4]}Q{first_quarter[4]}: {row['start_congestion']:.1f} -> {last_quarter[:4]}Q{last_quarter[4]}: {row['end_congestion']:.1f} (변화: {row['change']:.1f})")
            print()

    # 조건 완화 시 증가 추세 역
    print("=" * 70)
    print("   [조건 완화 시 증가 추세 역] (slope > 0.5, p < 0.2)")
    print("=" * 70)
    increasing_relaxed = result_df[(result_df['slope'] > 0.5) & (result_df['p_value'] < 0.2)].sort_values('slope', ascending=False)
    print(f"총 {len(increasing_relaxed)}개 역")
    print()

    for _, row in increasing_relaxed.iterrows():
        sig = "**" if row['p_value'] < 0.1 else ""
        print(f"  {sig}{row['station_name_kr']} ({row['line_name']}, {row['station_number']})")
        print(f"    기울기: +{row['slope']:.2f}, p={row['p_value']:.3f}, 변화: {row['start_congestion']:.1f} -> {row['end_congestion']:.1f}")

    # 호선별 분포
    print()
    print("=" * 70)
    print("   [호선별 증가 추세 역 분포] (조건 완화 기준)")
    print("=" * 70)
    if len(increasing_relaxed) > 0:
        line_dist = increasing_relaxed.groupby('line_name').agg({
            'station_number': 'count',
            'slope': 'mean'
        }).rename(columns={'station_number': 'count', 'slope': 'avg_slope'}).sort_values('count', ascending=False)

        for line, data in line_dist.iterrows():
            print(f"  {line}: {int(data['count'])}개역 (평균 기울기: +{data['avg_slope']:.2f})")
    else:
        print("  해당 역 없음")

    # 전체 추세 요약
    print()
    print("=" * 70)
    print("   [전체 추세 요약]")
    print("=" * 70)
    avg_slope = result_df['slope'].mean()
    positive_slope_pct = (result_df['slope'] > 0).sum() / len(result_df) * 100
    print(f"  - 전체 평균 기울기: {avg_slope:+.3f}")
    print(f"  - 증가 추세(slope>0) 역 비율: {positive_slope_pct:.1f}%")
    print(f"  - 감소 추세(slope<0) 역 비율: {100 - positive_slope_pct:.1f}%")


def main():
    """메인 실행 함수"""
    conn = get_db_connection()

    try:
        # 데이터 현황 출력
        print_data_overview(conn)

        # 데이터 로드
        df = load_congestion_data(conn)

        # 추세 분석
        result_df, quarters = analyze_trend(df)

        # 결과 출력
        print_analysis_results(result_df, quarters)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
