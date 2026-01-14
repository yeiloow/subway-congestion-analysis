# -*- coding: utf-8 -*-
"""
Q1. 출퇴근 시간대 혼잡도는 다른 시간대와 얼마나 차이가 나는가?

분석 내용:
- 출근 시간대 (07:00~09:00)와 퇴근 시간대 (18:00~20:00) 혼잡도
- 기타 시간대 혼잡도와의 비교
- 통계적 차이 분석
"""

import pandas as pd
import numpy as np
import glob
import os
from scipy import stats

# 결과 저장 디렉토리
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# 시간대 정의
TIME_COLUMNS = {
    '5시30분': '5시30분', '6시00분': '6시00분', '6시30분': '6시30분',
    '7시00분': '7시00분', '7시30분': '7시30분', '8시00분': '8시00분', '8시30분': '8시30분',
    '9시00분': '9시00분', '9시30분': '9시30분', '10시00분': '10시00분', '10시30분': '10시30분',
    '11시00분': '11시00분', '11시30분': '11시30분', '12시00분': '12시00분', '12시30분': '12시30분',
    '13시00분': '13시00분', '13시30분': '13시30분', '14시00분': '14시00분', '14시30분': '14시30분',
    '15시00분': '15시00분', '15시30분': '15시30분', '16시00분': '16시00분', '16시30분': '16시30분',
    '17시00분': '17시00분', '17시30분': '17시30분', '18시00분': '18시00분', '18시30분': '18시30분',
    '19시00분': '19시00분', '19시30분': '19시30분', '20시00분': '20시00분', '20시30분': '20시30분',
    '21시00분': '21시00분', '21시30분': '21시30분', '22시00분': '22시00분', '22시30분': '22시30분',
    '23시00분': '23시00분', '23시30분': '23시30분', '0시00분': '0시00분', '0시30분': '0시30분'
}

# 시간대 분류
MORNING_COMMUTE = ['7시00분', '7시30분', '8시00분', '8시30분']  # 출근 시간대
EVENING_COMMUTE = ['18시00분', '18시30분', '19시00분', '19시30분']  # 퇴근 시간대
COMMUTE_TIMES = MORNING_COMMUTE + EVENING_COMMUTE


def load_congestion_data(data_dir):
    """모든 혼잡도 CSV 파일 로드"""
    pattern = os.path.join(data_dir, 'data/01_raw/02_congestion/서울교통공사_지하철혼잡도정보_*.csv')
    files = glob.glob(pattern)

    all_data = []
    for f in files:
        try:
            df = pd.read_csv(f, encoding='utf-8')
            all_data.append(df)
        except Exception as e:
            print(f'파일 로드 실패: {f} - {e}')

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return None


def get_time_columns(df):
    """시간대 컬럼 추출"""
    time_cols = [col for col in df.columns if '시' in col and '분' in col]
    return time_cols


def classify_time_period(col_name):
    """시간대 분류"""
    if col_name in MORNING_COMMUTE:
        return '출근시간대'
    elif col_name in EVENING_COMMUTE:
        return '퇴근시간대'
    else:
        return '기타시간대'


def analyze_congestion(df):
    """출퇴근 시간대 vs 기타 시간대 혼잡도 분석"""
    time_cols = get_time_columns(df)

    results = {
        '시간대': [],
        '평균_혼잡도': [],
        '표준편차': [],
        '최소값': [],
        '최대값': [],
        '중앙값': []
    }

    # 각 시간대별 통계
    for col in time_cols:
        period = classify_time_period(col)
        values = df[col].dropna()

        results['시간대'].append(col)
        results['평균_혼잡도'].append(values.mean())
        results['표준편차'].append(values.std())
        results['최소값'].append(values.min())
        results['최대값'].append(values.max())
        results['중앙값'].append(values.median())

    return pd.DataFrame(results)


def analyze_by_period(df):
    """시간대 그룹별 분석"""
    time_cols = get_time_columns(df)

    morning_cols = [c for c in time_cols if c in MORNING_COMMUTE]
    evening_cols = [c for c in time_cols if c in EVENING_COMMUTE]
    other_cols = [c for c in time_cols if c not in COMMUTE_TIMES]

    # 각 그룹의 평균 혼잡도 계산
    morning_values = df[morning_cols].values.flatten()
    evening_values = df[evening_cols].values.flatten()
    other_values = df[other_cols].values.flatten()

    # NaN 제거
    morning_values = morning_values[~np.isnan(morning_values)]
    evening_values = evening_values[~np.isnan(evening_values)]
    other_values = other_values[~np.isnan(other_values)]

    summary = {
        '구분': ['출근시간대 (07:00~09:00)', '퇴근시간대 (18:00~20:00)', '기타시간대'],
        '평균_혼잡도': [morning_values.mean(), evening_values.mean(), other_values.mean()],
        '표준편차': [morning_values.std(), evening_values.std(), other_values.std()],
        '최소값': [morning_values.min(), evening_values.min(), other_values.min()],
        '최대값': [morning_values.max(), evening_values.max(), other_values.max()],
        '중앙값': [np.median(morning_values), np.median(evening_values), np.median(other_values)],
        '데이터수': [len(morning_values), len(evening_values), len(other_values)]
    }

    return pd.DataFrame(summary), morning_values, evening_values, other_values


def calculate_difference(df_summary):
    """혼잡도 차이 계산"""
    morning_avg = df_summary[df_summary['구분'].str.contains('출근')]['평균_혼잡도'].values[0]
    evening_avg = df_summary[df_summary['구분'].str.contains('퇴근')]['평균_혼잡도'].values[0]
    other_avg = df_summary[df_summary['구분'].str.contains('기타')]['평균_혼잡도'].values[0]

    diff_results = {
        '비교항목': [
            '출근시간대 - 기타시간대',
            '퇴근시간대 - 기타시간대',
            '출근시간대 - 퇴근시간대'
        ],
        '혼잡도_차이': [
            morning_avg - other_avg,
            evening_avg - other_avg,
            morning_avg - evening_avg
        ],
        '차이_비율(%)': [
            ((morning_avg - other_avg) / other_avg) * 100,
            ((evening_avg - other_avg) / other_avg) * 100,
            ((morning_avg - evening_avg) / evening_avg) * 100 if evening_avg != 0 else 0
        ]
    }

    return pd.DataFrame(diff_results)


def statistical_test(morning, evening, other):
    """통계적 유의성 검정"""
    # t-검정: 출근 vs 기타
    t_morning_other, p_morning_other = stats.ttest_ind(morning, other)

    # t-검정: 퇴근 vs 기타
    t_evening_other, p_evening_other = stats.ttest_ind(evening, other)

    # t-검정: 출근 vs 퇴근
    t_morning_evening, p_morning_evening = stats.ttest_ind(morning, evening)

    test_results = {
        '비교항목': [
            '출근시간대 vs 기타시간대',
            '퇴근시간대 vs 기타시간대',
            '출근시간대 vs 퇴근시간대'
        ],
        't-통계량': [t_morning_other, t_evening_other, t_morning_evening],
        'p-값': [p_morning_other, p_evening_other, p_morning_evening],
        '유의수준_0.05': [
            '유의함' if p_morning_other < 0.05 else '유의하지 않음',
            '유의함' if p_evening_other < 0.05 else '유의하지 않음',
            '유의함' if p_morning_evening < 0.05 else '유의하지 않음'
        ]
    }

    return pd.DataFrame(test_results)


def analyze_by_line(df):
    """호선별 출퇴근 시간대 혼잡도 분석"""
    time_cols = get_time_columns(df)
    morning_cols = [c for c in time_cols if c in MORNING_COMMUTE]
    evening_cols = [c for c in time_cols if c in EVENING_COMMUTE]
    other_cols = [c for c in time_cols if c not in COMMUTE_TIMES]

    line_col = [c for c in df.columns if '호선' in c][0]

    results = []
    for line in df[line_col].unique():
        line_data = df[df[line_col] == line]

        morning_vals = line_data[morning_cols].values.flatten()
        evening_vals = line_data[evening_cols].values.flatten()
        other_vals = line_data[other_cols].values.flatten()

        morning_vals = morning_vals[~np.isnan(morning_vals)]
        evening_vals = evening_vals[~np.isnan(evening_vals)]
        other_vals = other_vals[~np.isnan(other_vals)]

        results.append({
            '호선': line,
            '출근시간_평균': morning_vals.mean() if len(morning_vals) > 0 else np.nan,
            '퇴근시간_평균': evening_vals.mean() if len(evening_vals) > 0 else np.nan,
            '기타시간_평균': other_vals.mean() if len(other_vals) > 0 else np.nan,
            '출근vs기타_차이': (morning_vals.mean() - other_vals.mean()) if len(morning_vals) > 0 and len(other_vals) > 0 else np.nan,
            '퇴근vs기타_차이': (evening_vals.mean() - other_vals.mean()) if len(evening_vals) > 0 and len(other_vals) > 0 else np.nan
        })

    return pd.DataFrame(results).sort_values('호선')


def main():
    # 데이터 로드
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    df = load_congestion_data(base_dir)

    if df is None:
        print("데이터 로드 실패")
        return

    print("=" * 70)
    print("Q1. 출퇴근 시간대 혼잡도는 다른 시간대와 얼마나 차이가 나는가?")
    print("=" * 70)
    print(f"\n총 데이터 수: {len(df):,}개")

    # 1. 시간대별 상세 분석
    df_hourly = analyze_congestion(df)

    # 2. 시간대 그룹별 분석
    df_summary, morning, evening, other = analyze_by_period(df)

    # 3. 혼잡도 차이 계산
    df_diff = calculate_difference(df_summary)

    # 4. 통계 검정
    df_test = statistical_test(morning, evening, other)

    # 5. 호선별 분석
    df_line = analyze_by_line(df)

    # 결과 출력
    print("\n" + "=" * 70)
    print("[1] 시간대 그룹별 혼잡도 요약")
    print("=" * 70)
    print(df_summary.to_string(index=False))

    print("\n" + "=" * 70)
    print("[2] 혼잡도 차이 분석")
    print("=" * 70)
    print(df_diff.to_string(index=False))

    print("\n" + "=" * 70)
    print("[3] 통계적 유의성 검정 (t-test)")
    print("=" * 70)
    print(df_test.to_string(index=False))

    print("\n" + "=" * 70)
    print("[4] 호선별 시간대 혼잡도 비교")
    print("=" * 70)
    print(df_line.to_string(index=False))

    # 결과 파일 저장
    output_file = os.path.join(OUTPUT_DIR, 'Q1_출퇴근_혼잡도_분석결과.csv')

    with open(output_file, 'w', encoding='utf-8-sig') as f:
        f.write("Q1. 출퇴근 시간대 혼잡도는 다른 시간대와 얼마나 차이가 나는가?\n")
        f.write("=" * 70 + "\n\n")

        f.write("[1] 시간대 그룹별 혼잡도 요약\n")
        df_summary.to_csv(f, index=False)
        f.write("\n")

        f.write("[2] 혼잡도 차이 분석\n")
        df_diff.to_csv(f, index=False)
        f.write("\n")

        f.write("[3] 통계적 유의성 검정 (t-test)\n")
        df_test.to_csv(f, index=False)
        f.write("\n")

        f.write("[4] 호선별 시간대 혼잡도 비교\n")
        df_line.to_csv(f, index=False)
        f.write("\n")

        f.write("[5] 시간대별 상세 혼잡도\n")
        df_hourly.to_csv(f, index=False)

    print(f"\n결과 파일 저장 완료: {output_file}")

    # 주요 결론 출력
    print("\n" + "=" * 70)
    print("[결론]")
    print("=" * 70)

    morning_avg = df_summary[df_summary['구분'].str.contains('출근')]['평균_혼잡도'].values[0]
    evening_avg = df_summary[df_summary['구분'].str.contains('퇴근')]['평균_혼잡도'].values[0]
    other_avg = df_summary[df_summary['구분'].str.contains('기타')]['평균_혼잡도'].values[0]

    print(f"1. 출근 시간대(07:00~09:00) 평균 혼잡도: {morning_avg:.1f}%")
    print(f"2. 퇴근 시간대(18:00~20:00) 평균 혼잡도: {evening_avg:.1f}%")
    print(f"3. 기타 시간대 평균 혼잡도: {other_avg:.1f}%")
    print(f"\n4. 출근 시간대는 기타 시간대 대비 {((morning_avg - other_avg) / other_avg * 100):.1f}% 더 혼잡")
    print(f"5. 퇴근 시간대는 기타 시간대 대비 {((evening_avg - other_avg) / other_avg * 100):.1f}% 더 혼잡")
    print(f"6. 출근 시간대와 퇴근 시간대의 차이: {abs(morning_avg - evening_avg):.1f}%p")


if __name__ == '__main__':
    main()
