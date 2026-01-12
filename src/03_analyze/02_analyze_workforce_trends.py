import pandas as pd
import plotly.express as px
import os


def analyze_population_trends(input_path):
    print(f"Loading data from {input_path}...")
    df = pd.read_csv(input_path)

    # 1. Preprocessing
    # Extract Year and Quarter from '기준_년분기_코드' (e.g., 20231 -> 2023, 1)
    df["Year"] = df["기준_년분기_코드"].astype(str).str[:4].astype(int)
    df["Quarter"] = df["기준_년분기_코드"].astype(str).str[4].astype(int)

    print("\nData loaded. Years found:", df["Year"].unique())
    print("Quarters found:", df["Quarter"].unique())

    # 2. Aggregating Data
    # Calculate Total Workforce Population per Quarter (summing up all dongs)
    quarterly_total = (
        df.groupby(["Year", "Quarter"])["총_직장_인구_수"].sum().reset_index()
    )

    print("\n--- 서울 전체 분기별 총 직장인구 ---")
    print(quarterly_total)

    # Calculate Intra-year statistics
    yearly_stats = quarterly_total.groupby("Year")["총_직장_인구_수"].agg(
        ["mean", "std", "min", "max", "count"]
    )
    yearly_stats["std_pct"] = (yearly_stats["std"] / yearly_stats["mean"]) * 100

    print("\n--- 연도별 변동성 통계 ---")
    print(yearly_stats)

    # Check for significant changes
    for year in yearly_stats.index:
        row = yearly_stats.loc[year]
        print(f"\nYear {year}:")
        print(f"  평균 인구: {row['mean']:,.0f}")
        print(f"  최소 - 최대 범위: {row['min']:,.0f} - {row['max']:,.0f}")
        print(f"  표준 편차: {row['std']:,.0f} ({row['std_pct']:.2f}%)")

        if row["std_pct"] < 1.0:
            print("  -> 해석: 연중 매우 안정적임 (변동폭 1% 미만).")
        elif row["std_pct"] < 5.0:
            print("  -> 해석: 경미한 계절적 변동 있음.")
        else:
            print("  -> 해석: 연중 유의미한 변화가 있음.")

    # 3. Visualization
    fig = px.line(
        quarterly_total,
        x="Quarter",
        y="총_직장_인구_수",
        color="Year",
        markers=True,
        title="분기별 총 직장인구 추이",
        labels={"총_직장_인구_수": "총 직장인구 수", "Quarter": "분기", "Year": "연도"},
        height=600,
    )
    fig.update_layout(
        hovermode="x unified",
        xaxis=dict(tickvals=[1, 2, 3, 4], title="분기"),
    )
    output_html_path = "output/eda_workforce/yearly_trends.html"
    os.makedirs(os.path.dirname(output_html_path), exist_ok=True)
    fig.write_html(output_html_path)
    print(f"\nSaved trend plot to {output_html_path}")

    # 4. Detailed Analysis (Dong level variation)
    # Are there specific dongs that fluctuate a lot?
    # We calculate Coefficient of Variation (CV) for each dong within each year
    dong_variance = df.groupby(["Year", "행정동_코드_명"])["총_직장_인구_수"].agg(
        ["mean", "std"]
    )
    dong_variance["coef_var"] = dong_variance["std"] / dong_variance["mean"]

    # Filter for valid std (at least 2 quarters of data)
    dong_variance = dong_variance.dropna()

    print("\n--- 연도별 변동성이 큰 상위 5개 행정동 ---")
    for year in df["Year"].unique():
        if year in dong_variance.index.get_level_values(0):
            top_var_dongs = (
                dong_variance.loc[year].sort_values("coef_var", ascending=False).head(5)
            )
            print(f"\n[Year {year}]")
            print(top_var_dongs[["mean", "std", "coef_var"]])


if __name__ == "__main__":
    input_csv = (
        "data/01_raw/07_openapi/서울시_상권분석서비스_직장인구_행정동_2023_2025.csv"
    )
    analyze_population_trends(input_csv)
