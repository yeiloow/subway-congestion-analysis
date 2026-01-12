import pandas as pd
from kiwipiepy import Kiwi
from collections import Counter
from wordcloud import WordCloud
import os
import plotly.express as px
from src.utils.visualization import save_plot, apply_theme, get_font_path
from src.utils.config import OUTPUT_DIR

# Apply Theme
apply_theme()


def analyze_and_visualize_text():
    """
    Load news data, perform text analysis (WordCloud, basic sentiment), and save plots.
    """

    # 1. Load Data
    data_path = "data/news_data.csv"
    if not os.path.exists(data_path):
        print(f"Data file not found at {data_path}. Please run crawl_news.py first.")
        # Create dummy data for testing if verification needs it? No, just return.
        return

    df = pd.read_csv(data_path)
    print(f"Loaded {len(df)} news items.")

    # Combined text from title and description
    df["title"] = df["title"].fillna("")
    df["description"] = df["description"].fillna("")

    texts = df["title"] + " " + df["description"]
    full_text = " ".join(texts.tolist())

    # 2. Preprocessing & Tokenization using Kiwi
    print("Tokenizing text with Kiwi... (This might take a moment)")
    kiwi = Kiwi()

    # Analyze and extract nouns (NNG, NNP, NR, NP)
    tokens = kiwi.tokenize(full_text)

    # Filter for Nouns
    noun_tags = {"NNG", "NNP", "NR", "NP"}
    nouns = [t.form for t in tokens if t.tag in noun_tags]

    # Filter stopwords
    stopwords = [
        "지하철",
        "서울",
        "교통",
        "공사",
        "혼잡",
        "시민",
        "승객",
        "운행",
        "구간",
        "지난",
        "오전",
        "오늘",
        "때문",
        "관련",
        "대해",
        "가장",
        "정도",
        "경우",
        "뉴스",
        "기자",
        "위해",
        "사진",
        "제공",
        "이번",
        "우리",
        "그것",
        "곳",
        "수",
        "등",
        "및",
    ]
    filtered_nouns = [n for n in nouns if len(n) > 1 and n not in stopwords]

    # Count frequency
    count = Counter(filtered_nouns)
    top_n = count.most_common(50)
    print(f"Top 10 words: {top_n[:10]}")

    # 3. Generate WordCloud
    # For WordCloud, we still use matplotlib backend logic to generate image, but save it directly.
    output_dir = OUTPUT_DIR / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)

    font_path = get_font_path()
    if not font_path:
        print("Warning: Korean font not found. WordCloud might look broken.")
        font_path = "arial"  # Fallback

    try:
        wc = WordCloud(
            font_path=font_path, background_color="white", width=800, height=600
        )
        wc.generate_from_frequencies(dict(top_n))

        # Save WordCloud as Image (Standard for WordCloud)
        wc_output_path = output_dir / "wc_subway.png"
        wc.to_file(str(wc_output_path))
        print(f"WordCloud saved to {wc_output_path}")

    except Exception as e:
        print(f"WordCloud generation failed: {e}")

    # 4. Simple "Sentiment" Analysis (Keyword Counting) -> Plotly Bar Chart
    negative_keywords = [
        "지연",
        "시위",
        "고장",
        "불편",
        "대란",
        "사고",
        "멈춤",
        "북적",
        "공포",
        "비명",
        "밀집",
        "호흡",
    ]
    positive_keywords = [
        "개통",
        "재개",
        "안전",
        "개선",
        "편리",
        "원활",
        "정상",
        "증편",
        "대책",
    ]

    neg_counts = {k: full_text.count(k) for k in negative_keywords}

    # Create DataFrame for Plotly
    df_sentiment = pd.DataFrame(list(neg_counts.items()), columns=["Keyword", "Count"])
    df_sentiment = df_sentiment.sort_values(by="Count", ascending=False)

    # Plotly Bar Chart
    fig = px.bar(
        df_sentiment,
        x="Keyword",
        y="Count",
        title="지하철 뉴스 주요 부정 키워드 빈도",
        color="Count",
        color_continuous_scale="Reds",
    )

    sentiment_output_path = OUTPUT_DIR / "sentiment_bar.html"
    save_plot(fig, sentiment_output_path)
    print(f"Sentiment Analysis plot saved to {sentiment_output_path}")


if __name__ == "__main__":
    analyze_and_visualize_text()
