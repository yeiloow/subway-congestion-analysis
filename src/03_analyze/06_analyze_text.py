import pandas as pd
from kiwipiepy import Kiwi
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import os


def analyze_and_visualize_text():
    """
    Load news data, perform text analysis (WordCloud, basic sentiment), and save plots.
    """

    # 1. Load Data
    data_path = "data/news_data.csv"
    if not os.path.exists(data_path):
        print(f"Data file not found at {data_path}. Please run crawl_news.py first.")
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
    # Tag list: https://github.com/bab2min/kiwipiepy
    noun_tags = {"NNG", "NNP", "NR", "NP"}
    nouns = [t.form for t in tokens if t.tag in noun_tags]

    # Filter stopwords and single-character words
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
    output_dir = "output/plots"
    os.makedirs(output_dir, exist_ok=True)

    # Use a font that supports Korean. Mac usually has AppleGothic.
    font_path = "/System/Library/Fonts/Supplemental/AppleGothic.ttf"
    if not os.path.exists(font_path):
        font_path = "/System/Library/Fonts/AppleGothic.ttf"
        if not os.path.exists(font_path):
            # Try another common Korean font
            font_path = "/System/Library/Fonts/Malgun Gothic.ttf"

    try:
        wc = WordCloud(
            font_path=font_path, background_color="white", width=800, height=600
        )
        wc.generate_from_frequencies(dict(top_n))

        plt.figure(figsize=(10, 8))
        plt.imshow(wc, interpolation="bilinear")
        plt.axis("off")
        wc_output_path = os.path.join(output_dir, "wc_subway.png")
        plt.savefig(wc_output_path)
        print(f"WordCloud saved to {wc_output_path}")
        plt.close()
    except Exception as e:
        print(f"WordCloud generation failed (font issue?): {e}")

    # 4. Simple "Sentiment" Analysis (Keyword Counting)
    # Define negative keywords related to subway issues
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
    pos_counts = {k: full_text.count(k) for k in positive_keywords}

    # Sort for plotting
    neg_counts = dict(
        sorted(neg_counts.items(), key=lambda item: item[1], reverse=True)
    )

    plt.figure(figsize=(10, 6))
    # Matplotlib needs font setting for Korean
    from matplotlib import rc

    try:
        rc("font", family="AppleGothic")
    except:
        pass

    plt.rcParams["axes.unicode_minus"] = False

    plt.bar(neg_counts.keys(), neg_counts.values(), color="salmon")
    plt.title("Frequency of Negative Keywords in Subway News")
    plt.xlabel("Keyword")
    plt.ylabel("Frequency")

    sentiment_output_path = os.path.join(output_dir, "sentiment_bar.png")
    plt.savefig(sentiment_output_path)
    print(f"Sentiment Analysis plot saved to {sentiment_output_path}")
    plt.close()


if __name__ == "__main__":
    analyze_and_visualize_text()
