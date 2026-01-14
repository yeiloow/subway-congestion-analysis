import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os


def crawl_naver_news(keywords, max_pages=5):
    """
    Crawl Naver News titles and descriptions for given keywords.

    Args:
        keywords (list): List of keywords to search.
        max_pages (int): Number of pages to crawl per keyword.

    Returns:
        pd.DataFrame: DataFrame with columns ['title', 'description', 'link', 'keyword']
    """
    base_url = "https://search.naver.com/search.naver?where=news&sm=tab_jum&query="
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    all_news = []

    for keyword in keywords:
        print(f"Crawling keyword: {keyword}")
        for page in range(max_pages):
            start_val = page * 10 + 1
            url = f"{base_url}{keyword}&start={start_val}"

            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")

                # Naver News search results usually have 'news_tit' class for titles
                title_tags = soup.select("a.news_tit")

                if not title_tags:
                    print(f"No news titles found for {keyword} at page {page + 1}")
                    break

                print(f"Found {len(title_tags)} items for {keyword} page {page + 1}")

                for title_tag in title_tags:
                    title = title_tag.get_text()
                    link = title_tag["href"]

                    # Try to find the parent container (div.news_area or div.news_contents)
                    container = title_tag.find_parent("div", class_="news_area")
                    if not container:
                        container = title_tag.find_parent("div", class_="news_contents")

                    description = ""
                    if container:
                        desc_tag = container.select_one("div.news_dsc")
                        if not desc_tag:
                            desc_tag = container.select_one("div.dsc_wrap")

                        if desc_tag:
                            description = desc_tag.get_text()

                    if description:
                        all_news.append(
                            {
                                "keyword": keyword,
                                "title": title,
                                "description": description,
                                "link": link,
                            }
                        )

                time.sleep(random.uniform(0.5, 1.5))  # Politeness delay

            except Exception as e:
                print(f"Error scraping {url}: {e}")

    return pd.DataFrame(all_news)


def main():
    target_dir = "data"
    os.makedirs(target_dir, exist_ok=True)

    keywords = [
        "서울 지하철 혼잡",
        "지옥철",
        "9호선 혼잡",
        "2호선 출근 시간",
        "지하철 연착",
    ]
    print(f"Starting crawl for {len(keywords)} keywords...")

    df = crawl_naver_news(keywords, max_pages=3)

    if not df.empty:
        output_path = os.path.join(target_dir, "news_data.csv")
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"Successfully saved {len(df)} news items to {output_path}")
    else:
        print("No news found.")


if __name__ == "__main__":
    main()
