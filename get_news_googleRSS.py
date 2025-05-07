import feedparser
from googlenewsdecoder import gnewsdecoder
from newspaper import Article
from datetime import datetime, timedelta
import dateutil.parser
from tqdm import tqdm
import pandas as pd
import pandas_market_calendars as mcal
import time
import os
import json
import random


def collect_news_for_ticker(ticker, start_loop, end_loop, config_path="tickers.json"):
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except Exception as e:
        print(f"tickers.json 파일 읽기 실패: {e}")
        return

    if ticker not in config:
        print(f"'{ticker}'는 tickers.json안에 존재하지 않는 ticker입니다.")
        return

    query = config[ticker]["query"]
    output_dir = os.path.join(ticker, "NewsData")

    seen_links = set()
    seen_titles = set()
    os.makedirs(output_dir, exist_ok=True)
    date_list = pd.date_range(start=start_loop, end=end_loop)
    tolerance_days = 1

    for i, end_date in enumerate(date_list, start=1):
        end_date = end_date.date()
        start_date = end_date - timedelta(days=1)

        print(f"\n[{i}/{len(date_list)}] 뉴스 수집 기간: {start_date} ~ {end_date}")

        encoded_query = query.replace(" ", "+")
        rss_url = (
            f'https://news.google.com/rss/search?q={encoded_query}+after:{start_date.strftime("%Y-%m-%d")}'
            f'+before:{(end_date + timedelta(days=1)).strftime("%Y-%m-%d")}&hl=en&gl=US&ceid=US:en'
        )

        feed = feedparser.parse(rss_url)
        entries = feed.entries
        print(
            f"[{i}/{len(date_list)}] , {start_date} ~ {end_date} 총 기사 수: {len(entries)}"
        )

        all_records = []
        filter_start = datetime.combine(end_date, datetime.min.time()) - timedelta(
            days=tolerance_days
        )
        filter_end = datetime.combine(end_date, datetime.max.time()) + timedelta(
            days=tolerance_days
        )

        for entry in tqdm(
            entries, desc=f"{end_date} 기사 처리", unit="article", leave=False
        ):
            try:
                pubDate = dateutil.parser.parse(entry.published).replace(tzinfo=None)

                if not (filter_start <= pubDate <= filter_end):
                    continue
                if pubDate.date() not in nasdaq_open_dates:
                    continue

                title = entry.title.strip()
                if not title or title in seen_titles:
                    continue

                google_link = entry.link
                if google_link in seen_links:
                    continue

                seen_titles.add(title)
                seen_links.add(google_link)

                google_link = entry.link
                result = gnewsdecoder(google_link)
                time.sleep(random.uniform(1.5, 2.0))

                real_url = result.get("decoded_url")
                if (
                    not result.get("status")
                    or not real_url
                    or "news.google.com" in real_url
                ):
                    continue
                if real_url in seen_links:
                    continue
                seen_links.add(real_url)

                article = Article(real_url)
                article.download()
                time.sleep(random.uniform(1.5, 2.0))
                article.parse()

                title = article.title
                body = article.text.strip()
                final_date = article.publish_date or pubDate

                if final_date.date() not in nasdaq_open_dates:
                    continue

                if not (filter_start <= final_date <= filter_end):
                    continue

                if not body:
                    continue

                all_records.append(
                    {
                        "title": title,
                        "date": final_date.date(),
                        "link": real_url,
                        "body": body,
                    }
                )

            except Exception as e:
                print(f"  [오류] {entry.link}\n  사유: {e}")

        if all_records:
            df = pd.DataFrame(all_records)
            grouped = df.groupby("date")

            for pub_date, group_df in grouped:
                filename = os.path.join(output_dir, f"{ticker}_{pub_date}.parquet")
                temp_filename = filename + ".tmp"

                if os.path.exists(filename):
                    existing_df = pd.read_parquet(filename)
                    combined_df = pd.concat([existing_df, group_df], ignore_index=True)
                    combined_df.drop_duplicates(subset=["link"], inplace=True)
                    print(f"기존 파일 병합: {filename} → 총 {len(combined_df)}건")
                else:
                    combined_df = group_df
                    print(f"새 파일 생성: {filename} → {len(combined_df)}건")

                combined_df.to_parquet(temp_filename, index=False)
                os.replace(temp_filename, filename)
                print(f"저장 완료: {filename}")
        else:
            print("저장할 뉴스가 없습니다.")

        time.sleep(random.uniform(2.5, 5.0))


if __name__ == "__main__":
    # start_loop ~ end_loop 기간동안의 크롤링을 진행
    start_loop = datetime(2025, 1, 1)
    end_loop = datetime(2025, 5, 1)
    nasdaq = mcal.get_calendar("NASDAQ")
    open_days = nasdaq.valid_days(start_date=start_loop, end_date=end_loop)
    open_days = open_days.tz_localize(None)
    nasdaq_open_dates = set(pd.to_datetime(open_days).date)

    # tickers.json에 있는 티커에 대해서만 크롤링
    tickers = ["NVDA"]

    for ticker in tickers:
        print(f"\n[{ticker}] 뉴스 수집 시작")
        collect_news_for_ticker(ticker=ticker, start_loop=start_loop, end_loop=end_loop)
