import json
import time
import os
import feedparser
import pandas as pd
from confluent_kafka import Producer


KAKFA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
TOPIC = "raw-news"
RSS_FEED_URLs = {
    "Google News": "https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en",
    "BBC World": "http://feeds.bbci.co.uk/news/world/rss.xml",
    "CNN": "http://rss.cnn.com/rss/edition.rss",
    "The Onion": "https://theonion.com/feed/",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "HuffPost": "https://www.huffpost.com/section/world-news/feed"
}

producer = Producer({'bootstrap.servers': KAKFA_SERVER})

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

seen_links = set()


def stream_from_rss():
    print("Reading from RSS feed...")
    for source_name, feed_url in RSS_FEED_URLs.items():
        try:
            feed = feedparser.parse(feed_url, agent=USER_AGENT)

            for entry in feed.entries:
                news_id = entry.get("link", entry.get("id", entry.title))
                
                if news_id not in seen_links:
                    seen_links.add(news_id)
                    
                    payload = {
                        "id": str(hash(news_id)),
                        "headline": entry.title,
                        "text": entry.summary if 'summary' in entry else entry.title,
                        "source": source_name
                    }
                    producer.produce(TOPIC, value=json.dumps(payload).encode('utf-8'))
                    producer.poll(0)
                    print(f"[PRODUCER RSS]: Sent [{source_name}] {entry.title[:50]}...")
                    time.sleep(1)
        except Exception as feed_err:
            print(f"Error fetching {source_name}: {feed_err}")


if __name__ == "__main__":
    print("Starting...")
    time.sleep(10)
    while True:
        try:
            stream_from_rss()
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(60)