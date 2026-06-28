import sys
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone
import json

import yfinance as yf
from config import config
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool

# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_news(ticker: str) -> List[Dict[str, Any]]:
    logger.info(f"Fetching news for {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        # yfinance returns a list of dictionaries for news
        return stock.news
    except Exception as e:
        logger.error(f"Failed to fetch news for {ticker}: {e}")
        return []

def process_news_data(ticker: str, raw_news: List[Dict[str, Any]]) -> List[tuple]:
    processed = []
    for item in raw_news:
        try:
            # yfinance recent update wraps data in a 'content' dict
            content_dict = item.get("content", item)
            
            # fallback for older yfinance versions vs new versions
            source_id = item.get("id") or content_dict.get("id") or item.get("uuid")
            if not source_id:
                continue
                
            pub_date_str = content_dict.get("pubDate")
            if pub_date_str:
                try:
                    # Handle 'Z' suffix for Python versions < 3.11
                    pub_date_str = pub_date_str.replace("Z", "+00:00")
                    published_at = datetime.fromisoformat(pub_date_str)
                except ValueError:
                    published_at = datetime.now(tz=timezone.utc)
            else:
                provider_time = item.get("providerPublishTime")
                if provider_time:
                    published_at = datetime.fromtimestamp(provider_time, tz=timezone.utc)
                else:
                    published_at = datetime.now(tz=timezone.utc)
                
            title = content_dict.get("title", "")
            
            url_dict = content_dict.get("canonicalUrl", {})
            link = url_dict.get("url", item.get("link", ""))
            summary = content_dict.get("summary", "")
            content_text = f"{summary}\n\nLink: {link}".strip()
            
            raw_payload = json.dumps(item)
            
            processed.append((
                ticker,
                source_id,
                published_at,
                title,
                content_text,
                raw_payload
            ))
        except Exception as e:
            logger.warning(f"Error processing news item for {ticker}: {e}")
            
    return processed

def upsert_news(pool: SimpleConnectionPool, data: List[tuple]) -> int:
    if not data:
        return 0
        
    upsert_query = """
        INSERT INTO ticker_news (ticker, source_id, published_at, title, content, raw_payload)
        VALUES %s
        ON CONFLICT (ticker, source_id) DO NOTHING
        RETURNING id;
    """
    
    conn = None
    inserted_count = 0
    try:
        conn = pool.getconn()
        with conn.cursor() as cur:
            result = execute_values(
                cur,
                upsert_query,
                data,
                template="(%s, %s, %s, %s, %s, %s::jsonb)",
                fetch=True
            )
            inserted_count = len(result) if result else 0
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error during upsert: {e}")
    finally:
        if conn:
            pool.putconn(conn)
            
    return inserted_count

def main():
    db_url = config.DATABASE_URL
    if not db_url:
        logger.error("DATABASE_URL environment variable is not set.")
        sys.exit(1)
        
    tickers = [t.strip().upper() for t in config.TRACKED_TICKERS.split(",") if t.strip()]
    if not tickers:
        logger.error("TRACKED_TICKERS environment variable is not set or empty.")
        sys.exit(1)
    
    try:
        pool = SimpleConnectionPool(1, 5, dsn=db_url)
    except Exception as e:
        logger.error(f"Failed to establish database connection pool: {e}")
        sys.exit(1)
        
    total_fetched = 0
    total_upserted = 0
    
    try:
        for ticker in tickers:
            raw_news = fetch_news(ticker)
            total_fetched += len(raw_news)
            
            processed_data = process_news_data(ticker, raw_news)
            upserted = upsert_news(pool, processed_data)
            total_upserted += upserted
            
            logger.info(f"[{ticker}] Fetched {len(raw_news)} items, Upserted {upserted} new rows into DB")
            
        logger.info(f"Pipeline complete. Total Fetched: {total_fetched}, Total Upserted: {total_upserted}")
    finally:
        pool.closeall()

if __name__ == "__main__":
    main()
