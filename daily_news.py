import time
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from nselib import capital_market

# --- 1. Target Database Configurations ---
PG_USER = 'kunal.nandwana'
PG_PASS = 'root'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB   = 'kunal.nandwana'

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# --- 2. DDL Infrastructure Initialization Block ---
print("Initializing alternative structural data layers...")
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS alternative_data;"))
    
    # Reset table to establish pristine structural alignment across all metrics
    conn.execute(text("DROP TABLE IF EXISTS alternative_data.daily_ticker_news;"))
    conn.execute(text("""
        CREATE TABLE alternative_data.daily_ticker_news (
            article_id TEXT PRIMARY KEY,
            ticker TEXT NOT NULL,
            published_at TIMESTAMP NOT NULL,
            source TEXT DEFAULT 'NSE_CORPORATE_ANNOUNCEMENT',
            headline TEXT NOT NULL,
            summary TEXT,
            sentiment_label TEXT DEFAULT 'NEUTRAL',
            last_ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """))
print("Structure verified.\n")

# --- 3. Dynamic Date Window (Set range for back-testing or current days) ---
start_dt = '22-06-2026'
end_dt   = '28-06-2026'

# --- 4. Mass Exchange Announcement Extraction Core ---
compiled_announcements = []

try:
    print(f"Extracting entire corporate news footprint from {start_dt} to {end_dt}...")
    # This pulls ALL corporate updates across all listed tickers in one master call
    df = capital_market.corporate_announcements(from_date=start_dt, to_date=end_dt)
    
    if df is not None and not df.empty:
        df.columns = [col.strip().upper() for col in df.columns]
        
        # Map dynamic exchange column names safely
        ticker_col = next((c for c in df.columns if c in ['SYMBOL', 'TICKER']), None)
        date_col   = next((c for c in df.columns if c in ['ANNOUNCEMENT_DATE', 'TIMESTAMP', 'DATE']), None)
        desc_col   = next((c for c in df.columns if c in ['SUBJECT', 'DESC', 'DETAILS']), None)
        
        for _, row in df.iterrows():
            if pd.isna(row[ticker_col]) or pd.isna(row[desc_col]):
                continue
                
            ticker = str(row[ticker_col]).strip().upper()
            headline = str(row[desc_col]).strip()
            
            # Create a unique tracking identifier to protect against duplicated entry records
            clean_date_str = str(row[date_col]).strip()
            article_id = f"NSE_{ticker}_{hash(clean_date_str + headline)}"
            
            # Naive programmatic sentiment assignment placeholder for downstream AI parsing logic
            headline_lower = headline.lower()
            if any(w in headline_lower for w in ['order win', 'profit up', 'dividend', 'acquisition', 'expansion']):
                sentiment = 'BULLISH'
            elif any(w in headline_lower for w in ['loss', 'resignation', 'penalty', 'sebi order', 'strike']):
                sentiment = 'BEARISH'
            else:
                sentiment = 'NEUTRAL'
                
            # Safely handle mixed corporate time stamps
            try:
                published_time = pd.to_datetime(clean_date_str, format='mixed').strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                published_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

            compiled_announcements.append({
                'article_id': article_id,
                'ticker': ticker,
                'published_at': published_time,
                'headline': headline,
                'summary': str(row.get('DETAILS', '')).strip() if row.get('DETAILS') else headline,
                'sentiment_label': sentiment
            })
            
        print(f" ✅ Success! Extracted and mapped {len(compiled_announcements)} master corporate entries.")
    else:
        print(" ⚠️ Exchange returned an empty payload wrapper for this timeframe.")
except Exception as e:
    print(f" ❌ Failed to execute corporate extraction: {str(e)}")

# --- 5. Database Multi-row Bulk Upsert ---
if compiled_announcements:
    print(f"Bulk loading rows to alternative_data.daily_ticker_news...")
    with engine.begin() as conn:
        for record in compiled_announcements:
            conn.execute(text("""
                INSERT INTO alternative_data.daily_ticker_news (article_id, ticker, published_at, headline, summary, sentiment_label)
                VALUES (:article_id, :ticker, :published_at, :headline, :summary, :sentiment_label)
                ON CONFLICT (article_id) DO NOTHING;
            """), record)
    print("🚀 Pipeline Complete! Your multi-ticker news database layer is fully loaded.")
else:
    print("🚨 Ingestion complete with no rows extracted.")