import pandas as pd
import sqlalchemy
from sqlalchemy import text
import yfinance as yf

# --- 1. Target Database Configurations ---
PG_USER = 'kunal.nandwana'
PG_PASS = 'root'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB   = 'kunal.nandwana'

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# --- 2. Complete Ticker Translation Matrix ---
index_mappings = {
    'NIFTY 50': '^NSEI',
    'NIFTY NEXT 50': '^NSMIDCP',
    'NIFTY 100': '^CNX100',
    'NIFTY 200': '^CNX200',
    'NIFTY 500': '^CRSLDX',
    'NIFTY MIDCAP 100': '^CRSMID',
    'NIFTY SMALLCAP 100': '^CNXSC',
    'NIFTY AUTO': '^CNXAUTO',
    'NIFTY BANK': '^NSEBANK',
    'NIFTY FINANCIAL SERVICES': 'NIFTY_FIN_SERVICE.NS',
    'NIFTY FMCG': '^CNXFMCG',
    'NIFTY HEALTHCARE': 'NIFTY_HEALTHCARE.NS',
    'NIFTY IT': '^CNXIT',
    'NIFTY METAL': '^CNXMETAL',
    'NIFTY PHARMA': '^CNXPHARMA',
    'NIFTY PRIVATE BANK': 'NIFTY_PVT_BANK.NS',
    'NIFTY PSU BANK': 'NIFTY_PSU_BANK.NS',
    'NIFTY REALTY': '^CNXREALTY',
    'NIFTY OIL AND GAS': 'NIFTY_OIL_AND_GAS.NS',
    'NIFTY CAPITAL GOODS': 'NIFTY_CONSR_DURBL.NS',
    'NIFTY POWER': 'NIFTY_ENERGY.NS'
}

# Configurable sliding window lookback parameter
LOOKBACK_DAYS = 5

# --- 3. Dynamic Date Engine Block ---
print("Calculating sliding execution window limits...")
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(trade_date) FROM silver.daily_index_prices;")).fetchone()
        max_db_date = result[0]
except Exception as e:
    print(f"⚠️ Could not read target database watermark. Defaulting lock step fallback. Error: {str(e)}")
    max_db_date = None

if max_db_date:
    # Look back a few days from the max date to safely overwrite provisional prints or capture missing sessions
    optimized_start = (pd.to_datetime(max_db_date) - pd.Timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    print(f"Pristine state identified. Max table date: {max_db_date}. Sliding window start date: {optimized_start}")
else:
    optimized_start = (pd.Timestamp.now() - pd.Timedelta(days=15)).strftime('%Y-%m-%d')
    print(f"No database state found. Defaulting to safe catch-up window start date: {optimized_start}")

# --- 4. Yahoo Incremental Extraction Engine ---
compiled_incremental_records = []

for index_name, ticker in index_mappings.items():
    try:
        # Download just the sliding lookback window
        df = yf.download(ticker, start=optimized_start, progress=False)
        
        if df is not None and not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
                
            df = df.reset_index()
            
            for _, row in df.iterrows():
                if pd.isna(row['Close']) or pd.isna(row['Open']):
                    continue
                    
                clean_date = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
                
                compiled_incremental_records.append({
                    'index_name': index_name,
                    'trade_date': clean_date,
                    'open_price': float(row['Open']),
                    'high_price': float(row['High']),
                    'low_price': float(row['Low']),
                    'close_price': float(row['Close']),
                    'volume': int(row['Volume']) if pd.notna(row['Volume']) else 0
                })
    except Exception as e:
        print(f" ❌ Failed tracking increment for {index_name}: {str(e)}")

# --- 5. High-Speed Incremental Merge/UPSERT ---
if compiled_incremental_records:
    print(f"\nMerging {len(compiled_incremental_records)} sliding rows into silver.daily_index_prices...")
    with engine.begin() as conn:
        for record in compiled_incremental_records:
            conn.execute(text("""
                INSERT INTO silver.daily_index_prices (index_name, trade_date, open_price, high_price, low_price, close_price, volume)
                VALUES (:index_name, :trade_date, :open_price, :high_price, :low_price, :close_price, :volume)
                ON CONFLICT (index_name, trade_date) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    last_ingested_at = CURRENT_TIMESTAMP;
            """), record)
    print("🚀 Incremental Sync Complete! Current sessions merged seamlessly.")
else:
    print("💤 Database tables are already completely up to date. No new sessions to merge.")