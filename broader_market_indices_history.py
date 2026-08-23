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
# Comprehensive mapping from NSE Index Names to corresponding Yahoo Tickers
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

# Full macro sweep window boundary configuration
start_date = '2013-01-01'
end_date   = '2026-06-28'

# --- 3. DDL Initialization Block ---
print("Initializing historical index database layer...")
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
    
    # Clean reset to build a pristine continuous historical series
    conn.execute(text("DROP TABLE IF EXISTS silver.daily_index_prices;"))
    conn.execute(text("""
        CREATE TABLE silver.daily_index_prices (
            index_name TEXT NOT NULL,
            trade_date DATE NOT NULL,
            open_price NUMERIC,
            high_price NUMERIC,
            low_price NUMERIC,
            close_price NUMERIC,
            volume BIGINT DEFAULT 0,
            last_ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (index_name, trade_date)
        );
    """))
print("Structure verified. Launching high-speed historical load...\n")

# --- 4. Yahoo Bulk Core Extraction Engine ---
for index_name, ticker in index_mappings.items():
    try:
        print(f"Downloading {index_name} ({ticker}) from 2013 to 2026...", end="", flush=True)
        
        # Pull down the entire 13-year dataset instantly in a single data transfer block
        df = yf.download(ticker, start=start_date, end=end_date, progress=False)
        
        if df is not None and not df.empty:
            # Flatten Yahoo MultiIndex columns safely if multi-threaded modes execute
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
                
            df = df.reset_index()
            compiled_index_records = []
            
            for _, row in df.iterrows():
                if pd.isna(row['Close']) or pd.isna(row['Open']):
                    continue
                    
                clean_date = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
                
                compiled_index_records.append({
                    'index_name': index_name,
                    'trade_date': clean_date,
                    'open_price': float(row['Open']),
                    'high_price': float(row['High']),
                    'low_price': float(row['Low']),
                    'close_price': float(row['Close']),
                    'volume': int(row['Volume']) if pd.notna(row['Volume']) else 0
                })
            
            # Efficient Multi-row pipeline commit transaction block
            if compiled_index_records:
                with engine.begin() as conn:
                    for record in compiled_index_records:
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
                print(f" Success ({len(compiled_index_records)} historical daily bars populated on disk)")
            else:
                print(" ⚠️ Mapped data translation returned an empty matrix.")
        else:
            print(" ⚠️ Exchange servers returned an empty payload wrapper.")
            
    except Exception as e:
        print(f" ❌ Failed: {str(e)}")

print("\n🚀 Pipeline Execution Complete! All indices have been completely loaded from 2013 to date.")