import io
import time
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import text

# --- 1. Target Database Configurations ---
PG_USER = 'kunal.nandwana'
PG_PASS = 'root'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB   = 'kunal.nandwana'

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# --- 2. Master NSE Index Mapping Configs ---
broad_market_indices = {
    'NIFTY 50': 'https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv',
    'NIFTY NEXT 50': 'https://nsearchives.nseindia.com/content/indices/ind_niftynext50list.csv',
    'NIFTY 100': 'https://nsearchives.nseindia.com/content/indices/ind_nifty100list.csv',
    'NIFTY 200': 'https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv',
    'NIFTY 500': 'https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv',
    'NIFTY MIDCAP 50': 'https://nsearchives.nseindia.com/content/indices/ind_niftymidcap50list.csv',
    'NIFTY MIDCAP 100': 'https://nsearchives.nseindia.com/content/indices/ind_niftymidcap100list.csv',
    'NIFTY SMALLCAP 100': 'https://nsearchives.nseindia.com/content/indices/ind_niftysmallcap100list.csv'
}

sectoral_indices = {
    'NIFTY AUTO': 'https://nsearchives.nseindia.com/content/indices/ind_niftyautolist.csv',
    'NIFTY BANK': 'https://nsearchives.nseindia.com/content/indices/ind_niftybanklist.csv',
    'NIFTY FINANCIAL SERVICES': 'https://nsearchives.nseindia.com/content/indices/ind_niftyfinancialserviceslist.csv',
    'NIFTY FMCG': 'https://nsearchives.nseindia.com/content/indices/ind_niftyfmcglist.csv',
    'NIFTY HEALTHCARE': 'https://nsearchives.nseindia.com/content/indices/ind_niftyhealthcarelist.csv',
    'NIFTY IT': 'https://nsearchives.nseindia.com/content/indices/ind_niftyitlist.csv',
    'NIFTY METAL': 'https://nsearchives.nseindia.com/content/indices/ind_niftymetallist.csv',
    'NIFTY PHARMA': 'https://nsearchives.nseindia.com/content/indices/ind_niftypharmalist.csv',
    'NIFTY PRIVATE BANK': 'https://nsearchives.nseindia.com/content/indices/ind_niftyprivatebanklist.csv',
    'NIFTY PSU BANK': 'https://nsearchives.nseindia.com/content/indices/ind_niftypsubanklist.csv',
    'NIFTY REALTY': 'https://nsearchives.nseindia.com/content/indices/ind_niftyrealtylist.csv',
    'NIFTY OIL AND GAS': 'https://nsearchives.nseindia.com/content/indices/ind_niftyoilgaslist.csv',
    'NIFTY CAPITAL GOODS': 'https://nsearchives.nseindia.com/content/indices/ind_niftycapitalgoodslist.csv',
    'NIFTY POWER': 'https://nsearchives.nseindia.com/content/indices/ind_niftypowerlist.csv'
}

# --- 3. DDL Creation & Schema Setup Block ---
print("Initializing database gold schema layers...")
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
    
    # Core mappings staging table
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS gold.index_universe_mappings (
            ticker TEXT NOT NULL,
            index_universe_name TEXT NOT NULL,
            index_type TEXT NOT NULL CHECK (index_type IN ('BROAD_MARKET', 'SECTORAL')),
            industry TEXT,
            last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, index_universe_name)
        );
    """))
    
    # Incrementally maintain master table — preserve existing rows across runs
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS gold.master_company_sectors (
            ticker TEXT PRIMARY KEY,
            name_of_company TEXT,
            cap_tier TEXT,
            broad_market_indices TEXT[],
            primary_sector TEXT,
            sub_sector_index TEXT,
            last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """))
    
    conn.execute(text("TRUNCATE TABLE gold.index_universe_mappings;"))
print("Database structures verified.\n")

# --- 4. Scraper Pipeline Engine ---
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com/"
})

# Optional quick splash initialization call to register cookies
try:
    session.get("https://www.nseindia.com", timeout=5)
except Exception:
    pass

compiled_records = []

# Scrape Broad Market Components
for idx_name, url in broad_market_indices.items():
    try:
        res = session.get(url, timeout=12)
        if res.status_code == 200:
            df = pd.read_csv(io.StringIO(res.text))
            df.columns = [c.strip().lower() for c in df.columns]
            sym_col = 'symbol' if 'symbol' in df.columns else 'ticker symbol'
            ind_col = 'industry' if 'industry' in df.columns else None
            
            for _, row in df.iterrows():
                industry_val = str(row[ind_col]).strip() if ind_col and pd.notna(row[ind_col]) else None
                compiled_records.append({
                    'ticker': str(row[sym_col]).strip().upper(),
                    'index_universe_name': idx_name,
                    'index_type': 'BROAD_MARKET',
                    'industry': industry_val
                })
            print(f"Successfully scraped: {idx_name}")
            time.sleep(0.3)
    except Exception as e:
        print(f"Failed to fetch {idx_name}: {str(e)}")

# Scrape Sectoral Components
for idx_name, url in sectoral_indices.items():
    try:
        res = session.get(url, timeout=12)
        if res.status_code == 200:
            df = pd.read_csv(io.StringIO(res.text))
            df.columns = [c.strip().lower() for c in df.columns]
            sym_col = 'symbol' if 'symbol' in df.columns else 'ticker symbol'
            ind_col = 'industry' if 'industry' in df.columns else None
            
            for _, row in df.iterrows():
                if ind_col and pd.notna(row[ind_col]):
                    industry_val = str(row[ind_col]).strip()
                else:
                    industry_val = idx_name.replace('NIFTY ', '')
                    
                compiled_records.append({
                    'ticker': str(row[sym_col]).strip().upper(),
                    'index_universe_name': idx_name,
                    'index_type': 'SECTORAL',
                    'industry': industry_val
                })
            print(f"Successfully scraped: {idx_name}")
            time.sleep(0.3)
    except Exception as e:
        print(f"Failed to fetch {idx_name}: {str(e)}")

# --- 5. Database Load & Hierarchy Aggregation Block ---
if compiled_records:
    print(f"\nWriting raw records to staging layer...")
    with engine.begin() as conn:
        for record in compiled_records:
            conn.execute(text("""
                INSERT INTO gold.index_universe_mappings (ticker, index_universe_name, index_type, industry)
                VALUES (:ticker, :index_universe_name, :index_type, :industry)
                ON CONFLICT (ticker, index_universe_name) DO NOTHING;
            """), record)
            
        print("Raw mappings loaded. Committing array-flattened hierarchy into master company view...")

        # Remove tickers that are no longer present in the equities list (delistings)
        conn.execute(text("""
            DELETE FROM gold.master_company_sectors
            WHERE ticker NOT IN (
                SELECT DISTINCT company_name FROM bronze.equities_list
            );
        """))
        
        conn.execute(text("""
            INSERT INTO gold.master_company_sectors (ticker, name_of_company, cap_tier, broad_market_indices, primary_sector, sub_sector_index)
            WITH market_cap_assignments AS (
                SELECT 
                    ticker,
                    CASE 
                        WHEN 'NIFTY 50' = ANY(ARRAY_AGG(index_universe_name)) THEN 'LARGE CAP'
                        WHEN 'NIFTY NEXT 50' = ANY(ARRAY_AGG(index_universe_name)) THEN 'LARGE CAP'
                        WHEN 'NIFTY MIDCAP 100' = ANY(ARRAY_AGG(index_universe_name)) THEN 'MID CAP'
                        WHEN 'NIFTY SMALLCAP 100' = ANY(ARRAY_AGG(index_universe_name)) THEN 'SMALL CAP'
                        ELSE 'SMALL CAP'
                    END as cap_tier,
                    ARRAY_AGG(index_universe_name ORDER BY index_universe_name) AS broad_indices,
                    MAX(CASE WHEN index_type = 'BROAD_MARKET' THEN industry END) as broad_industry,
                    MAX(CASE WHEN index_type = 'SECTORAL' THEN industry END) as sectoral_industry
                FROM gold.index_universe_mappings
                GROUP BY ticker
            ),
            primary_sub_sector AS (
                SELECT DISTINCT ON (ticker)
                    ticker,
                    index_universe_name AS sub_sector_index
                FROM gold.index_universe_mappings
                WHERE index_type = 'SECTORAL'
                ORDER BY ticker, index_universe_name ASC
            ),
            deduplicated_equities AS (
                SELECT DISTINCT ON (company_name)
                    company_name AS ticker,
                    name_of_company
                FROM bronze.equities_list
                ORDER BY company_name, series ASC
            )
            SELECT 
                e.ticker,
                e.name_of_company,
                CASE 
                    WHEN c.cap_tier IS NULL THEN 'MICRO CAP' 
                    ELSE c.cap_tier 
                END AS cap_tier,
                COALESCE(c.broad_indices, ARRAY['NIFTY TOTAL MARKET']::TEXT[]) AS broad_market_indices,
                COALESCE(c.broad_industry, c.sectoral_industry, 'OTHER') AS primary_sector,
                COALESCE(s.sub_sector_index, 'NIFTY GENERAL') AS sub_sector_index
            FROM deduplicated_equities e
            LEFT JOIN market_cap_assignments c ON e.ticker = c.ticker
            LEFT JOIN primary_sub_sector s ON e.ticker = s.ticker
            ON CONFLICT (ticker) DO UPDATE SET
                name_of_company = EXCLUDED.name_of_company,
                cap_tier = EXCLUDED.cap_tier,
                broad_market_indices = EXCLUDED.broad_market_indices,
                primary_sector = EXCLUDED.primary_sector,
                sub_sector_index = EXCLUDED.sub_sector_index,
                last_updated_at = CURRENT_TIMESTAMP
            WHERE
                gold.master_company_sectors.name_of_company IS DISTINCT FROM EXCLUDED.name_of_company
                OR gold.master_company_sectors.cap_tier IS DISTINCT FROM EXCLUDED.cap_tier
                OR gold.master_company_sectors.broad_market_indices IS DISTINCT FROM EXCLUDED.broad_market_indices
                OR gold.master_company_sectors.primary_sector IS DISTINCT FROM EXCLUDED.primary_sector
                OR gold.master_company_sectors.sub_sector_index IS DISTINCT FROM EXCLUDED.sub_sector_index;
        """))
            
    print("🚀 Pipeline Complete! Run your queries against gold.master_company_sectors.")
else:
    print("🚨 No records parsed. Check connection state.")