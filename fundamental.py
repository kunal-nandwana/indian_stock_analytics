import sqlalchemy
from sqlalchemy import text
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re

# --- Database Configurations ---
PG_USER = 'kunal.nandwana'
PG_PASS = 'root'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB   = 'kunal.nandwana'

def get_db_engine():
    return sqlalchemy.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

def clean_float(val_str):
    """Removes commas, percentages, and signs to return a clean float."""
    if pd.isna(val_str) or val_str is None:
        return 0.0
    clean_str = re.sub(r'[^\d\.\-]', '', str(val_str))
    try:
        return float(clean_str) if clean_str.strip() != '' else 0.0
    except ValueError:
        return 0.0

def fetch_screener_all_data(ticker):
    """Scrapes the top widgets and all key data tables from Screener.in for a given ticker."""
    url = f"https://www.screener.in/company/{ticker}/consolidated/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code == 404:
        url = f"https://www.screener.in/company/{ticker}/"
        response = requests.get(url, headers=headers, timeout=10)
        
    if response.status_code != 200:
        print(f"Skipping {ticker} - Page unreachable.")
        return None

    soup = BeautifulSoup(response.text, 'lxml')
    data = {}

    # 1. Parse Top Information Block Widgets
    top_cards = soup.find('div', {'id': 'top-ratios'})
    if top_cards:
        for li in top_cards.find_all('li'):
            name_span = li.find('span', class_='name')
            value_span = li.find('span', class_='number')
            if name_span and value_span:
                name = name_span.text.strip()
                val = value_span.text.strip()
                if 'Market Cap' in name: data['market_cap'] = clean_float(val)
                elif 'ROCE' in name: data['roce_pct'] = clean_float(val)
                elif 'Debt to Equity' in name: data['debt_to_equity'] = clean_float(val)

    # 2. Extract Data Tables via Pandas helper
    # Quarters Table
    q_section = soup.find('section', id='quarters')
    if q_section and q_section.find('table'):
        q_df = pd.read_html(str(q_section.find('table')))[0]
        q_df.rename(columns={q_df.columns[0]: 'Item'}, inplace=True)
        q_df.set_index('Item', inplace=True)
        data['quarters_df'] = q_df

    # Balance Sheet Table
    bs_section = soup.find('section', id='balance-sheet')
    if bs_section and bs_section.find('table'):
        bs_df = pd.read_html(str(bs_section.find('table')))[0]
        bs_df.rename(columns={bs_df.columns[0]: 'Item'}, inplace=True)
        bs_df.set_index('Item', inplace=True)
        data['balance_df'] = bs_df

    # Shareholding Table
    sh_section = soup.find('section', id='shareholding')
    if sh_section and sh_section.find('table'):
        sh_df = pd.read_html(str(sh_section.find('table')))[0]
        sh_df.rename(columns={sh_df.columns[0]: 'Item'}, inplace=True)
        sh_df.set_index('Item', inplace=True)
        data['shareholding_df'] = sh_df

    return data

def populate_complete_fundamentals():
    engine = get_db_engine()
    
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.company_fundamentals (
                ticker VARCHAR(20),
                fiscal_period VARCHAR(20),
                market_cap NUMERIC(15, 2),
                revenue_yoy_pct NUMERIC(8, 2),
                pat_yoy_pct NUMERIC(8, 2),
                ebitda_margin_pct NUMERIC(5, 2),
                debt_to_equity NUMERIC(5, 2),
                roce_pct NUMERIC(5, 2),
                retail_holding_pct NUMERIC(5, 2),
                cwip_inr_cr NUMERIC(15, 2),
                fixed_assets_inr_cr NUMERIC(15, 2),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, fiscal_period)
            );
        """))

    try:
        with engine.connect() as conn:
            tickers_df = pd.read_sql(text("SELECT ticker FROM gold.master_company_sectors"), conn)
            ticker_list = tickers_df['ticker'].tolist()
    except Exception as e:
        print(f"Database error loading master universe: {e}")
        return

    for ticker in ticker_list:
        print(f"Parsing complete Screener metrics for: {ticker}")
        scraped = fetch_screener_all_data(ticker)
        
        if not scraped or 'quarters_df' not in scraped or 'balance_df' not in scraped:
            continue
            
        q_df = scraped['quarters_df']
        bs_df = scraped['balance_df']
        sh_df = scraped.get('shareholding_df', None)

        # Target recent timeline frames dynamically
        available_periods = [col for col in q_df.columns if any(year in col for year in ['2024', '2025', '2026'])][-2:]

        for period in available_periods:
            try:
                # 🛠️ FIXED: Dynamic Row-Mapping Matrix for Revenue Variations
                revenue_keys = ['Sales', 'Revenue', 'Revenue from Operations', 'Interest Income', 'Operating Revenue']
                sales_row = None
                for key in revenue_keys:
                    if key in q_df.index:
                        sales_row = key
                        break
                
                if not sales_row:
                    print(f"Could not map a distinct revenue line for {ticker} in period {period}. Skipping.")
                    continue

                curr_sales = clean_float(q_df.loc[sales_row, period])
                q_cols = list(q_df.columns)
                curr_idx = q_cols.index(period)
                
                # Compute YoY metrics
                prev_sales, sales_yoy = 0.0, 0.0
                if curr_idx >= 4:
                    prev_sales = clean_float(q_df.iloc[q_df.index.get_loc(sales_row), curr_idx - 4])
                    sales_yoy = round(((curr_sales - prev_sales) / (prev_sales if prev_sales != 0 else 1)) * 100, 2)

                # 🛠️ FIXED: Dynamic Row-Mapping Matrix for Net Profit Variations
                pat_keys = ['Net Profit', 'Net Income', 'Profit After Tax', 'PAT']
                pat_row = None
                for key in pat_keys:
                    if key in q_df.index:
                        pat_row = key
                        break

                if not pat_row:
                    continue

                curr_pat = clean_float(q_df.loc[pat_row, period])
                prev_pat, pat_yoy = 0.0, 0.0
                if curr_idx >= 4:
                    prev_pat = clean_float(q_df.iloc[q_df.index.get_loc(pat_row), curr_idx - 4])
                    pat_yoy = round(((curr_pat - prev_pat) / (prev_pat if prev_pat != 0 else 1)) * 100, 2)

                # EBITDA Margin Math
                exp_row = 'Expenses'
                curr_exp = clean_float(q_df.loc[exp_row, period]) if exp_row in q_df.index else 0.0
                ebitda = curr_sales - curr_exp
                ebitda_margin = round((ebitda / (curr_sales if curr_sales != 0 else 1)) * 100, 2)

                # --- Balance Sheet Extraction ---
                year_match = re.search(r'(Jun|Sep|Dec|Mar)\s+(\d{4})', period)
                bs_period = f"Mar {year_match.group(2)}" if year_match else bs_df.columns[-1]
                if bs_period not in bs_df.columns:
                    bs_period = bs_df.columns[-1]

                fa_raw = bs_df.loc['Fixed Assets', bs_period] if 'Fixed Assets' in bs_df.index else 0
                cwip_raw = bs_df.loc['CWIP', bs_period] if 'CWIP' in bs_df.index else 0
                
                fixed_assets = clean_float(fa_raw)
                cwip = clean_float(cwip_raw)

                # --- Shareholding Public Tracking ---
                retail_holding = 0.0
                if sh_df is not None:
                    sh_period = period if period in sh_df.columns else sh_df.columns[-1]
                    public_row = [idx for idx in sh_df.index if 'Public' in idx or 'Retail' in idx]
                    if public_row:
                        retail_holding = clean_float(sh_df.loc[public_row[0], sh_period])

                # --- Static Ratios ---
                market_cap = scraped.get('market_cap', 0.0)
                roce = scraped.get('roce_pct', 0.0)
                debt_to_equity = scraped.get('debt_to_equity', 0.0)

                # --- Execute Atomic PostgreSQL Upsert ---
                with engine.begin() as upsert_conn:
                    upsert_conn.execute(text("""
                        INSERT INTO gold.company_fundamentals (
                            ticker, fiscal_period, market_cap, revenue_yoy_pct, pat_yoy_pct, 
                            ebitda_margin_pct, debt_to_equity, roce_pct, retail_holding_pct, 
                            cwip_inr_cr, fixed_assets_inr_cr, last_updated
                        ) VALUES (
                            :ticker, :period, :mcap, :rev, :pat, :ebitda, :dte, :roce, :retail, :cwip, :fa, CURRENT_TIMESTAMP
                        ) ON CONFLICT (ticker, fiscal_period) DO UPDATE SET
                            market_cap = EXCLUDED.market_cap,
                            revenue_yoy_pct = EXCLUDED.revenue_yoy_pct,
                            pat_yoy_pct = EXCLUDED.pat_yoy_pct,
                            ebitda_margin_pct = EXCLUDED.ebitda_margin_pct,
                            debt_to_equity = EXCLUDED.debt_to_equity,
                            roce_pct = EXCLUDED.roce_pct,
                            retail_holding_pct = EXCLUDED.retail_holding_pct,
                            cwip_inr_cr = EXCLUDED.cwip_inr_cr,
                            fixed_assets_inr_cr = EXCLUDED.fixed_assets_inr_cr,
                            last_updated = CURRENT_TIMESTAMP;
                    """), {
                        "ticker": ticker, "period": period, "mcap": market_cap, "rev": sales_yoy, "pat": pat_yoy,
                        "ebitda": ebitda_margin, "dte": debt_to_equity, "roce": roce, "retail": retail_holding,
                        "cwip": cwip, "fa": fixed_assets
                    })
                    
            except Exception as item_ex:
                print(f"Skipping period validation {period} for {ticker}: {item_ex}")
                continue

        print(f"Data sync complete for: {ticker}")
        time.sleep(1.5)

if __name__ == "__main__":
    populate_complete_fundamentals()