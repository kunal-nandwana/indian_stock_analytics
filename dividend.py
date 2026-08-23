import requests
import xml.etree.ElementTree as ET
import re
import sqlalchemy
from sqlalchemy import text
import pandas as pd
from datetime import datetime

# --- Database Configurations ---
PG_USER = 'kunal.nandwana'
PG_PASS = 'root'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB   = 'kunal.nandwana'

def get_db_engine():
    return sqlalchemy.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

def fetch_and_parse_nse_rss():
    url = "https://nsearchives.nseindia.com/content/RSS/Corporate_action.xml"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"Error calling NSE RSS URL: {e}")
        return pd.DataFrame()

    try:
        root = ET.fromstring(response.content)
    except Exception as e:
        print(f"XML Parsing Error: {e}")
        return pd.DataFrame()

    parsed_actions = []
    
    for item in root.findall(".//item"):
        title_text = item.find("title").text or ""
        desc_text = item.find("description").text or ""
        pub_date = item.find("pubDate").text or ""

        # Extract Company Name and Ex-Date from title: "HEG Limited - Ex-Date: 22-Jul-2026 "
        company_name = ""
        ex_date_str = None
        if " - Ex-Date:" in title_text:
            parts = title_text.split(" - Ex-Date:")
            company_name = parts[0].strip()
            ex_date_str = parts[1].strip()

        # Parse pipe-separated Description attributes
        desc_parts = desc_text.split("|")
        desc_dict = {}
        for part in desc_parts:
            if ":" in part:
                k, v = part.split(":", 1)
                desc_dict[k.strip().upper()] = v.strip()

        purpose = desc_dict.get("PURPOSE", "")
        record_date_str = desc_dict.get("RECORD DATE", "")
        
        # Only parse entries related to Dividends
        if "DIVIDEND" not in purpose.upper():
            continue

        # Matches numbers following 'RS' or 'RS.' safely by treating the trailing dot as optional
        amounts = re.findall(r"RS\.?\s*(\d+(?:\.\d+)?)", purpose, re.IGNORECASE)
        
        # Safe extraction via try/except loop to entirely avoid ValueError crashes
        valid_amounts = []
        for x in amounts:
            try:
                valid_amounts.append(float(x))
            except ValueError:
                pass
        
        amount = sum(valid_amounts) if valid_amounts else 0.0

        # Standardize Dates
        try:
            ex_date = datetime.strptime(ex_date_str, "%d-%b-%Y").date() if ex_date_str else None
        except Exception:
            ex_date = None

        try:
            record_date = datetime.strptime(record_date_str, "%d-%b-%Y").date() if record_date_str else None
        except Exception:
            record_date = None

        parsed_actions.append({
            "company_name": company_name,
            "purpose": "Dividend" if "dividend" in purpose.lower() else "Special Dividend",
            "amount": amount,
            "ex_date": ex_date,
            "record_date": record_date,
            "payment_date": None, # Filled dynamically or leave null if missing
            "raw_text": purpose
        })
        
    return pd.DataFrame(parsed_actions) if parsed_actions else pd.DataFrame()

def hb_helper(ticker):
    """Fallback helper maps common names to standard trading ticker codes"""
    mapping = {
        "HEG Limited": "HEG",
        "Heritage Foods Limited": "HERITGFOOD",
        "Hero MotoCorp Limited": "HEROMOTOCO",
        "Hindalco Industries Limited": "HINDALCO",
        "Hindustan Petroleum Corporation Limited": "HINDPETRO",
        "Honeywell Automation India Limited": "HONAUT",
        "Intellect Design Arena Limited": "INTELLECT"
    }
    return mapping.get(ticker, None)

def ingest_rss_to_db():
    df = fetch_and_parse_nse_rss()
    if df.empty:
        print("No dividend entries extracted from RSS Feed.")
        return False

    engine = get_db_engine()
    
    # Initialize Table Schema with Composite Primary Key to support multiple corporate events
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.dividend_calendar (
                ticker VARCHAR(20),
                company_name VARCHAR(150),
                purpose VARCHAR(100),
                amount NUMERIC(10, 2),
                ex_date DATE,
                record_date DATE,
                payment_date DATE,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, ex_date)
            );
        """))

    matched_records = []
    
    # Map full company names to short ticker symbols
    with engine.connect() as conn:
        for _, row in df.iterrows():
            company = row['company_name']
            ticker = hb_helper(company)
            
            if not ticker:
                # Fuzzy DB lookup by substring
                clean_name = re.sub(r'\b(limited|ltd|corp|corporation|industries|ind|infrastructure|holding|holdings)\b', '', company, flags=re.IGNORECASE).strip()
                
                # FIXED: Updated to use your real database column: name_of_company
                query = text("""
                    SELECT ticker FROM gold.master_company_sectors 
                    WHERE LOWER(name_of_company) LIKE :c_name 
                       OR :full_name LIKE '%' || LOWER(ticker) || '%' 
                    LIMIT 1;
                """)
                res = conn.execute(query, {"c_name": f"%{clean_name.lower()}%", "full_name": company.lower()}).fetchone()
                if res:
                    ticker = res[0]
                else:
                    # Fallback to the first word of the company name if unmatched
                    ticker = clean_name.split()[0].upper()

            matched_records.append({
                "ticker": ticker,
                "company_name": company,
                "purpose": row['purpose'],
                "amount": row['amount'],
                "ex_date": row['ex_date'],
                "record_date": row['record_date'],
                "payment_date": None
            })

    if matched_records:
        with engine.begin() as conn:
            for rec in matched_records:
                conn.execute(text("""
                    INSERT INTO gold.dividend_calendar (ticker, company_name, purpose, amount, ex_date, record_date, payment_date, last_updated)
                    VALUES (:ticker, :company_name, :purpose, :amount, :ex_date, :record_date, :payment_date, CURRENT_TIMESTAMP)
                    ON CONFLICT (ticker, ex_date) DO UPDATE SET
                        amount = EXCLUDED.amount,
                        record_date = EXCLUDED.record_date,
                        purpose = EXCLUDED.purpose,
                        last_updated = CURRENT_TIMESTAMP;
                """), rec)
        print(f"Ingested {len(matched_records)} records successfully.")
        return True
    return False

if __name__ == "__main__":
    ingest_rss_to_db()