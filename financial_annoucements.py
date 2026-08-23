import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

# --- Database Configurations ---
PG_USER = 'kunal.nandwana'
PG_PASS = 'root'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB   = 'kunal.nandwana'

def get_db_engine():
    return sqlalchemy.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

def fetch_nse_event_calendar():
    # Landing URL to acquire fresh, validated session security tokens
    base_url = "https://www.nseindia.com/companies-listing/corporate-filings-event-calendar"
    
    # Hidden API endpoint that streams the exact table view array shown in your UI screenshot
    api_url = "https://www.nseindia.com/api/event-calendar?index=equities"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-event-calendar"
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    try:
        # Step 1: Hit the frontend main page to establish dynamic exchange session cookies
        session.get(base_url, timeout=12)
        
        # Step 2: Query the live calendar JSON data vector directly using the active cookies
        response = session.get(api_url, timeout=12)
        response.raise_for_status()
        
        return response.json()
    except Exception as e:
        print(f"Error executing security handshake with NSE event API stream: {e}")
        return []

def ingest_calendar_to_db():
    raw_events = fetch_nse_event_calendar()
    if not raw_events:
        print("No active events found in current calendar window stream.")
        return False
        
    engine = get_db_engine()
    
    # Ensure structural schema matches the data elements perfectly
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.earnings_calendar (
                ticker VARCHAR(20),
                company_name VARCHAR(150),
                purpose VARCHAR(150),
                board_meeting_date DATE,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, board_meeting_date)
            );
        """))
        
    matched_records = []
    
    # Process each row dynamically parsed out of the exchange payload
    for item in raw_events:
        purpose = item.get("purpose", "")
        
        # Isolate entries detailing upcoming financial results disclosures
        if not purpose or "RESULTS" not in purpose.upper():
            continue
            
        ticker = item.get("symbol", "").strip()
        company_name = item.get("companyName", "").strip()
        date_str = item.get("date", "")  # Maps to text string e.g., "06-Aug-2026"
        
        try:
            board_meeting_date = datetime.strptime(date_str, "%d-%b-%Y").date()
        except Exception:
            continue
            
        matched_records.append({
            "ticker": ticker,
            "company_name": company_name,
            "purpose": purpose,
            "board_meeting_date": board_meeting_date
        })
        
    if matched_records:
        with engine.begin() as conn:
            for rec in matched_records:
                conn.execute(text("""
                    INSERT INTO gold.earnings_calendar (ticker, company_name, purpose, board_meeting_date, last_updated)
                    VALUES (:ticker, :company_name, :purpose, :board_meeting_date, CURRENT_TIMESTAMP)
                    ON CONFLICT (ticker, board_meeting_date) DO UPDATE SET
                        purpose = EXCLUDED.purpose,
                        company_name = EXCLUDED.company_name,
                        last_updated = CURRENT_TIMESTAMP;
                """), rec)
        print(f"Successfully scraped and stored {len(matched_records)} corporate announcement markers.")
        return True
        
    print("Execution complete: No upcoming financial calendar events tracked in this execution window.")
    return False

if __name__ == "__main__":
    ingest_calendar_to_db()