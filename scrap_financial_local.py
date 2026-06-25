import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import random
import re
import sqlalchemy
from sqlalchemy import text
from io import StringIO  # Required for Pandas 2.2+ compatibility

# Configuration
base_output_dir = os.environ.get('FINANCIAL_DATA_OUTPUT_DIR', '/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/Data/Indian Stock Analytics/financial_data')
os.makedirs(base_output_dir, exist_ok=True)
print(f"Using financial data output directory: {base_output_dir}")

session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Use consolidated URL first, add standalone fallback if needed
base_url_consolidated = "https://www.screener.in/company/{}/consolidated/"
base_url_standalone = "https://www.screener.in/company/{}/"

sections = {
    "Profit & Loss": "profit_loss",
    "Balance Sheet": "balance_sheet", 
    "Cash Flows": "cash_flow",
    "Quarterly Results": "quarterly",
    "Shareholding Pattern": "shareholding",
    "Ratios": "company_ratio"
}

def sanitize_column_name(name):
    name = re.sub(r"[^\w\s]", "", name)
    name = name.lower().strip().replace(" ", "_")
    return name

def scrape_sections(company):
    """Scrape financial sections for a company - try consolidated first, fallback to standalone if needed"""
    
    # Try consolidated first
    url = base_url_consolidated.format(company)
    print(f"\n🔗 Fetching data for {company} (consolidated): {url}")
    
    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        url_type = "consolidated"
    except requests.RequestException as e:
        print(f"❌ Error fetching {company} (consolidated): {e}")
        
        # Fallback to standalone
        url = base_url_standalone.format(company)
        print(f"🔄 Trying standalone URL for {company}: {url}")
        
        try:
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            url_type = "standalone"
        except requests.RequestException as e:
            print(f"❌ Error fetching {company} (standalone): {e}")
            wait = random.uniform(5, 10)
            print(f"⏳ Sleeping for {wait:.2f} seconds before next...")
            time.sleep(wait)
            return

    company_output_dir = os.path.join(base_output_dir, company)
    os.makedirs(company_output_dir, exist_ok=True)
    
    sections_processed = 0
    failed_sections = []

    for heading_text, section_filename in sections.items():
        heading = soup.find(lambda tag: tag.name in ["h2", "h4"] and heading_text in tag.text)
        if not heading:
            print(f"❌ Section '{heading_text}' not found for {company}")
            failed_sections.append(heading_text)
            continue

        table = heading.find_next("table", {"class": "data-table"})
        if not table:
            print(f"❌ Table not found for '{heading_text}' for {company}")
            failed_sections.append(heading_text)
            continue

        try:
            # FIX: Wrap table HTML string in StringIO to avoid "No such file or directory" error
            df = pd.read_html(StringIO(str(table)), flavor='lxml')[0]
        except Exception as e:
            print(f"⚠️ Failed to parse table in '{heading_text}' for {company}: {e}")
            failed_sections.append(heading_text)
            continue

        if df.empty or len(df.columns) < 2:
            print(f"⚠️ Not enough data in '{heading_text}' for {company} ({url_type} URL)")
            failed_sections.append(heading_text)
            continue
            
        year_columns = [col for col in df.columns if 'Mar' in str(col) or any(year in str(col) for year in ['2021', '2022', '2023', '2024', '2025'])]
        
        valid_year_columns = []
        for col in year_columns:
            non_empty_count = df[col].dropna().count()
            if non_empty_count > 0:
                valid_year_columns.append(col)
        
        print(f"📊 Found {len(year_columns)} year columns for '{heading_text}' in {url_type} URL")
        
        if len(valid_year_columns) < 3 and url_type == "consolidated":
            print(f"⚠️ Incomplete data in '{heading_text}' for {company} - checking standalone...")
            failed_sections.append(heading_text)
            continue

        # Transpose logic
        if heading_text == "Quarterly Results":
            df.rename(columns={df.columns[0]: "metric"}, inplace=True)
            df.set_index("metric", inplace=True)
            df = df.transpose().reset_index()
            df.rename(columns={"index": "quarter"}, inplace=True)
            df.columns = [sanitize_column_name(str(col)) for col in df.columns]
        else:
            df.rename(columns={df.columns[0]: "year"}, inplace=True)
            df.set_index("year", inplace=True)
            df = df.transpose().reset_index()
            df.columns = ["year" if col == "index" else sanitize_column_name(col) for col in df.columns]

        output_path = os.path.join(company_output_dir, f"{section_filename}.csv")
        df.to_csv(output_path, index=False)
        print(f"✅ Saved {section_filename} for {company} → {output_path}")
        sections_processed += 1

    # Standalone Fallback Loop
    if failed_sections and url_type == "consolidated":
        url = base_url_standalone.format(company)
        try:
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            standalone_soup = BeautifulSoup(response.text, "html.parser")
            
            for heading_text in failed_sections:
                section_filename = sections[heading_text]
                heading = standalone_soup.find(lambda tag: tag.name in ["h2", "h4"] and heading_text in tag.text)
                if not heading: continue
                table = heading.find_next("table", {"class": "data-table"})
                if not table: continue

                try:
                    # FIX: Wrap table HTML string in StringIO
                    df = pd.read_html(StringIO(str(table)), flavor='lxml')[0]
                except: continue

                if df.empty or len(df.columns) < 2: continue
                
                # Transpose logic
                if heading_text == "Quarterly Results":
                    df.rename(columns={df.columns[0]: "metric"}, inplace=True)
                    df.set_index("metric", inplace=True)
                    df = df.transpose().reset_index()
                    df.rename(columns={"index": "quarter"}, inplace=True)
                    df.columns = [sanitize_column_name(str(col)) for col in df.columns]
                else:
                    df.rename(columns={df.columns[0]: "year"}, inplace=True)
                    df.set_index("year", inplace=True)
                    df = df.transpose().reset_index()
                    df.columns = ["year" if col == "index" else sanitize_column_name(col) for col in df.columns]

                output_path = os.path.join(company_output_dir, f"{section_filename}.csv")
                df.to_csv(output_path, index=False)
                print(f"✅ Saved {section_filename} for {company} (standalone) → {output_path}")
                sections_processed += 1
        except: pass

    print(f"📊 Processed {sections_processed}/6 sections for {company}")
    time.sleep(5.0)

# Database Execution
PG_USER = os.environ.get('DATABASE_USER', 'kunal.nandwana')
PG_PASS = os.environ.get('DATABASE_PASSWORD', 'root')
PG_HOST = os.environ.get('DATABASE_HOST', 'localhost')
PG_PORT = os.environ.get('DATABASE_PORT', '5432')
PG_DB   = os.environ.get('DATABASE_NAME', 'kunal.nandwana')
connection_string = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

engine = sqlalchemy.create_engine(connection_string)

try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT company_name FROM bronze.equities_list ORDER BY (date_of_listing::date) DESC"))
        companies = [row[0] for row in result.fetchall()]
        print(f"📈 Fetched {len(companies)} companies")
except Exception as e:
    print(f"❌ Database error: {e}")
    companies = ["TCS", "RELIANCE"]

for i, company in enumerate(companies, 1):
    print(f"\nProcessing {i}/{len(companies)}: {company}")
    scrape_sections(company)

print(f"\n🚀 Pipeline Finished Successfully.")