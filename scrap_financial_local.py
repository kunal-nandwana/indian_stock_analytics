import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import random
import re
import sqlalchemy
from sqlalchemy import text

# Configuration
base_output_dir = os.environ.get('FINANCIAL_DATA_OUTPUT_DIR', '/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/Data/Indian Stock Analytics/financial_data')
os.makedirs(base_output_dir, exist_ok=True)
print(f"Using financial data output directory: {base_output_dir}")

session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0"
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
            df = pd.read_html(str(table))[0]
        except Exception as e:
            print(f"⚠️ Failed to parse table in '{heading_text}' for {company}: {e}")
            failed_sections.append(heading_text)
            continue

        if df.empty or len(df.columns) < 2:
            print(f"⚠️ Not enough data in '{heading_text}' for {company} ({url_type} URL)")
            failed_sections.append(heading_text)
            continue
            
        # Check if we got incomplete data (fewer than 3 years of data)
        # Most companies should have at least 3-4 years of historical data
        year_columns = [col for col in df.columns if 'Mar' in str(col) or any(year in str(col) for year in ['2021', '2022', '2023', '2024', '2025'])]
        
        # Check which year columns actually have data (not just NaN/empty values)
        valid_year_columns = []
        for col in year_columns:
            non_empty_count = df[col].dropna().count()
            if non_empty_count > 0:
                valid_year_columns.append(col)
        
        print(f"📊 Found {len(year_columns)} year columns for '{heading_text}' in {url_type} URL: {year_columns}")
        print(f"📊 But only {len(valid_year_columns)} have actual data: {valid_year_columns}")
        
        if len(valid_year_columns) < 3 and url_type == "consolidated":
            print(f"⚠️ Incomplete data in '{heading_text}' for {company} - only {len(valid_year_columns)} years with actual data in consolidated URL")
            failed_sections.append(heading_text)
            continue

        # Process data same as working version
        if heading_text == "Quarterly Results":
            # Transpose like profit_loss
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

    # If we got some failed sections with consolidated, try standalone for those
    if failed_sections and url_type == "consolidated":
        print(f"🔄 Trying standalone URL for failed/incomplete sections: {failed_sections}")
        
        url = base_url_standalone.format(company)
        try:
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            standalone_soup = BeautifulSoup(response.text, "html.parser")
            print(f"🔗 Fetching additional data for {company} (standalone): {url}")
            
            for heading_text in failed_sections:
                section_filename = sections[heading_text]
                
                heading = standalone_soup.find(lambda tag: tag.name in ["h2", "h4"] and heading_text in tag.text)
                if not heading:
                    print(f"❌ Section '{heading_text}' not found for {company} (standalone)")
                    continue

                table = heading.find_next("table", {"class": "data-table"})
                if not table:
                    print(f"❌ Table not found for '{heading_text}' for {company} (standalone)")
                    continue

                try:
                    df = pd.read_html(str(table))[0]
                except Exception as e:
                    print(f"⚠️ Failed to parse table in '{heading_text}' for {company} (standalone): {e}")
                    continue

                if df.empty or len(df.columns) < 2:
                    print(f"⚠️ Still not enough data in '{heading_text}' for {company} (standalone URL)")
                    continue
                    
                # Check if standalone has more complete data
                year_columns = [col for col in df.columns if 'Mar' in str(col) or any(year in str(col) for year in ['2021', '2022', '2023', '2024', '2025'])]
                
                # Check which year columns actually have data (not just NaN/empty values)
                valid_year_columns = []
                for col in year_columns:
                    non_empty_count = df[col].dropna().count()
                    if non_empty_count > 0:
                        valid_year_columns.append(col)
                
                print(f"📊 Standalone URL has {len(year_columns)} year columns for '{heading_text}': {year_columns}")
                print(f"📊 With {len(valid_year_columns)} columns having actual data: {valid_year_columns}")

                # Process data same as working version
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
                
        except requests.RequestException as e:
            print(f"❌ Error fetching {company} (standalone): {e}")

    print(f"📊 Processed {sections_processed}/6 sections for {company}")
    
    # Add 5 second sleep as requested
    wait = 5.0  
    print(f"🕒 Waiting {wait:.1f}s before next company...\n")
    time.sleep(wait)

# Database configuration (consistent with daily_stock_local.py)
PG_USER = os.environ.get('DATABASE_USER', 'kunal.nandwana')
PG_PASS = os.environ.get('DATABASE_PASSWORD', 'root')
PG_HOST = os.environ.get('DATABASE_HOST', 'localhost')
PG_PORT = os.environ.get('DATABASE_PORT', '5432')
PG_DB   = os.environ.get('DATABASE_NAME', 'kunal.nandwana')

if 'DATABASE_URL' in os.environ:
    connection_string = os.environ['DATABASE_URL']
else:
    connection_string = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

print(f"Connecting to database: {PG_HOST}:{PG_PORT} as {PG_USER}")
engine = sqlalchemy.create_engine(connection_string)

# Fetch companies from database instead of hardcoded list
try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT symbol FROM bronze.equities_list ORDER BY (date_of_listing::date) DESC"))
        companies = [row[0] for row in result.fetchall()]
        print(f"📈 Fetched {len(companies)} companies from bronze.equities_list")
except Exception as e:
    print(f"❌ Error fetching companies from database: {e}")
    # Fallback to sample companies for testing
    companies = ["TCS", "INFY", "RELIANCE", "HDFCBANK", "HDFC"]
    print(f"🔄 Using fallback sample companies: {companies}")

print(f"🚀 Starting to scrape financial data for {len(companies)} companies...")
print(f"📁 Output directory: {base_output_dir}")
print(f"⏰ With 5 second delay between companies for monitoring")

for i, company in enumerate(companies, 1):
    print(f"\n{'='*60}")
    print(f"Processing {i}/{len(companies)}: {company}")
    print(f"{'='*60}")
    scrape_sections(company)

print(f"\n✅ All done! Processed {len(companies)} companies")