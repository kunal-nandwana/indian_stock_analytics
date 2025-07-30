import functions_framework
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import random
import time
from google.cloud import storage
from io import BytesIO
from datetime import datetime

# GCS bucket name
BUCKET_NAME = "indian_stock_analytics"

# Map section display names to folder & filenames
sections = {
    "Profit & Loss": "profit_loss",
    "Balance Sheet": "balance_sheet",
    "Cash Flows": "cash_flow",
    "Quarterly Results": "quarterly",
    "Shareholding Pattern": "shareholding",
    "Ratios": "company_ratio"
}

current_date = datetime.now().strftime("%Y-%m-%d")
root_folder = f"financial/{current_date}"

base_url = "https://www.screener.in/company/{}/consolidated/"
storage_client = storage.Client()

def sanitize_column_name(name):
    name = re.sub(r"[^\w\s]", "", name)
    name = name.lower().strip().replace(" ", "_")
    return name

def convert_object_columns_to_numeric(df):
    for col in df.select_dtypes(include='object').columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def upload_to_gcs_parquet(df, section_folder, company, section_filename):
    """
    Upload DataFrame as Parquet to GCS:
    BUCKET/section_folder/company/section_filename.parquet
    """
    destination_path = f"{root_folder}/{section_folder}/{company}/{section_filename}.parquet"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_path)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type="application/octet-stream")
    print(f"✅ Uploaded to gs://{BUCKET_NAME}/{destination_path}")

def scrape_sections(company):
    url = base_url.format(company)
    print(f"\n🔗 Fetching data for {company}: {url}")

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Error fetching {company}: {e}")
        time.sleep(random.uniform(5, 10))
        return

    soup = BeautifulSoup(response.text, "html.parser")

    for heading_text, section_folder in sections.items():
        heading = soup.find(lambda tag: tag.name in ["h2", "h4"] and heading_text in tag.text)
        if not heading:
            print(f"❌ Section '{heading_text}' not found for {company}")
            continue

        table = heading.find_next("table", {"class": "data-table"})
        if not table:
            print(f"❌ Table not found for '{heading_text}' for {company}")
            continue

        try:
            df = pd.read_html(str(table))[0]
        except Exception as e:
            print(f"⚠️ Failed to parse table in '{heading_text}' for {company}: {e}")
            continue

        if df.empty or len(df.columns) < 2:
            print(f"⚠️ Not enough data in '{heading_text}' for {company}")
            continue

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

        # 🔁 Convert object columns to numeric
        df = convert_object_columns_to_numeric(df)

        # ➕ Add company_name column
        df["company_name"] = company

        # ✅ Upload as Parquet
        upload_to_gcs_parquet(df, section_folder, company, section_folder)

    time.sleep(random.uniform(2, 4))

@functions_framework.http
def scrape_company_data(request):
    """
    HTTP Cloud Function entry point.

    Scrapes data for a fixed set of companies and stores them as Parquet in GCS.
    """
    companies = [
        "TCS", "INFY", "RELIANCE", "HDFCBANK", "HDFC", "ICICIBANK", "KOTAKBANK", "SBIN", "AXISBANK",
        "LT", "ITC", "BAJFINANCE", "HINDUNILVR", "MARUTI", "BHARTIARTL", "ASIANPAINT", "BAJAJ-AUTO",
        "NESTLEIND", "ULTRACEMCO", "TITAN", "TECHM", "WIPRO", "HCLTECH", "ONGC", "POWERGRID", "NTPC",
        "IOC", "CIPLA", "DRREDDY", "GRASIM", "M&M", "EICHERMOT", "HDFCLIFE", "TATASTEEL", "JSWSTEEL",
        "BRITANNIA", "ADANIGREEN", "SUNPHARMA", "DIVISLAB", "BPCL", "HEROMOTOCO", "UPL", "LUPIN",
        "TATAMOTORS", "VEDL", "COALINDIA", "INDUSINDBK", "BAJAJFINSV", "SHREECEM", "TATACONSUM",
        "GAIL", "COLPAL", "MUTHOOTFIN", "TATAPOWER", "DLF", "IDFCFIRSTB", "ZOMATO", "INDIGO", "BOSCHLTD",
        "SBILIFE", "PAGEIND", "BANKBARODA", "AUROPHARMA", "NMDC", "SRF", "PETRONET", "CROMPTON",
        "GODREJCP", "ACC", "AMBUJACEM", "APOLLOHOSP", "CANBK", "MGL", "PVR", "HAVELLS", "VOLTAS",
        "FEDERALBNK", "BERGEPAINT", "LICHSGFIN", "HINDPETRO", "ADANIPORTS", "EXIDEIND", "BANDHANBNK",
        "ICICIPRULI", "SIEMENS", "HINDALCO", "TATAELXSI", "BHARATFORG", "GODREJPROP", "BANKINDIA",
        "UBL", "IDBI", "IGL", "MINDTREE", "MCDOWELL-N", "ADANITRANS", "BIOCON", "CASTROLIND", "HDFCAMC",
        "INDUSTOWER", "PIIND", "CANFINHOME", "TRENT", "TORNTPHARM", "BHEL", "TATAINVEST", "IRCTC",
        "GMRINFRA", "TATACHEM", "MCX", "INDHOTEL", "WHIRLPOOL", "SANOFI", "MRF", "GODREJIND",
        "CUMMINSIND", "AUBANK", "GLAXO", "CONCOR", "ICICIGI", "POLYCAB", "NIITTECH", "LTI", "JUBILANT",
        "SRTRANSFIN", "PIDILITIND", "BALKRISIND", "MARICO", "INDIAMART", "BATAINDIA", "ALKEM",
        "SHRIRAMFIN", "MOTHERSUMI", "PEL", "BAJAJHLDNG", "CUB", "HINDZINC", "NAVINFLUOR", "TORNTPOWER",
        "VBL", "JINDALSTEL", "CADILAHC", "DABUR", "ASHOKLEY", "IBULHSGFIN", "AMARAJABAT", "GLENMARK",
        "IDFC", "ADANIENT", "ADANIPOWER", "DMART", "RAJESHEXPO", "NAM-INDIA", "PGHH", "GILLETTE",
        "AWL", "DIXON", "KAJARIACER", "HONAUT", "RELAXO", "BLUESTARCO", "ABBOTIND", "PFIZER",
        "ASTRAZEN", "ERIS", "LALPATHLAB", "MEDANTA", "FORTIS", "UNITDSPR", "RADICO", "JIOFIN",
        "ANGELONE", "BAJAJHFL", "TEAMLEASE", "QUESS"
    ]

    for company in companies:
        scrape_sections(company)

    return f"✅ Scraped and uploaded Parquet data for {len(companies)} companies", 200
