import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import random
import re

session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0"
}

base_url = "https://www.screener.in/company/{}/consolidated/"

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
    url = base_url.format(company)
    print(f"\n🔗 Fetching data for {company}: {url}")

    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Error fetching {company}: {e}")
        wait = random.uniform(5, 10)
        print(f"⏳ Sleeping for {wait:.2f} seconds before retrying next...")
        time.sleep(wait)
        return

    soup = BeautifulSoup(response.text, "html.parser")
    os.makedirs(f"output/{company}", exist_ok=True)

    for heading_text, section_filename in sections.items():
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

        output_path = f"output/{company}/{section_filename}.csv"
        df.to_csv(output_path, index=False)
        print(f"✅ Saved {section_filename} for {company} → {output_path}")

    wait = random.uniform(2, 4)
    print(f"🕒 Waiting {wait:.2f}s before next company...\n")
    time.sleep(wait)

# ✅ Sample company list for testing
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

print("\n✅ All done!")
