import os
import pandas as pd
from nselib import capital_market
from google.cloud import storage
from datetime import date, timedelta
import tempfile

def sanitize_column_names(columns):
    return [
        col.strip()
           .lower()
           .replace(" ", "_")
           .replace("-", "_")
           .replace(".", "")
           .replace("%", "percent")
           .replace('"', "")
           .replace("ï»¿", "")
        for col in columns
    ]

def clean_dataframe(df):
    """
    Cleans and converts numeric columns.
    """
    numeric_cols = [c for c in df.columns if c not in ("symbol", "date", "series")]
    for col in numeric_cols:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "", regex=False),
            errors="coerce"
        )
    return df

def scrape_and_upload(request):
    """
    Cloud Function: Fetches NSE daily data for selected symbols,
    cleans it, and uploads as Parquet files to GCS.
    """
    bucket_name    = "indian_stock_analytics"
    storage_client = storage.Client()
    bucket         = storage_client.bucket(bucket_name)

    # Build GCS path for today
    today= date.today()
    yesterday= today - timedelta(days=1)
    current_date=yesterday.strftime("%d-%m-%Y")                # "YYYY-MM-DD"
    gcs_base_path = f"daily/{current_date}"

    # Use yesterday→today so to_date > from_date
    from_period  = yesterday.strftime("%d-%m-%Y")    # e.g. "24-07-2025"
    to_period    = today.strftime("%d-%m-%Y")        # e.g. "25-07-2025"
    print(f"[DEBUG] Fetching data from {from_period} to {to_period}")

    company_symbols = [
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

    results = []
    for symbol in company_symbols:
        try:
            print(f"[DEBUG] Calling API for {symbol}")
            data = capital_market.price_volume_and_deliverable_position_data(
                symbol    = symbol,
                from_date = from_period,
                to_date   = to_period
            )
            df = pd.DataFrame(data)
            if df.empty or "Series" not in df.columns:
                results.append(f"No data for {symbol}")
                continue

            # Keep only EQ series
            df = df[df["Series"] == "EQ"].copy()
            if df.empty:
                results.append(f"No EQ data for {symbol}")
                continue
   

            # Drop original Series column
            df.drop(columns=["Series"], inplace=True, errors="ignore")

            # Remove any existing symbol‐like columns, then add clean one
            drop_syms = [c for c in df.columns if "symbol" in c.lower()]
            if drop_syms:
                df.drop(columns=drop_syms, inplace=True, errors="ignore")
            df.insert(0, "symbol", symbol)

            # Sanitize names & convert numeric
            df.columns = sanitize_column_names(df.columns)
            df = clean_dataframe(df)

            # Write to temp Parquet & upload
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                df.to_parquet(tmp.name, index=False)
                blob = bucket.blob(f"{gcs_base_path}/{symbol}.parquet")
                blob.upload_from_filename(tmp.name)
            os.unlink(tmp.name)

            results.append(f"Uploaded {symbol} to gs://{bucket_name}/{gcs_base_path}/{symbol}.parquet")

        except Exception as e:
            results.append(f"Failed for {symbol}: {e}")

    return ("\n".join(results), 200)