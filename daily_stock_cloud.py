import os
import pandas as pd
from nselib import capital_market
from google.cloud import storage
from datetime import date, timedelta
import tempfile
import time
from companies_list import all_companies
from concurrent.futures import ThreadPoolExecutor, as_completed
 
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
    numeric_cols = [c for c in df.columns if c not in ("symbol", "date", "series")]
    for col in numeric_cols:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "", regex=False),
            errors="coerce"
        )
    return df
 
def process_symbol(symbol, bucket, gcs_base_path, from_period, to_period):
    try:
        print(f"[DEBUG] Calling API for {symbol}")
        data = capital_market.price_volume_and_deliverable_position_data(
            symbol    = symbol,
            from_date = from_period,
            to_date   = to_period
        )
        df = pd.DataFrame(data)
        if df.empty or "Series" not in df.columns:
            return f"No data for {symbol}"
 
        df = df[df["Series"] == "EQ"].copy()
        if df.empty:
            return f"No EQ data for {symbol}"
 
        df.drop(columns=["Series"], inplace=True, errors="ignore")
 
        drop_syms = [c for c in df.columns if "symbol" in c.lower()]
        if drop_syms:
            df.drop(columns=drop_syms, inplace=True, errors="ignore")
        df.insert(0, "symbol", symbol)
 
        df.columns = sanitize_column_names(df.columns)
        df = clean_dataframe(df)
 
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            df.to_parquet(tmp.name, index=False)
            blob = bucket.blob(f"{gcs_base_path}/{symbol}.parquet")
            blob.upload_from_filename(tmp.name)
        os.unlink(tmp.name)
 
        return f"Uploaded {symbol} to gs://{bucket.name}/{gcs_base_path}/{symbol}.parquet"
 
    except Exception as e:
        return f"Failed for {symbol}: {e}"
 
def scrape_and_upload(request):
    bucket_name    = "indian_stock_analytics"
    storage_client = storage.Client()
    bucket         = storage_client.bucket(bucket_name)
 
 
    today = date.today()
 
    yesterday = today - timedelta(days=1)
    yesterday_formatted = yesterday.strftime("%d-%m-%Y")
 
    today = date.today()
    gcs_base_path = f"daily/{yesterday_formatted}"
 
    from_period  = yesterday_formatted
    to_period    = today.strftime("%d-%m-%Y")
    print(f"[DEBUG] Fetching data from {from_period} to {to_period}")
 
    results = []
    # Run 20 threads at a time
    with ThreadPoolExecutor(max_workers=15) as executor:
        future_to_symbol = {
            executor.submit(process_symbol, symbol, bucket, gcs_base_path, from_period, to_period): symbol
            for symbol in all_companies
        }
        for future in as_completed(future_to_symbol):
            results.append(future.result())
 
    return ("\n".join(results), 200)