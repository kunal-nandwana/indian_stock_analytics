

import os
import pandas as pd
from nselib import capital_market
from datetime import datetime, date, timedelta
import sqlalchemy
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed

def sanitize_column_names(columns):
    # Ensure all column names are string before applying string methods
    return [str(col).strip().lower().replace(" ","").replace("-", "_").replace(".", "").replace("%", "percent") for col in columns]



today = date.today()
current_date    = today.strftime("%d-%m-%Y")

output_dir = f"/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/Data/Indian Stock Analytics/daily_data/{current_date}"
if not os.path.exists(output_dir):
    os.makedirs(output_dir, exist_ok=True)




# Fetch company symbols from bronze.equities_list table
PG_USER = 'kunal.nandwana'
PG_PASS = 'root'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB   = 'kunal.nandwana'

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")
query = "SELECT symbol FROM bronze.equities_list ORDER BY (date_of_listing::date) DESC"
with engine.connect() as conn:
    result = conn.execute(text(query))
    company_symbols = [row[0] for row in result]


from_period = '01-01-2013'
# to_period = '22-11-2024'


def process_symbol(symbol, from_period, to_period, output_dir):
    try:
        print(f"Fetching data for {symbol}")
        data = capital_market.price_volume_and_deliverable_position_data(
            symbol=symbol,
            from_date=from_period,
            to_date=to_period
        )
        df = pd.DataFrame(data)

        # Filter only 'EQ' series
        if 'Series' in df.columns:
            df = df[df['Series'] == 'EQ']
        else:
            return f"No 'Series' column for {symbol}"

        if df.empty:
            return f"No EQ data found for {symbol}"

        cols_to_drop = []
        for col in df.columns:
            if "symbol" in col.lower():
                if col.lower() != "symbol" or df[col].isnull().all() or (df[col] == '').all():
                    cols_to_drop.append(col)
        if cols_to_drop:
            print(f"Dropping unwanted columns: {cols_to_drop}")
            df.drop(columns=cols_to_drop, inplace=True)

        # Drop any existing clean 'symbol' column before inserting ours
        if 'symbol' in df.columns:
            cols_to_drop = [col for col in df.columns if 'symbol' in col.lower()]
            if cols_to_drop:
                df.drop(columns=cols_to_drop, inplace=True, errors="ignore")
        df.insert(0, 'symbol', symbol)

        # Sanitize column names
        df.columns = sanitize_column_names(df.columns)
        # Ensure all columns are string type before any .str accessor is used
        for col in df.columns:
            df[col] = df[col].astype(str)

        # Fix .str accessor error: ensure all columns are string before using .str
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str)

        # Save CSV
        output_file = os.path.join(output_dir, f"{symbol}.csv")
        df.to_csv(output_file, index=False)
        return f"Saved: {output_file}"

    except Exception as e:
        return f"Failed for {symbol}: {str(e)}"


# Multithreading: run up to 20 threads at a time
today = date.today()
yesterday = today - timedelta(days=1)
from_period = '01-01-2013'
to_period = today.strftime('%d-%m-%Y')

results = []
with ThreadPoolExecutor(max_workers=20) as executor:
    future_to_symbol = {
        executor.submit(process_symbol, symbol, from_period, to_period, output_dir): symbol
        for symbol in company_symbols
    }
    for future in as_completed(future_to_symbol):
        result = future.result()
        print(result)
