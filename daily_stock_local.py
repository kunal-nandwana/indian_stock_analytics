import os
import pandas as pd
from nselib import capital_market
from datetime import datetime, date
import sqlalchemy
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed

def sanitize_column_names(columns):
    return [
        str(col).strip().lower().replace(" ", "").replace("-", "_")
        .replace(".", "").replace("%", "percent")
        for col in columns
    ]

# Dynamic output directory (date folder optional). If you don’t want date subfolder, remove current_date part.
current_date = datetime.today().strftime('%Y-%m-%d')
if 'STOCK_DATA_OUTPUT_DIR' in os.environ:
    base_output_dir = os.environ['STOCK_DATA_OUTPUT_DIR'].rstrip('/ ')
    output_dir = f"{base_output_dir}"   # or f"{base_output_dir}/{current_date}"
else:
    output_dir = "/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/Data/Indian Stock Analytics/daily_data"

os.makedirs(output_dir, exist_ok=True)
print(f"Using output_dir={output_dir}")

# Database config (env override; fallback to defaults)
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
print(f"Database connection string: postgresql+psycopg2://{PG_USER}:***@{PG_HOST}:{PG_PORT}/{PG_DB}")

engine = sqlalchemy.create_engine(connection_string)

# Load symbol list once
# SYMBOL_QUERY = "with cte as(select symbol,max(date) as dates from bronze.daily_nse_data group by symbol) select symbol from cte where dates!=current_date"
SYMBOL_QUERY = "SELECT symbol FROM bronze.equities_list ORDER BY (date_of_listing::date) DESC"

with engine.connect() as conn:
    company_symbols = [row[0] for row in conn.execute(text(SYMBOL_QUERY))]
print(f"Total symbols: {len(company_symbols)}")

def process_symbol(symbol, engine, output_dir):
    try:
        # Determine from_period based on existing max(date)
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT max(date) FROM bronze.daily_nse_data WHERE symbol = :symbol"),
                {"symbol": symbol}
            )
            max_date_row = result.fetchone()
            max_date = max_date_row[0] if max_date_row and max_date_row[0] else None

        if max_date:
            from_period = max_date.strftime('%d-%m-%Y')
        else:
            from_period = '01-01-2013'
        to_period = date.today().strftime('%d-%m-%Y')

        print(f"[{symbol}] Fetching data {from_period} -> {to_period}")
        data = capital_market.price_volume_and_deliverable_position_data(
            symbol=symbol,
            from_date=from_period,
            to_date=to_period
        )

        df = pd.DataFrame(data)
        if df.empty:
            return f"[{symbol}] No data returned"

        # Keep only EQ series if Series column exists
        if 'Series' in df.columns:
            df = df[df['Series'] == 'EQ']
            if df.empty:
                return f"[{symbol}] No EQ rows"

        # Drop redundant/empty symbol-like columns, then ensure 'symbol' first
        cols_to_drop = []
        for col in df.columns:
            if "symbol" in col.lower():
                if col.lower() != "symbol" or df[col].isna().all() or (df[col] == '').all():
                    cols_to_drop.append(col)
        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

        df.insert(0, 'symbol', symbol)

        # Sanitize columns
        df.columns = sanitize_column_names(df.columns)
        for col in df.columns:
            df[col] = df[col].astype(str)

        output_file = os.path.join(output_dir, f"{symbol}.csv")
        df.to_csv(output_file, index=False)
        return f"[{symbol}] Saved {output_file}"
    except Exception as e:
        return f"[{symbol}] Failed: {e}"

def main():
    results = []
    max_workers = min(20, max(4, len(company_symbols)//10 or 4))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(process_symbol, symbol, engine, output_dir): symbol
            for symbol in company_symbols
        }
        for future in as_completed(future_map):
            print(future.result())

if __name__ == "__main__":
    main()