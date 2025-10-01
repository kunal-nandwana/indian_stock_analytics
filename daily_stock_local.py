import os
import pandas as pd
from nselib import capital_market
import yfinance as yf
from datetime import datetime, date, timedelta
import sqlalchemy
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

def sanitize_column_names(columns):
    return [
        str(col).strip().lower().replace(" ", "").replace("-", "_")
        .replace(".", "").replace("%", "percent")
        for col in columns
    ]

def transform_yfinance_to_nse_format(df_yf, symbol):
    """
    Transform yfinance data to NSE format with all 15 columns
    """
    if df_yf.empty:
        return pd.DataFrame()
    
    # Reset index to get Date as column
    df_yf = df_yf.reset_index()
    
    # Create NSE format DataFrame
    nse_df = pd.DataFrame()
    
    # Map yfinance columns to NSE columns
    nse_df['symbol'] = symbol
    nse_df['series'] = 'EQ'
    nse_df['date'] = df_yf['Date'].dt.strftime('%Y-%m-%d')
    nse_df['prevclose'] = df_yf['Close'].shift(1)
    nse_df['openprice'] = df_yf['Open']
    nse_df['highprice'] = df_yf['High']
    nse_df['lowprice'] = df_yf['Low']
    nse_df['lastprice'] = df_yf['Close']
    nse_df['closeprice'] = df_yf['Close']
    nse_df['averageprice'] = (df_yf['High'] + df_yf['Low'] + df_yf['Close']) / 3
    nse_df['totaltradedquantity'] = df_yf['Volume']
    nse_df['turnoverinrs'] = df_yf['Volume'] * nse_df['averageprice']
    
    # Estimate missing NSE columns
    # Estimate nooftrades based on volume and volatility
    daily_volatility = (nse_df['highprice'] - nse_df['lowprice']) / nse_df['closeprice']
    base_trades = 100
    volume_factor = (nse_df['totaltradedquantity'] / 10000).clip(0, 5000)
    volatility_factor = (daily_volatility * 1000).clip(0, 2000)
    nse_df['nooftrades'] = (base_trades + volume_factor + volatility_factor).round().astype('Int64')
    
    # Estimate deliverableqty as ~45-47% of volume
    delivery_ratio = 0.45 + (nse_df['closeprice'] % 10) * 0.002
    nse_df['deliverableqty'] = (nse_df['totaltradedquantity'] * delivery_ratio).round().astype('Int64')
    
    # Calculate percentdlyqttotradedqty
    nse_df['percentdlyqttotradedqty'] = ((nse_df['deliverableqty'] / nse_df['totaltradedquantity']) * 100).round(2)
    
    # Drop rows with NaN prevclose (first row)
    nse_df = nse_df.dropna(subset=['prevclose'])
    
    return nse_df

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
    """
    Hybrid API approach: Try nselib first, fallback to yfinance if it fails
    """
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
            from_period_nse = max_date.strftime('%d-%m-%Y')  # nselib format
            from_period_yf = max_date.strftime('%Y-%m-%d')   # yfinance format
            
            # Smart buffer: If asking for very recent data (<=3 days), extend range for better API reliability
            days_diff = (date.today() - max_date).days
            if days_diff <= 3:
                # Extend to at least 7 days for better API success rate
                buffer_date = date.today() - timedelta(days=7)
                from_period_nse = buffer_date.strftime('%d-%m-%Y')
                from_period_yf = buffer_date.strftime('%Y-%m-%d')
                print(f"[{symbol}] Using 7-day buffer (was {days_diff} days) for API reliability")
        else:
            from_period_nse = '01-01-2013'
            from_period_yf = '2013-01-01'
        
        to_period_nse = date.today().strftime('%d-%m-%Y')
        to_period_yf = date.today().strftime('%Y-%m-%d')

        # Try nselib first (primary API)
        try:
            print(f"[{symbol}] Trying nselib: {from_period_nse} -> {to_period_nse}")
            data = capital_market.price_volume_and_deliverable_position_data(
                symbol=symbol,
                from_date=from_period_nse,
                to_date=to_period_nse
            )

            df = pd.DataFrame(data)
            if not df.empty:
                # Keep only EQ series if Series column exists
                if 'Series' in df.columns:
                    df = df[df['Series'] == 'EQ']
                    if df.empty:
                        raise Exception("No EQ series data")

                # Clean up symbol columns with better error handling
                try:
                    # First, find and remove ALL existing symbol-related columns
                    cols_to_drop = []
                    for col in df.columns:
                        if "symbol" in col.lower():
                            cols_to_drop.append(col)
                            print(f"[{symbol}] Removing existing symbol column: {col}")
                                
                    if cols_to_drop:
                        df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
                        print(f"[{symbol}] Dropped {len(cols_to_drop)} symbol columns")
                except Exception as cleanup_error:
                    print(f"[{symbol}] Warning: Column cleanup failed: {cleanup_error}, proceeding anyway...")

                # Now add the correct symbol column at the beginning
                df.insert(0, 'symbol', symbol)
                print(f"[{symbol}] Added clean symbol column")

                # Sanitize columns and save with better error handling
                df.columns = sanitize_column_names(df.columns)
                try:
                    for col in df.columns:
                        if 'date' in col.lower():
                            # Keep date columns as-is
                            continue
                        try:
                            df[col] = df[col].astype(str)
                        except Exception as col_convert_error:
                            print(f"[{symbol}] Warning: Could not convert column {col} to string: {col_convert_error}")
                            # Try alternative conversion
                            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else '')
                except Exception as convert_error:
                    print(f"[{symbol}] Warning: Column conversion failed: {convert_error}, using defaults...")

                output_file = os.path.join(output_dir, f"{symbol}.csv")
                df.to_csv(output_file, index=False)
                return f"[{symbol}] ✅ nselib: Saved {output_file} ({len(df)} rows)"
            else:
                raise Exception("Empty data from nselib")
                
        except Exception as nse_error:
            # Fallback to yfinance - handle specific nselib bugs
            error_msg = str(nse_error)
            if "Can only use .str accessor with string values" in error_msg:
                print(f"[{symbol}] nselib has data type bug, using yfinance...")
            else:
                print(f"[{symbol}] nselib failed: {nse_error}, trying yfinance...")
            
            yf_symbol = f"{symbol}.NS"
            ticker = yf.Ticker(yf_symbol)
            
            # Get data from yfinance
            df_yf = ticker.history(
                start=from_period_yf,
                end=to_period_yf,
                auto_adjust=False,
                prepost=False
            )
            
            if df_yf.empty:
                # Try shorter range if no data
                df_yf = ticker.history(
                    start="2020-01-01",
                    end=to_period_yf,
                    auto_adjust=False,
                    prepost=False
                )
            
            if df_yf.empty:
                return f"[{symbol}] ❌ Both APIs failed: nselib='{nse_error}', yfinance='No data'"
            
            # Transform yfinance data to NSE format
            nse_df = transform_yfinance_to_nse_format(df_yf, symbol)
            
            if nse_df.empty:
                return f"[{symbol}] ❌ yfinance: No valid data after transformation"
            
            # Sanitize columns and save
            nse_df.columns = sanitize_column_names(nse_df.columns)
            for col in nse_df.columns:
                if 'date' in col.lower():
                    continue  # Keep date as-is
                nse_df[col] = nse_df[col].astype(str)
            
            output_file = os.path.join(output_dir, f"{symbol}.csv")
            nse_df.to_csv(output_file, index=False)
            return f"[{symbol}] 🔄 yfinance: Saved {output_file} ({len(nse_df)} rows, estimated columns)"
            
    except Exception as e:
        return f"[{symbol}] ❌ Hybrid failed: {e}"

def main():
    print("🚀 Starting HYBRID API data fetching with SMART BUFFER...")
    print("✅ Features:")
    print("   - Primary: nselib (100% accurate NSE data)")
    print("   - Fallback: yfinance (estimated NSE columns)")
    print("   - Incremental loading from last DB date")
    print("   - SMART BUFFER: Extends very short date ranges (≤3 days) to 7 days")
    print("   - Automatic API switching on failures")
    print("-" * 60)
    
    results = []
    success_nse = 0
    success_yf = 0
    failed = 0
    
    max_workers = min(20, max(4, len(company_symbols)//10 or 4))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(process_symbol, symbol, engine, output_dir): symbol
            for symbol in company_symbols
        }
        for future in as_completed(future_map):
            result = future.result()
            print(result)
            
            if "✅ nselib" in result:
                success_nse += 1
            elif "🔄 yfinance" in result:
                success_yf += 1
            else:
                failed += 1
        1
    print("-" * 60)
    print(f"📊 Hybrid API Summary:")
    print(f"   ✅ nselib success: {success_nse}")
    print(f"   🔄 yfinance fallback: {success_yf}")
    print(f"   ❌ Both failed: {failed}")
    print(f"   📦 Total: {len(company_symbols)}")
    print(f"   🎯 Overall success: {success_nse + success_yf}/{len(company_symbols)} ({((success_nse + success_yf)/len(company_symbols)*100):.1f}%)")
    print("🎉 Hybrid API fetching completed!")

if __name__ == "__main__":
    main()