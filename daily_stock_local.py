import os
import pandas as pd
from nselib import capital_market
from datetime import datetime, date, timedelta

def sanitize_column_names(columns):
    return [col.strip().lower().replace(" ", "").replace("-", "_").replace(".", "").replace("%", "percent") for col in columns]

output_dir = "nse_data"
os.makedirs(output_dir, exist_ok=True)

company_symbols = ["VBL"]  # Extend as needed

# from_period = '01-01-2013'
# to_period = '22-11-2024'

for symbol in company_symbols:
    try:
        print(f"Fetching data for {symbol}")
        # Ensure date strings are in 'DD-MM-YYYY' format
        # from_date_str = pd.to_datetime(from_period).strftime('%d-%m-%Y')
        # to_date_str = pd.to_datetime(to_period).strftime('%d-%m-%Y')

        # today and yesterday for API: to_date must be greater than from_date
        today = date.today()
        yesterday = today - timedelta(days=1)
        from_period = yesterday.strftime('%d-%m-%Y')  # e.g. '24-07-2025'
        to_period = today.strftime('%d-%m-%Y')        # e.g. '25-07-2025'
        print(f"[DEBUG] Using from_date={from_period}, to_date={to_period}")
        data = capital_market.price_volume_and_deliverable_position_data(
            symbol=symbol,
            from_date=from_period,
            to_date=to_period
        )
        df = pd.DataFrame(data)

        # Filter only 'EQ' series
        df = df[df['Series'] == 'EQ']

        if df.empty:
            print(f"No EQ data found for {symbol}")
            continue

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
            # drop any existing symbol-like columns (case-insensitive)
            cols_to_drop = [col for col in df.columns if 'symbol' in col.lower()]
            if cols_to_drop:
                df.drop(columns=cols_to_drop, inplace=True, errors="ignore")
        df.insert(0, 'symbol', symbol)

        # Sanitize column names
        df.columns = sanitize_column_names(df.columns)

        # Save CSV
        output_file = os.path.join(output_dir, f"{symbol}.csv")
        df.to_csv(output_file, index=False)
        print(f"Saved: {output_file}")

    except Exception as e:
        print(f"Failed for {symbol}: {str(e)}")
