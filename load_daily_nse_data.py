import os
import glob
import pandas as pd
import sqlalchemy

# PostgreSQL connection details
PG_USER = os.environ.get('DATABASE_USER', 'kunal.nandwana')
PG_PASS = os.environ.get('DATABASE_PASSWORD', 'root')
PG_HOST = os.environ.get('DATABASE_HOST', 'localhost')
PG_PORT = os.environ.get('DATABASE_PORT', '5432')
PG_DB   = os.environ.get('DATABASE_NAME', 'kunal.nandwana')
PG_SCHEMA = 'bronze'
PG_TABLE  = 'daily_nse_data'

# Path to CSV files
data_path = "/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/Data/Indian Stock Analytics/daily_data/*.csv"

# Get all CSV files
csv_files = glob.glob(data_path)

# Read and concatenate all CSVs
df_list = []
for file in csv_files:
    try:
        df = pd.read_csv(file)
        
        # Validate and clean symbol columns
        if 'symbol' not in df.columns:
            print(f"⚠️  Skipping {os.path.basename(file)}: No symbol column")
            continue
            
        # Remove rows where symbol is null or empty
        initial_rows = len(df)
        df = df[df['symbol'].notna() & (df['symbol'] != '') & (df['symbol'] != 'nan')]
        
        if len(df) == 0:
            print(f"⚠️  Skipping {os.path.basename(file)}: No valid symbol data")
            continue
            
        if len(df) < initial_rows:
            print(f"⚠️  {os.path.basename(file)}: Removed {initial_rows - len(df)} rows with invalid symbols")
        
        df_list.append(df)
        print(f"✅ Loaded {os.path.basename(file)}: {len(df)} rows")
        
    except Exception as e:
        print(f"❌ Error loading {os.path.basename(file)}: {e}")
        continue

if not df_list:
    print("No CSV files found.")
    exit(0)

full_df = pd.concat(df_list, ignore_index=True)

# Rename columns to match DDL (if needed)
col_map = {
    'symbol': 'symbol',
    'series': 'series',
    'date': 'date',
    'prevclose': 'prevclose',
    'openprice': 'openprice',
    'highprice': 'highprice',
    'lowprice': 'lowprice',
    'lastprice': 'lastprice',
    'closeprice': 'closeprice',
    'averageprice': 'averageprice',
    'totaltradedquantity': 'totaltradedquantity',
    'turnoverinrs': 'turnoverinrs',
    'nooftrades': 'nooftrades',
    'deliverableqty': 'deliverableqty',
    'percentdlyqttotradedqty': 'percentdlyqttotradedqty',
}
full_df.columns = [c.lower() for c in full_df.columns]
full_df = full_df.rename(columns=col_map)

# Ensure correct dtypes for numeric columns
numeric_cols = [
    'prevclose', 'openprice', 'highprice', 'lowprice', 'lastprice', 'closeprice',
    'averageprice', 'totaltradedquantity', 'turnoverinrs', 'nooftrades',
    'deliverableqty', 'percentdlyqttotradedqty'
]

# Remove commas and convert numeric columns
for col in numeric_cols:
    if col in full_df.columns:
        full_df[col] = full_df[col].astype(str).str.replace(',', '', regex=False)
        full_df[col] = pd.to_numeric(full_df[col], errors='coerce')

# Convert date column to datetime.date
if 'date' in full_df.columns:
    full_df['date'] = pd.to_datetime(full_df['date'], errors='coerce').dt.date

# --- Final validation before database insertion ---
print(f"\n📊 Final data validation:")
print(f"   Total rows before validation: {len(full_df)}")

# Remove any remaining rows with null symbols
null_symbols = full_df['symbol'].isna().sum()
empty_symbols = (full_df['symbol'] == '').sum()
if null_symbols > 0 or empty_symbols > 0:
    print(f"   ⚠️  Found {null_symbols} null + {empty_symbols} empty symbols, removing...")
    full_df = full_df[full_df['symbol'].notna() & (full_df['symbol'] != '')]

print(f"   Total rows after validation: {len(full_df)}")
print(f"   Unique symbols: {full_df['symbol'].nunique()}")

if len(full_df) == 0:
    print("❌ No valid data to load after validation!")
    exit(1)

# --- Deduplicate before loading ---
# Keep only the row with the highest turnover for each (symbol, date)
full_df = (
    full_df.sort_values("turnoverinrs", ascending=False)
           .drop_duplicates(subset=["symbol", "date"], keep="first")
)

# Connect to PostgreSQL
engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# --- UPSERT (MERGE) LOGIC ---
STAGING_TABLE = 'daily_nse_data_staging'

# 1. Load to staging table (replace)
full_df.to_sql(STAGING_TABLE, engine, if_exists='replace', index=False, schema=PG_SCHEMA)

# 2. Upsert from staging to main table
merge_sql = f'''
INSERT INTO {PG_SCHEMA}.{PG_TABLE} AS target
    (symbol, date, prevclose, openprice, highprice, lowprice, lastprice, closeprice, averageprice, totaltradedquantity, turnoverinrs, nooftrades, deliverableqty, percentdlyqttotradedqty)
SELECT symbol, date, prevclose, openprice, highprice, lowprice, lastprice, closeprice, averageprice, totaltradedquantity, turnoverinrs, nooftrades, deliverableqty, percentdlyqttotradedqty
FROM {PG_SCHEMA}.{STAGING_TABLE}
ON CONFLICT (symbol, date) DO UPDATE SET
    prevclose              = EXCLUDED.prevclose,
    openprice              = EXCLUDED.openprice,
    highprice              = EXCLUDED.highprice,
    lowprice               = EXCLUDED.lowprice,
    lastprice              = EXCLUDED.lastprice,
    closeprice             = EXCLUDED.closeprice,
    averageprice           = EXCLUDED.averageprice,
    totaltradedquantity    = EXCLUDED.totaltradedquantity,
    turnoverinrs           = EXCLUDED.turnoverinrs,
    nooftrades             = EXCLUDED.nooftrades,
    deliverableqty         = EXCLUDED.deliverableqty,
    percentdlyqttotradedqty= EXCLUDED.percentdlyqttotradedqty;
'''

with engine.begin() as conn:
    conn.execute(sqlalchemy.text(merge_sql))


# Move processed files to archive directory with current date
import shutil
from datetime import date

archive_base = "/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/Data/Indian Stock Analytics/archive"
current_date = date.today().strftime('%d-%m-%Y')
archive_dir = os.path.join(archive_base, current_date)
os.makedirs(archive_dir, exist_ok=True)

for file in csv_files:
    try:
        shutil.move(file, archive_dir)
        print(f"Moved {file} to {archive_dir}")
    except Exception as e:
        print(f"Failed to move {file}: {e}")

print(f"Upserted {len(full_df)} rows into {PG_SCHEMA}.{PG_TABLE} table and archived files.")
