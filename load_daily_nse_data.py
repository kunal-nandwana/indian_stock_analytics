import os
import glob
import pandas as pd
import time
import sqlalchemy
import shutil
from datetime import date

# --- Configuration ---
PG_USER = os.environ.get('DATABASE_USER', 'kunal.nandwana')
PG_PASS = os.environ.get('DATABASE_PASSWORD', 'root')
PG_HOST = os.environ.get('DATABASE_HOST', 'localhost')
PG_PORT = os.environ.get('DATABASE_PORT', '5432')
PG_DB   = os.environ.get('DATABASE_NAME', 'kunal.nandwana')
PG_SCHEMA = 'bronze'
PG_TABLE  = 'daily_nse_data'
STAGING_TABLE = 'daily_nse_data_staging'

# Path to CSV files
data_path = os.environ.get("STOCK_DATA_INPUT_DIR", "/Users/kunal.nandwana/daily_data") + "/*.csv"
csv_files = glob.glob(data_path)

# Path to CSV files
data_dir = os.environ.get("STOCK_DATA_INPUT_DIR", "/Users/kunal.nandwana/daily_data")
data_path = data_dir + "/*.csv"

print(f"[DEBUG] Looking for CSV files in: {data_path}")
try:
    all_files = os.listdir(data_dir)
    print(f"[DEBUG] All files in directory: {all_files}")
except Exception as e:
    print(f"[ERROR] Could not list directory contents: {e}")

# Check if directory exists and is accessible
if not os.path.isdir(data_dir):
    print(f"[ERROR] Directory does not exist or is not accessible: {data_dir}")
    exit(1)

csv_files = glob.glob(data_path)
print(f"[DEBUG] Files found: {csv_files}")

# --- 1. Load and Clean Data ---
df_list = []
for file in csv_files:
    try:
        df = pd.read_csv(file)
        filename = os.path.basename(file)
        symbol_from_filename = os.path.splitext(filename)[0].upper()

        if 'symbol' not in df.columns:
            df['symbol'] = symbol_from_filename
        
        df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
        df = df[df['symbol'].notna() & (df['symbol'] != '') & (df['symbol'] != 'NAN')]
        
        if len(df) > 0:
            df_list.append(df)
            print(f"✅ Loaded {filename}: {len(df)} rows.")
    except Exception as e:
        print(f"❌ Error loading {file}: {e}")

if not df_list:
    print("No CSV files found.")
    exit(0)

full_df = pd.concat(df_list, ignore_index=True)

# --- 2. Standardize Columns & Types ---
full_df.columns = [c.lower() for c in full_df.columns]
col_map = {
    'symbol': 'symbol', 'date': 'date', 'prevclose': 'prevclose',
    'openprice': 'openprice', 'highprice': 'highprice', 'lowprice': 'lowprice',
    'lastprice': 'lastprice', 'closeprice': 'closeprice', 'averageprice': 'averageprice',
    'totaltradedquantity': 'totaltradedquantity', 'turnoverinrs': 'turnoverinrs',
    'nooftrades': 'nooftrades', 'deliverableqty': 'deliverableqty', 
    'percentdlyqttotradedqty': 'percentdlyqttotradedqty'
}
full_df = full_df.rename(columns=col_map)

# Convert numeric columns
numeric_cols = ['prevclose', 'openprice', 'highprice', 'lowprice', 'lastprice', 'closeprice', 'averageprice', 'totaltradedquantity', 'turnoverinrs', 'nooftrades', 'deliverableqty', 'percentdlyqttotradedqty']
for col in numeric_cols:
    if col in full_df.columns:
        full_df[col] = full_df[col].astype(str).str.replace(',', '', regex=False)
        full_df[col] = pd.to_numeric(full_df[col], errors='coerce')


# Convert and filter date column
if 'date' in full_df.columns:
    full_df['date'] = pd.to_datetime(full_df['date'], errors='coerce').dt.date
    before = len(full_df)
    full_df = full_df[full_df['date'].notna()]
    after = len(full_df)
    print(f"[INFO] Dropped {before - after} rows with null date.")

full_df = full_df.sort_values("turnoverinrs", ascending=False).drop_duplicates(subset=["symbol", "date"], keep="first")

# --- 3. Database Operations ---
db_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = sqlalchemy.create_engine(db_url)

print(f"\n📊 Final Row Count: {len(full_df)}")

# Step A: Load to Staging
print(f"⏳ Loading staging table: {PG_SCHEMA}.{STAGING_TABLE}...")

full_df.to_sql(
    STAGING_TABLE, 
    engine, 
    if_exists='replace', 
    index=False, 
    schema=PG_SCHEMA
)

print("✅ Staging load complete.")

# Step B: Perform Upsert (Merge)
merge_sql = f'''
INSERT INTO {PG_SCHEMA}.{PG_TABLE} 
    (symbol, date, prevclose, openprice, highprice, lowprice, lastprice, closeprice, averageprice, totaltradedquantity, turnoverinrs, nooftrades, deliverableqty, percentdlyqttotradedqty)
SELECT 
    symbol, date, prevclose, openprice, highprice, lowprice, lastprice, closeprice, averageprice, totaltradedquantity, turnoverinrs, nooftrades, deliverableqty, percentdlyqttotradedqty
FROM {PG_SCHEMA}.{STAGING_TABLE}
ON CONFLICT (symbol, date) DO UPDATE SET
    prevclose               = EXCLUDED.prevclose,
    openprice               = EXCLUDED.openprice,
    highprice               = EXCLUDED.highprice,
    lowprice                = EXCLUDED.lowprice,
    lastprice               = EXCLUDED.lastprice,
    closeprice              = EXCLUDED.closeprice,
    averageprice            = EXCLUDED.averageprice,
    totaltradedquantity     = EXCLUDED.totaltradedquantity,
    turnoverinrs            = EXCLUDED.turnoverinrs,
    nooftrades              = EXCLUDED.nooftrades,
    deliverableqty          = EXCLUDED.deliverableqty,
    percentdlyqttotradedqty = EXCLUDED.percentdlyqttotradedqty;
'''

with engine.begin() as conn:
    print(f"⏳ Upserting into final table: {PG_SCHEMA}.{PG_TABLE}...")
    conn.execute(sqlalchemy.text(merge_sql))
    conn.execute(sqlalchemy.text(f"DROP TABLE IF EXISTS {PG_SCHEMA}.{STAGING_TABLE};"))

print("✅ Database upsert complete.")

# --- 4. Delete Processed Files ---
print(f"\n🧹 Cleaning up processed files...")
for file in csv_files:
    try:
        os.remove(file)
        print(f"🗑️  Deleted: {os.path.basename(file)}")
    except Exception as e:
        print(f"❌ Failed to delete {file}: {e}")

print("\n🚀 Pipeline Finished Successfully.")