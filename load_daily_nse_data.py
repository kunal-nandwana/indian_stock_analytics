import os
import glob
import pandas as pd
import sqlalchemy

# PostgreSQL connection details
PG_USER = 'kunal.nandwana'
PG_PASS = 'root'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB   = 'kunal.nandwana'
PG_SCHEMA = 'bronze'
PG_TABLE = 'daily_nse_data'

# Path to CSV files
data_path = "/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/Data/Indian Stock Analytics/daily_data/*.csv"

# Get all CSV files
csv_files = glob.glob(data_path)

# Read and concatenate all CSVs
df_list = []
for file in csv_files:
    df = pd.read_csv(file)
    df_list.append(df)

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
    'averageprice', 'totaltradedquantity', 'turnoverinrs', 'nooftrades', 'deliverableqty', 'percentdlyqttotradedqty'
]

# Remove commas from all string fields before converting to numeric
for col in numeric_cols:
    if col in full_df.columns:
        # Remove commas if present
        full_df[col] = full_df[col].astype(str).str.replace(',', '', regex=False)
        full_df[col] = pd.to_numeric(full_df[col], errors='coerce')

# Convert date column to datetime.date
if 'date' in full_df.columns:
    full_df['date'] = pd.to_datetime(full_df['date'], errors='coerce').dt.date

# Connect to PostgreSQL
engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# Truncate the table before loading
with engine.connect() as conn:
    conn.execute(sqlalchemy.text(f"TRUNCATE TABLE {PG_SCHEMA}.{PG_TABLE}"))

# Load to PostgreSQL (append)
full_df.to_sql(PG_TABLE, engine, if_exists='append', index=False, schema=PG_SCHEMA)
print(f"Loaded {len(full_df)} rows into {PG_SCHEMA}.{PG_TABLE} table.")
