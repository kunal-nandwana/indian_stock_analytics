import pandas as pd
import requests
from io import StringIO

url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com/"
}

response = requests.get(url, headers=headers)
response.raise_for_status()

df = pd.read_csv(StringIO(response.text))
required_columns = [
    'SYMBOL',
    'NAME OF COMPANY',
    ' SERIES',
    ' DATE OF LISTING',
    ' PAID UP VALUE',
    ' MARKET LOT',
    ' ISIN NUMBER',
    ' FACE VALUE'
]
# Remove leading/trailing spaces from column names for robust selection
df.columns = [col.strip() for col in df.columns]
selected_columns = [col.strip() for col in required_columns]
df_selected = df[selected_columns]

# --- PostgreSQL DDL ---
# CREATE TABLE equities_list (
#     symbol TEXT,
#     name_of_company TEXT,
#     series TEXT,
#     date_of_listing DATE,
#     paid_up_value NUMERIC,
#     market_lot INTEGER,
#     isin_number TEXT,
#     face_value NUMERIC
# );

# --- Load DataFrame into PostgreSQL ---
import sqlalchemy
from sqlalchemy import text

# Update these with your actual PostgreSQL credentials
PG_USER = 'kunal.nandwana'
PG_PASS = 'root'
PG_HOST = 'localhost'
PG_PORT = '5432'
PG_DB   = 'kunal.nandwana'
PG_TABLE = 'equities_list'

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# Truncate the table before loading new data
with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE bronze.equities_list"))

# Rename columns to match DDL
df_selected = df_selected.rename(columns={
    'SYMBOL': 'symbol',
    'NAME OF COMPANY': 'name_of_company',
    'SERIES': 'series',
    'DATE OF LISTING': 'date_of_listing',
    'PAID UP VALUE': 'paid_up_value',
    'MARKET LOT': 'market_lot',
    'ISIN NUMBER': 'isin_number',
    'FACE VALUE': 'face_value'
})

# Load to PostgreSQL using raw SQL to avoid pandas/SQLAlchemy compatibility issues
with engine.begin() as conn:
    for _, row in df_selected.iterrows():
        conn.execute(text("""
            INSERT INTO bronze.equities_list (symbol, name_of_company, series, date_of_listing, 
                                            paid_up_value, market_lot, isin_number, face_value)
            VALUES (:symbol, :name_of_company, :series, :date_of_listing, 
                   :paid_up_value, :market_lot, :isin_number, :face_value)
        """), {
            'symbol': row['symbol'],
            'name_of_company': row['name_of_company'],
            'series': row['series'],
            'date_of_listing': row['date_of_listing'],
            'paid_up_value': row['paid_up_value'],
            'market_lot': row['market_lot'],
            'isin_number': row['isin_number'],
            'face_value': row['face_value']
        })

print(f"Loaded {len(df_selected)} rows into bronze.equities_list table.")