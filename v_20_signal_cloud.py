import os
import pandas as pd
import requests
import gcsfs
from flask import Request
from datetime import datetime, date

# Configuration via environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN', '8285208063:AAEYAV85nuj8jQYFMQ3yFEmJbbsmazjZdvI')
CHAT_ID  = os.getenv('CHAT_ID', '5754721240')
# GCS prefix holding date subfolders, e.g. "indian_stock_analytics/daily"
GCS_DAILY_PREFIX = os.getenv("GCS_DAILY_PREFIX", "indian_stock_analytics/daily")

symbols = [
    'LT','RELIANCE','SBIN','HDFCBANK','ICICIBANK','AXISBANK','KOTAKBANK',
    'HCLTECH','INFY','TCS','HDFCAMC','HDFCLIFE','ICICIPRULI','ICICIGI','BAJAJFINSV',
    'BAJAJHLDNG','BAJFINANCE','BAJAJ-AUTO','MARUTI','HINDUNILVR','NESTLEIND','PGHH',
    'PIDILITIND','COLPAL','DABUR','GILLETTE','MARICO','ITC','TITAN','PAGEIND','BATAINDIA',
    'HAVELLS','VOLTAS','GLAXO','ABBOTIND','PFIZER','SANOFI','ASIANPAINT','BERGERPAINT',
    'CDSL','BSE','JIOFIN','ANGELONE','CAMS','BAJAJHFL','MCX','ULTRACEMCO','ACC','MANPOWER',
    'TEAMLEASE','QUESS','ASTRAZEN','CIPLA','ERIS','LALPATHLAB','APOLLOHOSP','MEDANTA','FORTIS',
    'ADANIPORTS','JSWINFRA','AWL','GODREJCP','DIXON','KAJARIACER','HONAUT','DMART','RELAXO',
    'BLUESTARCO','BOSCHLTD','EICHERMOT','MRF','M&M','TATAMOTORS','HYUNDAI','INDHOTEL','ITCHOTELS',
    'UNITDSPR','RADICO','UBL','VBL'
]


def load_combined_symbol(symbol: str) -> pd.DataFrame:
    """
    Reads every `<date>/<symbol>.parquet` under GCS_DAILY_PREFIX,
    concatenates, dedupes on date, and sorts ascending.
    """
    fs = gcsfs.GCSFileSystem()
    base = GCS_DAILY_PREFIX.rstrip('/') + '/'
    try:
        dirs = fs.ls(base, detail=False)
    except Exception:
        return pd.DataFrame()

    df_list, paths = [], []
    for d in dirs:
        path = f"gs://{d}/{symbol}.parquet"
        try:
            if fs.exists(path):
                df = pd.read_parquet(path)
                df_list.append(df)
                paths.append(path)
        except Exception:
            continue
    if not df_list:
        return pd.DataFrame()

    combined = pd.concat(df_list, ignore_index=True)
    combined['date'] = pd.to_datetime(combined['date'])
    combined = (
        combined
        .drop_duplicates(subset=['date'])
        .sort_values('date')
        .reset_index(drop=True)
    )
    print(f"[DEBUG] {symbol} combined from sources: {paths}")
    return combined


def send_telegram(message: str):
    """
    Send a message via Telegram bot.
    """
    if not BOT_TOKEN or not CHAT_ID:
        print(f"[TELEGRAM DEBUG] {message}")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": message})


def process_symbol(symbol: str):
    df = load_combined_symbol(symbol)
    if df.empty:
        return

    # only keep 2025 data
    df = df[df['date'].dt.year == 2025]
    if df.empty:
        return

    # compute data coverage range
    min_date = df['date'].min().date()
    max_date = df['date'].max().date()

    # V20 logic on combined df
    df['green'] = df['closeprice'] > df['openprice']
    df['run_id'] = (df['green'] != df['green'].shift()).cumsum()

    runs = (
        df[df['green']]
          .groupby('run_id')
          .agg(
            start_date=('date','first'),
            end_date=('date','last'),
            buy_price=('lowprice','min'),
            sell_price=('highprice','max'),
            length=('date','count')
          )
          .reset_index(drop=True)
    )
    runs['gain_pct'] = (runs['sell_price'] - runs['buy_price']) / runs['buy_price'] * 100

    v20 = runs[(runs['length'] >= 2) & (runs['gain_pct'] >= 20)].reset_index(drop=True)
    if v20.empty:
        return

    # retest detection
    buy_retests, sell_retests = [], []
    for _, row in v20.iterrows():
        d_buy   = df.loc[(df['date'] > row.start_date) & (df['lowprice'] <= row.buy_price), 'date']
        buy_dt  = d_buy.min() if not d_buy.empty else pd.NaT
        buy_retests.append(buy_dt)
        if pd.isna(buy_dt):
            sell_retests.append(pd.NaT)
        else:
            d_sell = df.loc[(df['date'] > buy_dt) & (df['highprice'] >= row.sell_price), 'date']
            sell_retests.append(d_sell.min() if not d_sell.empty else pd.NaT)

    v20['buy_retest_date']  = pd.to_datetime(buy_retests)
    v20['sell_retest_date'] = pd.to_datetime(sell_retests)

    ARR = 0.02
    buy_statuses, sell_statuses = [], []
    for _, row in v20.iterrows():
        if pd.notnull(row.buy_retest_date):
            buy_statuses.append('Completed')
        else:
            sub = df[df['date'] > row.start_date]
            buy_statuses.append(
                'About to Arrive' if (not sub.empty and sub['lowprice'].min() <= row.buy_price * (1+ARR)) 
                else 'Pending'
            )
        sell_statuses.append('Completed' if pd.notnull(row.sell_retest_date) else 'Pending')

    v20['buy_status']            = buy_statuses
    v20['sell_status']           = sell_statuses
    v20['retest_interval_days']  = (v20['sell_retest_date'] - v20['buy_retest_date']).dt.days

    actionable = v20.loc[~((v20['buy_status']=='Completed') & (v20['sell_status']=='Completed'))]
    if actionable.empty:
        return

    lines = [
        f"📊 {symbol} data range: {min_date} → {max_date}",
        f"📈 {symbol} V20 Signals:"
    ]
    for _, row in actionable.iterrows():
        lines.append(
            f"🟢 Buy: {row.start_date.date()} @ {row.buy_price:.2f} | "
            f"🔴 Sell: {row.end_date.date()} @ {row.sell_price:.2f} | "
            f"Gain: {row.gain_pct:.2f}% | "
            f"BuyRetest: {row.buy_retest_date.date() if pd.notnull(row.buy_retest_date) else 'None'} ({row.buy_status}) | "
            f"SellRetest: {row.sell_retest_date.date() if pd.notnull(row.sell_retest_date) else 'None'} ({row.sell_status})"
        )

    send_telegram("\n".join(lines))


def v20_handler(request: Request):
    for sym in symbols:
        process_symbol(sym)
    return ('OK', 200)


def scrape_and_upload(request):
    """
    Scrape data from website and upload to GCS.
    Triggered by HTTP request.
    """
    current_date = date.today().isoformat()
    bucket_path  = f"gs://{GCS_DAILY_PREFIX}/{current_date}/"

    today       = date.today()
    today_str   = today.strftime("%d-%m-%Y")
    from_period = today_str
    to_period   = today_str
    print(f"[DEBUG] Fetching data from {from_period} to {to_period}")

    # ... existing scraping logic ...

    return "Scraping and upload completed.", 200