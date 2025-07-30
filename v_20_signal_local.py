import pandas as pd

# list of symbols to run V20 backtest on
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

for sym in symbols:
    print(f"\n===== {sym} =====")
    # 1. load full history for this symbol
    path = f"gs://indian_stock_analytics/daily/2025-07-23/{sym}.parquet"
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"  ✗ Could not load {sym} data ({e}), skipping.")
        continue

    # 2. ensure datetime & sort
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['symbol','date']).reset_index(drop=True)

    # 3. flag green candles
    df['green'] = df['closeprice'] > df['openprice']

    # 4. identify runs of consecutive greens (use transform to avoid FutureWarning)
    df['run_id'] = (
        df.groupby('symbol')['green']
          .transform(lambda x: (x != x.shift()).cumsum())
    )

    # 5. aggregate each green‐run
    runs = (
        df[df['green']]
          .groupby(['symbol', 'run_id'])
          .agg(
            start_date = ('date',      'first'),
            end_date   = ('date',      'last'),
            buy_price  = ('lowprice',  'min'),  # buy at run's lowest low
            sell_price = ('highprice', 'max'),  # sell at run's highest high
            length     = ('date',      'count')
          )
          .reset_index(drop=True)
    )

    # 6. compute total gain %
    runs['gain_pct'] = (runs['sell_price'] - runs['buy_price']) \
                       / runs['buy_price'] * 100

    # 7. filter V20 runs: ≥2 days & ≥20% total gain
    v20 = runs[(runs['length'] >= 2) & (runs['gain_pct'] >= 20)].reset_index(drop=True)

    # 8a. detect buy retest date
    buy_retests = []
    for _, row in v20.iterrows():
        dates = df.loc[(df['date'] > row.start_date) & (df['lowprice'] <= row.buy_price), 'date']
        buy_retests.append(dates.min() if not dates.empty else pd.NaT)
    v20['buy_retest_date'] = buy_retests
    # detect sell retest date only if buy retest occurred
    sell_retests = []
    for _, row in v20.iterrows():
        if pd.isna(row.buy_retest_date):
            sell_retests.append(pd.NaT)
        else:
            dates = df.loc[(df['date'] > row.buy_retest_date) & (df['highprice'] >= row.sell_price), 'date']
            sell_retests.append(dates.min() if not dates.empty else pd.NaT)
    v20['sell_retest_date'] = sell_retests
    # ensure retest dates are datetime for .dt operations
    v20['buy_retest_date'] = pd.to_datetime(v20['buy_retest_date'])
    v20['sell_retest_date'] = pd.to_datetime(v20['sell_retest_date'])

    # 8b. compute buy and sell statuses with explicit loops to avoid apply issues
    ARR_THRESHOLD = 0.02  # 2% above buy_price considered 'About to Arrive'
    def _compute_buy_status(row):
        if pd.notnull(row.buy_retest_date):
            return 'Completed'
        sub = df[df['date'] > row.start_date]
        if not sub.empty:
            if sub['lowprice'].min() <= row.buy_price * (1 + ARR_THRESHOLD):
                return 'About to Arrive'
        return 'Pending'

    buy_statuses = []
    sell_statuses = []
    for _, row in v20.iterrows():
        buy_statuses.append(_compute_buy_status(row))
        sell_statuses.append('Completed' if pd.notnull(row.sell_retest_date) else 'Pending')
    v20['buy_status'] = buy_statuses
    v20['sell_status'] = sell_statuses
    # 8c. compute days between buy and sell retest
    v20['retest_interval_days'] = (v20['sell_retest_date'] - v20['buy_retest_date']).dt.days

    # filter out signals where both buy and sell statuses are Completed
    actionable = v20.loc[~((v20['buy_status'] == 'Completed') & (v20['sell_status'] == 'Completed'))]

    # 8d. output all V20 signals with buy/sell statuses and interval
    print("\nV20 signals (>=2-day runs, >=20% gain):")
    if actionable.empty:
        print("  No V20 runs found.")
    else:
        for _, row in actionable.iterrows():
            print(
                f"  Buy on {row.start_date.date()} @ {row.buy_price:.2f}, "
                f"Sell on {row.end_date.date()} @ {row.sell_price:.2f}, "
                f"Days={int(row.length)}, Gain={row.gain_pct:.2f}%, "
                f"Buy Retest on {row.buy_retest_date.date() if pd.notnull(row.buy_retest_date) else 'None'} ({row.buy_status}), "
                f"Sell Retest on {row.sell_retest_date.date() if pd.notnull(row.sell_retest_date) else 'None'} ({row.sell_status}), "
                f"Interval={int(row.retest_interval_days) if pd.notnull(row.retest_interval_days) else 'N/A'} days"
            )

    # 9. show only 2025 signals
    v20['buy_year'] = v20['start_date'].dt.year
    signals_2025 = v20[v20['buy_year'] == 2025]

    print("\nV20 signals in 2025:")
    if signals_2025.empty:
        print("  None")
    else:
        for _, row in signals_2025.iterrows():
            print(
                f"  Buy on {row.start_date.date()} @ {row.buy_price:.2f}, "
                f"Sell on {row.end_date.date()} @ {row.sell_price:.2f}, "
                f"Days={int(row.length)}, Gain={row.gain_pct:.2f}%, "
                f"Buy Retest on {row.buy_retest_date.date() if pd.notnull(row.buy_retest_date) else 'None'} ({row.buy_status}), "
                f"Sell Retest on {row.sell_retest_date.date() if pd.notnull(row.sell_retest_date) else 'None'} ({row.sell_status}), "
                f"Interval={int(row.retest_interval_days) if pd.notnull(row.retest_interval_days) else 'N/A'} days"
            )