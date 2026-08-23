import requests

symbol = 'CIPLA'
frm = '2026-02-10'
to = '2026-02-17'

sess = requests.Session()
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}',
}

home = sess.get('https://www.nseindia.com', headers=headers, timeout=15)
print('Home status', home.status_code)

url = f'https://www.nseindia.com/api/historical/cm/equity?symbol={symbol}&series=[%22EQ%22]&from={frm}&to={to}'
r = sess.get(url, headers=headers, timeout=20)
print('Status', r.status_code)

if r.status_code == 200:
    data = r.json()
    rows = sorted(data.get('data', []), key=lambda x: x['CH_TIMESTAMP'], reverse=True)
    for row in rows:
        print(row['CH_TIMESTAMP'], row['OPEN_PRICE'], row['HIGH_PRICE'], row['LOW_PRICE'], row['CLOSE_PRICE'], row['TOT_TRDQTY'], row['TOT_TRD_VAL'], row['NO_OF_TRADES'])
else:
    print(r.text[:500])
