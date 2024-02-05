import threading

import yaml
import yfinance as yf
import fugle_marketdata as fg
import re

INTERVAL = 20
THREADS_NUM = 100
DAYS = 1

totals = []


def load_config(path: str):
    with open(path, 'r') as file:
        config = yaml.safe_load(file)
        if config['api_key'] is None:
            raise Exception("No api_key in config")
        return config


def filter_func(t: yf.Ticker, result, i):
    hist_60k = t.history(interval="60m", period="1mo")

    if len(hist_60k) < (INTERVAL + DAYS * 5):
        result[i] = False
        return

    hist_60k = hist_60k[-(INTERVAL + DAYS * 5):]

    last_hist = hist_60k[-1:]['Close'].iloc[0]
    if last_hist < 10 or last_hist > 100:
        result[i] = False
        return

    if int(t.history(interval="1d", period="1d")['Volume'].iloc[0]) < 500000:
        result[i] = False
        return

    ema_pre = int(hist_60k[INTERVAL - 1: INTERVAL]['Close'].iloc[0])

    vals = []

    for idx in range(INTERVAL, len(hist_60k)):
        hist_cur = hist_60k[idx - INTERVAL: idx]
        bb_basis = hist_cur['Close'].mean()
        bb_std = hist_cur['Close'].std()
        bb_upper = bb_basis + 2 * bb_std
        bb_lower = bb_basis - 2 * bb_std

        ema = int(hist_cur[-1:]['Close'].iloc[0]) * 2 / (INTERVAL + 1) + ema_pre * (1 - 2 / (INTERVAL + 1))
        ema_pre = ema
        kc_basis = ema
        atr = max(hist_cur[-1:]['High'].iloc[0] - hist_cur[-1:]['Low'].iloc[0]
                  , hist_cur[-1:]['High'].iloc[0] - hist_cur[-2:-1]['Close'].iloc[0]
                  , hist_cur[-1:]['Low'].iloc[0] - hist_cur[-2:-1]['Close'].iloc[0])
        kc_upper = kc_basis + 1.5 * atr
        kc_lower = kc_basis - 1.5 * atr

        cur = {'bu': bb_upper, 'bl': bb_lower, 'ku': kc_upper, 'kl': kc_lower}
        vals.append(cur)

    def interpolate(i, a, b):
        return (vals[i][a] > vals[i][b] and vals[i - 1][a] < vals[i - 1][b]) or (
                vals[i][b] > vals[i][a] and vals[i - 1][b] < vals[i - 1][a])

    for idx in range(1, len(vals)):
        if interpolate(idx, 'ku', 'bu'):
            result[i] = True
            return
        if interpolate(idx, 'kl', 'bl'):
            result[i] = True
            return

    result[i] = False


def thread_handler(threads, results):
    for idx in range(len(threads)):
        threads[idx][1].join()
        if results[idx]:
            totals.append(threads[idx][0])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    config = load_config("config.yaml")

    fg_client = fg.RestClient(api_key=config['api_key'])
    tickers = fg_client.stock.intraday.tickers(type='EQUITY', exchange='TWSE', market='TSE')['data']
    cnt = 0
    threads = []
    res = [False] * THREADS_NUM
    for x in tickers:
        if re.search('[a-zA-Z]', x['symbol']):
            continue
        if len(x['symbol']) > 4:
            continue
        ticker = yf.Ticker(x['symbol'] + '.TW')
        thread = threading.Thread(target=filter_func, args=(ticker, res, len(threads)))
        thread.start()
        threads.append((x, thread))

        if len(threads) == THREADS_NUM:
            thread_handler(threads, res)
            threads.clear()
            res = [False] * THREADS_NUM

    thread_handler(threads, res)

    for item in totals:
        print(item)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
