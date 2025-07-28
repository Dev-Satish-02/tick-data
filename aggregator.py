import redis
import json
import argparse
import csv
import os
import time
import logging
from datetime import datetime
from collections import defaultdict

# ------------------------- Setup Logging ------------------------- #
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# ------------------------- Parse Arguments ------------------------- #
parser = argparse.ArgumentParser()
parser.add_argument('--symbol', type=str, default='BTCUSDT', help='Symbol to subscribe to')
args = parser.parse_args()
SYMBOL = args.symbol

# ------------------------- Redis Connection ------------------------- #
def connect_to_redis():
    while True:
        try:
            r = redis.Redis()
            r.ping()
            logging.info("Connected to Redis successfully.")
            return r
        except redis.exceptions.ConnectionError:
            logging.error("Redis connection failed. Retrying in 5 seconds...")
            time.sleep(5)

r = connect_to_redis()
pubsub = r.pubsub()
pubsub.subscribe(SYMBOL)

# ------------------------- CSV Setup ------------------------- #
CSV_FILE = 'summary.csv'
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['symbol', 'minute', 'open', 'high', 'low', 'close', 'volume'])

# ------------------------- Utility Functions ------------------------- #
def get_minute(ts):
    return datetime.utcfromtimestamp(int(ts) / 1000).strftime('%Y%m%d_%H%M')

def aggregate_ticks(ticks):
    prices = [tick['price'] for tick in ticks]
    volumes = [tick['volume'] for tick in ticks]
    return {
        'open': prices[0],
        'high': max(prices),
        'low': min(prices),
        'close': prices[-1],
        'volume': sum(volumes)
    }

# ------------------------- Tick Aggregation Loop ------------------------- #
tick_data = defaultdict(list)
current_minute = None

logging.info(f"Started aggregator for symbol: {SYMBOL}")

for message in pubsub.listen():
    if message['type'] != 'message':
        continue

    try:
        tick = json.loads(message['data'])
        symbol = tick['symbol']
        price = float(tick['price'])
        volume = float(tick['volume'])
        ts = tick['timestamp']
        minute_key = get_minute(ts)

        if current_minute is None:
            current_minute = minute_key

        if minute_key != current_minute:
            summary = aggregate_ticks(tick_data[current_minute])
            redis_key = f"{symbol}:{current_minute}"

            # Publish to Redis
            try:
                r.set(redis_key, json.dumps(summary))
                logging.info(f"Published {redis_key} => {summary}")
            except redis.exceptions.RedisError as e:
                logging.error(f"Redis publish error: {e}")

            # Save to CSV
            try:
                with open(CSV_FILE, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        symbol, current_minute,
                        summary['open'], summary['high'],
                        summary['low'], summary['close'],
                        summary['volume']
                    ])
            except Exception as e:
                logging.error(f"CSV write error: {e}")

            # Reset
            del tick_data[current_minute]
            current_minute = minute_key

        tick_data[minute_key].append({
            'price': price,
            'volume': volume,
            'timestamp': ts
        })

    except Exception as e:
        logging.error(f"Processing error: {e}")
