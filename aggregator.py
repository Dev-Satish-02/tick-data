import redis
import json
from datetime import datetime
from collections import defaultdict
import logging
import time

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Connect to Redis
r = redis.Redis()
pubsub = r.pubsub()
pubsub.subscribe('BTCUSDT')

# Store ticks grouped by minute
tick_data = defaultdict(list)

def get_minute(ts):
    return datetime.fromisoformat(ts).strftime('%Y%m%d_%H%M')

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

current_minute = None

for message in pubsub.listen():
    if message['type'] != 'message':
        continue

    tick = json.loads(message['data'])
    symbol = tick['symbol']
    price = float(tick['price'])
    volume = float(tick['volume'])
    ts = tick['timestamp']
    minute_key = get_minute(ts)

    if current_minute is None:
        current_minute = minute_key

    if minute_key != current_minute:
        # Aggregate and publish
        summary = aggregate_ticks(tick_data[current_minute])
        redis_key = f"{symbol}:{current_minute}"
        r.set(redis_key, json.dumps(summary))
        logging.info(f"Published {redis_key} => {summary}")

        # Cleanup
        del tick_data[current_minute]
        current_minute = minute_key

    tick_data[minute_key].append({
        'price': price,
        'volume': volume,
        'timestamp': ts
    })
