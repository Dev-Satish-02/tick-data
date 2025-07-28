import redis
import json
import time
from datetime import datetime
from collections import defaultdict
import argparse
import logging
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def parse_args():
    parser = argparse.ArgumentParser(description='Real-Time Tick Data Aggregator')
    parser.add_argument('--symbol', type=str, required=True, help='Symbol name (e.g., BTCUSDT)')
    parser.add_argument('--redis_host', type=str, default='localhost', help='Redis host')
    parser.add_argument('--redis_port', type=int, default=6379, help='Redis port')
    return parser.parse_args()

def get_minute(timestamp):
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime('%Y%m%d_%H%M')

class TickAggregator:
    def __init__(self, symbol, redis_client):
        self.symbol = symbol
        self.redis_client = redis_client
        self.ticks_by_minute = defaultdict(list)
        self.lock = threading.Lock()

    def process_tick(self, tick):
        try:
            data = json.loads(tick)
            price = float(data['price'])
            volume = float(data['volume'])
            timestamp = float(data['timestamp'])
            minute = get_minute(timestamp)
            
            with self.lock:
                self.ticks_by_minute[minute].append((price, volume, timestamp))
                self.aggregate_and_publish(minute)

        except Exception as e:
            logging.error(f"Error processing tick: {e}")

    def aggregate_and_publish(self, minute):
        ticks = self.ticks_by_minute[minute]
        if len(ticks) == 0:
            return

        prices = [p for p, _, _ in ticks]
        volumes = [v for _, v, _ in ticks]

        summary = {
            'symbol': self.symbol,
            'minute': minute,
            'open': prices[0],
            'high': max(prices),
            'low': min(prices),
            'close': prices[-1],
            'volume': sum(volumes)
        }

        key = f"{self.symbol}:{minute}"
        self.redis_client.publish(key, json.dumps(summary))
        logging.info(f"Published summary for {key}: {summary}")

        # Optional: save to file
        with open(f"{self.symbol}_summary.csv", "a") as f:
            f.write(",".join([str(v) for v in summary.values()]) + "\n")

        # Cleanup
        del self.ticks_by_minute[minute]

def listen_for_ticks(symbol, redis_client, aggregator):
    pubsub = redis_client.pubsub()
    pubsub.subscribe(symbol)
    logging.info(f"Subscribed to Redis channel: {symbol}")

    for message in pubsub.listen():
        if message['type'] == 'message':
            tick_data = message['data']
            if isinstance(tick_data, bytes):
                tick_data = tick_data.decode('utf-8')
            aggregator.process_tick(tick_data)

def main():
    args = parse_args()
    redis_client = redis.Redis(host=args.redis_host, port=args.redis_port)
    aggregator = TickAggregator(args.symbol, redis_client)
    listen_for_ticks(args.symbol, redis_client, aggregator)

if __name__ == '__main__':
    main()
