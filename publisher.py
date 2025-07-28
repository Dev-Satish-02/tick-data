# publisher.py
import json
import redis
import websocket
import threading
import time
from datetime import datetime

SYMBOL = "btcusdt"
REDIS_CHANNEL = "BTCUSDT"
LOG_LIST = "log:publisher"

r = redis.Redis()

def on_message(ws, message):
    try:
        data = json.loads(message)
        price = float(data['p'])
        volume = float(data['q'])
        timestamp = int(data['T'])
        readable_time = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

        tick = {
            "symbol": "BTCUSDT",
            "price": price,
            "volume": volume,
            "timestamp": timestamp
        }

        r.publish(REDIS_CHANNEL, json.dumps(tick))
        r.rpush(LOG_LIST, json.dumps({
            "timestamp": readable_time,
            "symbol": "BTCUSDT",
            "price": price,
            "volume": volume
        }))
        r.ltrim(LOG_LIST, -100, -1)  # Keep only last 100 logs

    except Exception as e:
        print("Error parsing message:", e)

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed:", close_status_code, close_msg)

def on_open(ws):
    print("WebSocket opened")

def run_websocket():
    ws = websocket.WebSocketApp(
        f"wss://stream.binance.com:9443/ws/{SYMBOL}@trade",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws.run_forever()

if __name__ == "__main__":
    while True:
        try:
            run_websocket()
        except Exception as e:
            print("WebSocket crashed, reconnecting...", e)
            time.sleep(5)
