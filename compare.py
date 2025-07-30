import websocket
import threading
import json
import redis

r = redis.Redis()

def handle_trade(ws, message):
    r.set("compare:trade", message)

def handle_ticker(ws, message):
    r.set("compare:ticker", message)

def run_ws(symbol, stream, handler):
    url = f"wss://stream.binance.com:9443/ws/{symbol}@{stream}"
    def on_message(ws, message): handler(ws, message)
    def on_error(ws, error): print(f"{stream} error:", error)
    def on_close(ws): print(f"{stream} closed")
    ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.run_forever()

if __name__ == "__main__":
    threading.Thread(target=run_ws, args=("btcusdt", "trade", handle_trade)).start()
    threading.Thread(target=run_ws, args=("btcusdt", "ticker", handle_ticker)).start()
