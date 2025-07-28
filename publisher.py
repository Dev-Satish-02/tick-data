import redis
import time
import json
import random
from datetime import datetime

r = redis.Redis()

while True:
    tick = {
        "symbol": "BTCUSDT",
        "price": round(random.uniform(30000, 40000), 2),
        "volume": round(random.uniform(0.1, 5.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }
    r.publish("BTCUSDT", json.dumps(tick))
    time.sleep(1)
