import redis, json, time, random

r = redis.Redis()
while True:
    tick = {
        'price': round(random.uniform(29000, 30000), 2),
        'volume': round(random.uniform(0.01, 2), 4),
        'timestamp': time.time()
    }
    r.publish('BTCUSDT', json.dumps(tick))
    time.sleep(0.5)
