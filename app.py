from flask import Flask, render_template, jsonify, request, send_file
import redis
import csv
import json
import os
import subprocess
import signal

# global config
app = Flask(__name__)

CSV_FILE = 'summary.csv'
AGGREGATOR_PROCESS = None
PUBLISHER_PROCESS = None

# redis connection helper
def connect_to_redis():
    try:
        r = redis.Redis()
        r.ping()
        return r
    except redis.exceptions.ConnectionError:
        return None

# read aggregated data
def read_data(symbol):
    r = connect_to_redis()
    data = []
    if not r:
        return data
    keys = r.keys(f"{symbol}:*")
# fetches keys like BTCUSDT:20250728_1234
# decodes and parses each OHLCV entry from Redis
    if keys:
        for key in sorted(keys):
            key_str = key.decode()
            raw = r.get(key)
            if raw:
                ohlcv = json.loads(raw)
                timestamp = key_str.split(':')[1]
                data.append({
                    'minute': timestamp,
                    **ohlcv
                })
# fallback to CSV if no data in Redis
    elif os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['symbol'] == symbol:
                    data.append({
                        'minute': row['minute'],
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'volume': float(row['volume'])
                    })
    return data


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/data')
def get_data():
    symbol = request.args.get('symbol', 'BTCUSDT')
    return jsonify(read_data(symbol))


@app.route('/start_publisher')
def start_publisher():
    global PUBLISHER_PROCESS
    r = connect_to_redis()
    if not r:
        return jsonify({'status': 'Error: Redis not connected'}), 500
    if PUBLISHER_PROCESS is None or PUBLISHER_PROCESS.poll() is not None:
        PUBLISHER_PROCESS = subprocess.Popen(['python', 'publisher.py'])
        return jsonify({'status': 'Publisher started'})
    return jsonify({'status': 'Publisher already running'})


@app.route('/start_aggregator')
def start_aggregator():
    global AGGREGATOR_PROCESS
    r = connect_to_redis()
    if not r:
        return jsonify({'status': 'Error: Redis not connected'}), 500
    symbol = request.args.get('symbol', 'BTCUSDT')
    if AGGREGATOR_PROCESS is None or AGGREGATOR_PROCESS.poll() is not None:
        AGGREGATOR_PROCESS = subprocess.Popen(['python', 'aggregator.py', '--symbol', symbol])
        return jsonify({'status': f'Aggregator started for {symbol}'})
    return jsonify({'status': 'Aggregator already running'})


@app.route('/terminate')
def terminate_processes():
    global PUBLISHER_PROCESS, AGGREGATOR_PROCESS
    if PUBLISHER_PROCESS and PUBLISHER_PROCESS.poll() is None:
        PUBLISHER_PROCESS.terminate()
        PUBLISHER_PROCESS = None
    if AGGREGATOR_PROCESS and AGGREGATOR_PROCESS.poll() is None:
        AGGREGATOR_PROCESS.terminate()
        AGGREGATOR_PROCESS = None
    return jsonify({'status': 'Processes terminated'})


@app.route('/download_csv')
def download_csv():
    if os.path.exists(CSV_FILE):
        return send_file(CSV_FILE, as_attachment=True)
    return jsonify({'error': 'CSV file not found'}), 404


@app.route('/log/publisher')
def get_publisher_log():
    r = connect_to_redis()
    if not r:
        return jsonify([])
    raw = r.lrange("log:publisher", -20, -1)
    data = [json.loads(item) for item in raw]
    return jsonify(data)


@app.route('/log/aggregator')
def get_aggregator_log():
    r = connect_to_redis()
    if not r:
        return jsonify([])
    raw = r.lrange("log:aggregator", -20, -1)
    data = [json.loads(item) for item in raw]
    return jsonify(data)


@app.route('/aggregator_data')
def get_full_aggregator_data():
    data = []
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
    return jsonify(data)

@app.route('/summary.csv')
def serve_csv_file():
    return send_file(CSV_FILE)


@app.route('/chart')
def show_chart():
    return render_template('chart.html')

@app.route('/compare')
def compare():
    return render_template('compare.html')

@app.route('/compare_data')
def compare_data():
    r = connect_to_redis()
    if not r:
        return jsonify({"error": "Redis not connected"})
    trade = r.get("compare:trade")
    ticker = r.get("compare:ticker")
    return jsonify({
        "trade": json.loads(trade) if trade else {},
        "ticker": json.loads(ticker) if ticker else {}
    })

if __name__ == '__main__':
    app.run(debug=True)
