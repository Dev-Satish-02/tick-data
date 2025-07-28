# Real-Time Tick Aggregator

## Overview
Subscribes to a Redis pub/sub channel to receive real-time tick data and aggregates it minute-by-minute (OHLCV).

## Setup

1. Install Redis and start the server.

2. Install dependencies:
   ```bash
   pip install -r requirements.txt

3. Run the publisher:
   ```bash
    python publisher.py
    
4. Run the aggregator:
   ```bash
    python aggregator.py


