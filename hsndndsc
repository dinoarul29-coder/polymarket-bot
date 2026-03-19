import requests, time, sys, threading, os
from flask import Flask

app = Flask(__name__)

def get_btc_price():
    try:
        r = requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=10)
        r.raise_for_status()
        return float(r.json()["data"]["amount"])
    except Exception as e:
        print(f"Price fetch failed: {e}")
        return None

@app.route("/")
def home():
    return "Bot is running"

def loop():
    print("Bot started...")
    while True:
        price = get_btc_price()
        print(f"BTC Price: {price}")
        sys.stdout.flush()
        time.sleep(5)

threading.Thread(target=loop, daemon=True).start()

port = int(os.environ.get("PORT", 10000))
app.run(host="0.0.0.0", port=port)
