import requests, time, sys, threading
from flask import Flask

app = Flask(**name**)

def get_btc_price():
try:
r = requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot")
return float(r.json()["data"]["amount"])
except:
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

threading.Thread(target=loop).start()

app.run(host="0.0.0.0", port=10000)
