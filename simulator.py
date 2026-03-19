import requests, time, sys

def get_btc_price():
try:
r = requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot")
return float(r.json()["data"]["amount"])
except:
return None

print("Bot started...")

while True:
price = get_btc_price()

```
if price:
    print(f"BTC Price: ${price}")
else:
    print("Failed to fetch BTC price")

sys.stdout.flush()
time.sleep(5)
```
