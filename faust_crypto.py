from dataclasses import asdict, dataclass
import json
import requests
import time
import faust
import collections
import numpy as np
from lib import data, environ
from collections import namedtuple

Prices = collections.namedtuple('Prices', field_names=['open', 'high', 'low', 'close', 'volume'])

FAUST_BROKER_URL = "kafka://localhost:29092"

@dataclass
class CryptoAgg(faust.Record, validation=True, serializer="json"):
    SYMBOL: str
    V: list
    O: list
    C: list
    H: list
    L: list

# faust -A faust_crypto worker -l info

app = faust.App(
    "stock_trader",
    broker=FAUST_BROKER_URL,
    consumer_auto_offset_reset="latest",
    store='memory://',
    partitions=1)

stock_events = app.topic('CRYPTO_SLIDING_WINDOW')#, key_type=str, value_type=CryptoAgg)
stock_order = app.topic('CRYPTO_ORDERING_SYSTEM')

async def order_signal(response):
    print("order signal ->", response)
    return response

@app.agent(stock_order)
async def orders(orderevents):
    orderevents.add_processor(order_signal)
    async for orders in orderevents:
        print("orders ->", orders)

@app.agent(stock_events)
async def stocks(stockevents):
    async for stockevent in stockevents:
        #print(f"-> Sending observation {stockevent}")
        prices = Prices(
            open=np.array(stockevent['O'], dtype=np.float32),
            high=np.array(stockevent['H'], dtype=np.float32),
            low=np.array(stockevent['L'], dtype=np.float32),
            close=np.array(stockevent['C'], dtype=np.float32),
            volume=np.array(stockevent['V'], dtype=np.float32)
            )
        print("Worker: agent object", prices)
        prices = {stockevent['SYMBOL']: prices}
        env = environ.StocksEnv(
            prices,
            bars_count=30,
            reset_on_close=False,
            commission=0.00,
            state_1d=False,
            random_ofs_on_reset=False,
            reward_on_close=True,
            volumes=False)
        obs = env.reset()
        #print(f"-> Sending observation {obs}")
        resp = requests.get("http://127.0.0.1:8000/trade_crypto", json={"observation": obs.tolist()}).text
        print(f"<- Received response {stockevent['SYMBOL']}{':'}{resp}")
        #yield {stockevent['SYMBOL']: resp}
        await stock_order.send(value=resp)

if __name__ == "__main__":
    app.main()