# Be sure to pip install polygon-api-client

import time
import json
import config
from kafka import KafkaProducer
from websocket_client import WebSocketClient, CRYPTO_CLUSTER

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    acks=0,
    batch_size=0
)

""" This method below processes the one-minute aggregate bars from
the polygon.io websocket, and then sends them to the crypto_min_bars
topic producer."""

def my_custom_process_message(message):
    """{
        'ev': 'XA',
        'pair': 'BTC-USD',
        'v': 21.67807129,
        'vw': 58699.308,
        'z': 0,
        'o': 58662,
        'c': 58761.79,
        'h': 58790.48,
        'l': 58640.7767046,
        's': 1616022720000,
        'e': 1616022780000
    }"""

    TICK_INSTANCE = json.loads(message)[0]['ev'] == 'XA'
    try:
        if TICK_INSTANCE:
            message_str = (json.loads(message)[0])
            message_str['c'] = (message_str['c'] - message_str['o']) / message_str['o']
            message_str['l'] = (message_str['l'] - message_str['o']) / message_str['o']
            message_str['h'] = (message_str['h'] - message_str['o']) / message_str['o']
            print(message_str)
            producer.send('crypto_min_bars',value=message_str)
        else:
            pass
    except Exception as e:
        logging.error("{}".format(e.args))
    
def my_custom_error_handler(ws, error):
    print("this is my custom error handler", error)


def my_custom_close_handler(ws):
    print("this is my custom close handler")


def main():
    key = config.POLYGON_API
    my_client = WebSocketClient(CRYPTO_CLUSTER, key, my_custom_process_message)
    my_client.run_async()

    my_client.subscribe("XA.BTC-USD, XA.ETH-USD")
    time.sleep(1)

    #my_client.close_connection()


if __name__ == "__main__":
    main()