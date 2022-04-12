from binance.client import Client
import config as config
import numpy as np
import threading
import websocket
import datetime
import json


class Pair:
    A = ['ETHBTC',]
    B = ['ETHUSDT',]
    C = ['BTCUSDT',]

fee = 0.00
pair = Pair()

def arbitrage_opportunity(data, pair_id):
    try:
        return (1 - 3*fee) / data[pair.A[pair_id]] * data[pair.B[pair_id]] / data[pair.C[pair_id]], [pair.A[pair_id], pair.B[pair_id], pair.C[pair_id]]
    except:
        return np.nan, np.nan

def arbitrage(date, data):
    date = datetime.datetime.utcfromtimestamp(date/1000).strftime('%Y-%m-%d %H:%M:%S')
    gain, pair_symbol = arbitrage_opportunity(data, 0)
    if gain > 1.0:
        print(f'{date}, pair->{pair_symbol}, gain={gain}')

def on_message(ws, message):
    message = json.loads(message)
    print(message)

    def run(*args):
        arbitrage(message[0]['E'], {m['s']: float(m['c']) for m in message})

    threading.Thread(target=run).start()


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


if __name__ == "__main__":

    socket_spot = 'wss://stream.binance.com/ws/!ticker@arr'
    ws = websocket.WebSocketApp(socket_spot,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()