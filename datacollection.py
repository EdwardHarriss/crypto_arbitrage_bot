import json
import threading
import signal
import sys
import websocket
import datetime
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import pymysql

COUNT = 0
load_dotenv()
pw = os.getenv('pw')

CONN = create_engine("mysql+pymysql://" + "root" + ":" + pw + "@" + "localhost" + "/" + "binance_data")

def DataPreProcessing(df):
    global COUNT
    coins = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT',
    'DOGEUSDT', 'AVAXUSDT', 'TRXUSDT', 'ETCUSDT', 'APEUSDT', 'SANDUSDT', 'FTMUSDT']

    df = df.drop('e', axis=1)  #removing title of data
    df = df.drop('q', axis=1)  #removing quantity of base (will be the same USDT)
    df = df.drop('O', axis=1)
    df = df.drop('C', axis=1)
    df = df.drop('F', axis=1)
    df = df.drop('L', axis=1)
    df = df.drop('n', axis=1)
    df = df.drop('P', axis=1)
    df = df.drop('w', axis=1)
    df = df.drop('x', axis=1)
    df = df.drop('Q', axis=1)
    df = df.drop('b', axis=1)
    df = df.drop('B', axis=1)
    df = df.drop('a', axis=1)
    df = df.drop('A', axis=1)
    df = df[df['s'].isin(coins)]
    COUNT = COUNT + 1
    print((COUNT/1440) * 100)
    if COUNT > 1440:  #24 MINS
        print("Closing")
        sys.exit(0)
    else:
        df.to_sql('raw_data',con=CONN,if_exists ='append',index=False)

def on_message(ws, message):
    message = json.loads(message)
    df = pd.DataFrame(message)

    def run(*args):
        DataPreProcessing(df)
        #arbitrage(message[0]['E'], {m['s']: float(m['c']) for m in message}, {m['s']: float(m['v']) for m in message}, {m['s']: float(m['P']) for m in message})

    threading.Thread(target=run).start()


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print(close_status_code)
    print(close_msg)
    print("### closed ###")
    print("Tring to reconnect")
    DataCollection()

def handler(sig, frame):
    print("Closing")
    sys.exit(0)

signal.signal(signal.SIGINT, handler)

def DataCollection():

    socket_spot = 'wss://stream.binance.com/ws/!ticker@arr'
    ws = websocket.WebSocketApp(socket_spot,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()

if __name__ == "__main__":
    DataCollection()