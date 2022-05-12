import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import signal
import threading
import websocket
from datetime import datetime
import json
import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import load_workbook
from src.cex_trading import CentralizedExchangeTrading as CEX_Trading

def arbitrage_trading(date, data):
    date_format = datetime.fromtimestamp(date/1000)
    date = datetime.fromtimestamp(date/1000).strftime('%Y-%m-%d %H:%M:%S')
    date_now = datetime.now()
    #trade, predicted_gain = BinanceTrading.GetArbitrage(data, date)
    #date_after = datetime.now()

    ##Timing Analysis

    print (date_now-date_format)
    """
    delay_recieving_data = date_now-date_format
    delay_in_arb = date_after-date_now
    t_df = pd.DataFrame([[predicted_gain], [date_format], [date_now], [delay_recieving_data], [date_after], [delay_in_arb]])
    wb = load_workbook(filename="data/Trading_Timing.xlsx")
    ws = wb['Binance Timing']
    for r in dataframe_to_rows(t_df, index=False, header=False):
        ws.append(r)
    wb.save("data/Trading_Timing.xlsx")
    """

def on_message(ws, message):
    message = json.loads(message)
    print(datetime.now())

    def run(*args):
        arbitrage_trading(message[0]['E'], {m['s']: float(m['c']) for m in message})

    threading.Thread(target=run).start()


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print(close_status_code)
    print(close_msg)
    print("### closed ###")
    print("Trying to Reconnect")
    BinanceExchange(MIN_ARBITRAGE, FEE, BASE)

def handler(signum, frame):
    print("### closed ###")
    exit(1)

signal.signal(signal.SIGINT, handler)


def BinanceExchange(minimum_arbitrage_allowance_perc, fee_per_transaction_percent, base_):
    global MIN_ARBITRAGE, FEE, BASE, BinanceTrading
    MIN_ARBITRAGE = minimum_arbitrage_allowance_perc
    FEE = fee_per_transaction_percent
    BASE = base_

    BinanceTrading = CEX_Trading(base_, minimum_arbitrage_allowance_perc, fee_per_transaction_percent)

    socket_spot = 'wss://stream.binance.com/ws/!ticker@arr'
    ws = websocket.WebSocketApp(socket_spot,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()