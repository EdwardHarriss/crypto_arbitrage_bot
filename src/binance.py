import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import signal
import threading
import websocket
import datetime
import json
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import load_workbook
from src.centralized_exchange import CentralizedExchange as CE

def arbitrage(date, data):
    date = datetime.datetime.utcfromtimestamp(date/1000).strftime('%Y-%m-%d %H:%M:%S')
    routes = Binance.GetArbitrageRoutes(data)
    output = Binance.GetArbitrageReturns(routes, data, date)
    wb = load_workbook(filename="data/Triangular_Arbitrage_Binance.xlsx")
    ws = wb['Binance']
    for r in dataframe_to_rows(output, index=False, header=False):
        sr = ' -> '.join([str(elem) for elem in r[3]])
        r[3] = sr
        ws.append(r)
    
    ws = wb['Binance Occ.']
    for r in dataframe_to_rows(output, index=False, header=False):
        occ = [date, 'Binance', len(output)]
        ws.append(occ)
    wb.save("data/Triangular_Arbitrage_Binance.xlsx")

def on_message(ws, message):
    message = json.loads(message)

    def run(*args):
        arbitrage(message[0]['E'], {m['s']: float(m['c']) for m in message})

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
    global MIN_ARBITRAGE, FEE, BASE
    MIN_ARBITRAGE = minimum_arbitrage_allowance_perc
    FEE = fee_per_transaction_percent
    BASE = base_

    global Binance
    Binance = CE('Binance', base_, minimum_arbitrage_allowance_perc, fee_per_transaction_percent)

    socket_spot = 'wss://stream.binance.com/ws/!ticker@arr'
    ws = websocket.WebSocketApp(socket_spot,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()