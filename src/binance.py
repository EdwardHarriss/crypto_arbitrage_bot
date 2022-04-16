import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import numpy as np
import threading
import websocket
import datetime
import json
import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import load_workbook


def GetArbitrageReturns(routes, data, date):
    output = pd.DataFrame(columns=['Time','Exchange','Arbitrage Direction','Cryptocurrency Pairs','Gain'])
    for route in routes:

        p1 = 100/data[route[0]] * (1- (FEE/100))  #buy
        p2 = p1*data[route[1]] * (1- (FEE/100))   #sell
        p3 = p2*data[route[2]] * (1- (FEE/100))   #sell

        gain = p3-100
        if gain > MIN_ARBITRAGE:
            output_data = {'Time' : [date], 'Exchange' : ['binance'], 'Arbitrage Direction' : ['BUY -> SELL -> SELL'], 'Cryptocurrency Pairs' : [route], 'Gain' : [gain]}
            output_frame = pd.DataFrame(output_data)
            output = output.append(output_frame, ignore_index=False)
            print(route)
    return output

def GetArbitrageRoutes(data):
    listings = data.keys()
    routes = []
    for sym1 in listings:
        if sym1[-len(BASE):] == BASE:
            inter = sym1[:-len(BASE)]
            for sym2 in listings:
                if (sym2[:-len(BASE)] == inter) and (sym2 != sym1):
                    end = sym2[-len(BASE):]
                    for sym3 in listings:
                        if (end == sym3[:-len(BASE)]) and (sym3[-len(BASE):] == BASE):
                            routes.append([sym1,sym2,sym3])
    return routes


def arbitrage(date, data):
    date = datetime.datetime.utcfromtimestamp(date/1000).strftime('%Y-%m-%d %H:%M:%S')
    routes = GetArbitrageRoutes(data)
    output = GetArbitrageReturns(routes, data, date)
    wb = load_workbook(filename="data/Triangular_Arbitrage_Binance.xlsx")
    ws = wb['Binance']
    for r in dataframe_to_rows(output, index=False, header=False):
        sr = ' -> '.join([str(elem) for elem in r[3]])
        r[3] = sr
        ws.append(r)
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


def BinanceExchange(minimum_arbitrage_allowance_perc, fee_per_transaction_percent, base_):

    global MIN_ARBITRAGE
    MIN_ARBITRAGE = minimum_arbitrage_allowance_perc
    global BASE
    BASE = base_
    global FEE
    FEE = fee_per_transaction_percent

    socket_spot = 'wss://stream.binance.com/ws/!ticker@arr'
    ws = websocket.WebSocketApp(socket_spot,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()