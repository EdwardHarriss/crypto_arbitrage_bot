import warnings

from more_itertools import quantify
warnings.simplefilter(action='ignore', category=FutureWarning)

import signal
import threading
import websocket
from datetime import datetime
import json
import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import load_workbook
from src.triangular_arbitrage.cex_trading import CentralizedExchangeTrading as CEX_Trading
from dotenv import load_dotenv
from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException, BinanceOrderException
import os
import math

BALANCE = 100.0

def MakeTrade(trade):
    #init for test environment
    load_dotenv()
    api_key = os.getenv('binance_api')
    api_secret = os.getenv('binance_secret')
    client = Client(api_key, api_secret)
    #client.API_URL = 'https://testnet.binance.vision/api'

    #initial USDT balance
    balance_init = client.get_asset_balance(asset='USDT')
    step_size_LUT = {1.0:0, 0.1:1, 0.01:2, 0.001:3, 0.0001:4}
    
    #if float(balance_init['free']) < 110:
    #    return 0

    try:
        if trade[0] == 'buy':

            info = client.get_symbol_info(trade[1])
            c1_step = float(info['filters'][2]['stepSize'])
            info = client.get_symbol_info(trade[2])
            c2_step = float(info['filters'][2]['stepSize'])
            info = client.get_symbol_info(trade[3])
            c3_step = float(info['filters'][2]['stepSize'])
        

            q1 = round(BALANCE/trade[4], step_size_LUT[c1_step])
            q2 = round(q1/trade[5], step_size_LUT[c2_step])
            q3 = round(q2*trade[6], step_size_LUT[c3_step])

            #print(trade[1])
            #o1 = client.order_market_buy(
            #    symbol=trade[1],
            #    quantity=q1)

            #print(trade[2])
            #o2 = client.order_market_buy(
            #    symbol=trade[2],
            #    quantity=q2)

            #print(trade[3])
            #o3 = client.order_market_sell(
            #    symbol=trade[3],
            #    quantity=q3)
        
        if trade[0] == 'sell':

            info = client.get_symbol_info(trade[1])
            c1_step = float(info['filters'][2]['stepSize'])
            info = client.get_symbol_info(trade[2])
            c2_step = float(info['filters'][2]['stepSize'])
            info = client.get_symbol_info(trade[3])
            c3_step = float(info['filters'][2]['stepSize'])

            q1 = round(BALANCE/trade[4], step_size_LUT[c1_step])
            q2 = round(q1*trade[5], step_size_LUT[c2_step])
            q3 = round(q2*trade[6], step_size_LUT[c3_step])

            #print(trade[1])
            #o1 = client.order_market_buy(
            #    symbol=trade[1],
            #    quantity=q1)

            #print(trade[2])
            #o2 = client.order_market_sell(
            #    symbol=trade[2],
            #    quantity=q2)

            #print(trade[3])
            #o3 = client.order_market_sell(
            #    symbol=trade[3],
            #    quantity=q3)

    except BinanceAPIException as e:
        print(e)
    except BinanceOrderException as e:
        print(e)
    except Exception as e:
        print(e)


    #balance_after = client.get_asset_balance(asset='USDT')

    #print(float(balance_init['free'])-float(balance_after['free']))

    return q3-BALANCE



def arbitrage_trading(date, data, quantity):
    #date_format = datetime.fromtimestamp(date/1000)
    date = datetime.fromtimestamp(date/1000).strftime('%Y-%m-%d %H:%M:%S')
    date_now = datetime.now()
    trade, predicted_gain = BinanceTrading.GetArbitrage(data, date, quantity)
    #date_after = datetime.now()
    if trade is not None:
        actual_gain = MakeTrade(trade)
        print(trade, " Predicted Gain: ", predicted_gain, "Acc Gain: ", actual_gain)

    ##Timing Analysis
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
    textfile = open("message.txt", "w")

    def run(*args):
        arbitrage_trading(message[0]['E'], {m['s']: float(m['c']) for m in message}, {m['s']: float(m['v']) for m in message})

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