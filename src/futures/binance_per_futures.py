from ast import Global
import threading
import websocket
import json
import signal
import datetime
from binance.client import Client
from dotenv import load_dotenv
import time
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import pandas as pd
import os

BALANCE = 100

def cuttoff(data):
    output_dict = dict()
    for sym in data:
        if data[sym] > MIN_FUNDING:
            output_dict.update({ sym: data[sym] })
    return output_dict

def Arbitrage(date, funding_rates, funding_times, pricing_data):
    funding_rate_cuttoff = cuttoff(funding_rates)
    highest_funding_rate_sym = max(funding_rate_cuttoff, key=funding_rate_cuttoff.get)
    print(highest_funding_rate_sym, funding_rates[highest_funding_rate_sym])
    if datetime.datetime.utcfromtimestamp(funding_times[highest_funding_rate_sym]/1000) - datetime.datetime.utcfromtimestamp(date/1000)  <= datetime.timedelta(seconds=3):

        output_data = {'Time': funding_times[highest_funding_rate_sym], 'Exchange': 'Binance', 'Gain': funding_rates[highest_funding_rate_sym]}
        out_df = pd.DataFrame(output_data)
        wb = load_workbook(filename="data/Future_Arbitrage.xlsx")
        ws = wb['Binance']
        for r in dataframe_to_rows(out_df, index=False, header=False):
            ws.append(r)
        wb.save("data/Future_Arbitrage.xlsx")
        print("### closed ###")



def on_message(ws, message):
    message = json.loads(message)
    textfile = open("message.txt", "w")

    def run(*args):
        Arbitrage(message[0]['E'], {m['s']: float(m['r']) for m in message}, {m['s']: float(m['T']) for m in message}, {m['s']: float(m['P']) for m in message})

    threading.Thread(target=run).start()


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print(close_status_code)
    print(close_msg)
    print("### closed ###")

def handler(signum, frame):
    print("### closed ###")
    exit(1)

signal.signal(signal.SIGINT, handler)


def BinancePerpetualFutures(minimum_funding_rate, fee_per_transaction_percent):
    global MIN_FUNDING, FEE, client, long_position, short_position
    MIN_FUNDING = minimum_funding_rate
    FEE = fee_per_transaction_percent

    socket_spot = 'wss://fstream.binance.com/ws/!markPrice@arr'
    ws = websocket.WebSocketApp(socket_spot,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()