import json
import threading
import signal
import sys
from tkinter import FIRST
import websocket
import datetime
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
import pymysql
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import load_workbook


import src.stat_arb.ml as lr

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from statistics import mean

COUNT = 0
COUNT_TILL = 5#1440 for 24 mins
FIRST_COUNT = True
load_dotenv()
pw = os.getenv('pw')

BUY_POSITIONS = pd.DataFrame()
SELL_POSITIONS = pd.DataFrame()

CONN = create_engine("mysql+pymysql://" + "root" + ":" + pw + "@" + "localhost" + "/" + "binance_data")

def DataPreProcessing(df):
    global COUNT
    coins = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT',
    'DOGEUSDT', 'AVAXUSDT', 'TRXUSDT', 'ETCUSDT', 'APEUSDT', 'SANDUSDT']

    df = df.drop('e', axis=1)  #removing title of data
    df = df.drop('q', axis=1)  #removing quantity of base (will be the same USDT)
    df = df.drop('O', axis=1)
    df = df.drop('C', axis=1)
    df = df.drop('F', axis=1)
    df = df.drop('L', axis=1)
    df = df.drop('n', axis=1)
    df = df.drop('p', axis=1)
    df = df.drop('w', axis=1)
    df = df.drop('x', axis=1)
    df = df.drop('Q', axis=1)
    df = df.drop('b', axis=1)
    df = df.drop('B', axis=1)
    df = df.drop('a', axis=1)
    df = df.drop('A', axis=1)
    df = df[df['s'].isin(coins)]

    COUNT = COUNT + 1
    print((COUNT/COUNT_TILL) * 100)
    if COUNT >= COUNT_TILL:
        COUNT = 0
        on_FullData(df)
    else:
        df.to_sql('raw_data',con=CONN,if_exists ='append',index=False)

def on_FullData(df):
    global BUY_POSITIONS, SELL_POSITIONS, FIRST_COUNT, CONN
    #Train ML
    lr.LogRegressionTraining()
    new_buy, new_sell = lr.DataCollectionTesting(df)
    sql = text('DELETE FROM raw_data')
    result = CONN.execute(sql)

    if FIRST_COUNT == True:
        print("Setting First Positions")
        BUY_POSITIONS = new_buy
        SELL_POSITIONS = new_sell
        FIRST_COUNT = False

    elif (len(new_buy.index) and len(new_sell.index)) >= 2:

        new_buy_head = new_buy.head(2)
        new_sell_head = new_sell.head(2)

        new_buy_head.to_sql('buy_data',con=CONN,if_exists ='append',index=False)
        new_sell_head.to_sql('sell_data',con=CONN,if_exists ='append',index=False)

        buy_coin_symbol1 = BUY_POSITIONS.loc[BUY_POSITIONS.index[0], 's'] #symbol of purchasing
        buy_coin_symbol2 = BUY_POSITIONS.loc[BUY_POSITIONS.index[1], 's']

        sell_coin_symbol1 = SELL_POSITIONS.loc[SELL_POSITIONS.index[0], 's']
        sell_coin_symbol2 = SELL_POSITIONS.loc[SELL_POSITIONS.index[1], 's']

        index_1_buy = df.index[df['s'] == buy_coin_symbol1].tolist() #index of symbol in new data
        index_2_buy = df.index[df['s'] == buy_coin_symbol2].tolist()

        index_1_sell = df.index[df['s'] == sell_coin_symbol1].tolist()
        index_2_sell = df.index[df['s'] == sell_coin_symbol2].tolist()

        p2_buy1 = df.loc[index_1_buy[0], 'c']  #prices of new data symbols
        p2_buy2 = df.loc[index_2_buy[0], 'c']

        p2_sell1 = df.loc[index_1_sell[0], 'c']
        p2_sell2 = df.loc[index_2_sell[0], 'c']

        p1_buy1 = BUY_POSITIONS.loc[BUY_POSITIONS.index[0], 'c']  #prices of old data symbols
        p1_buy2 = BUY_POSITIONS.loc[BUY_POSITIONS.index[1], 'c']

        p1_sell1 = SELL_POSITIONS.loc[SELL_POSITIONS.index[0], 'c']
        p1_sell2 = SELL_POSITIONS.loc[SELL_POSITIONS.index[1], 'c']

        r1 = (float(p2_buy1)/float(p1_buy1)-1) + (float(p2_buy2)/float(p1_buy2)-1) + (float(p1_sell1)/float(p2_sell1)-1) + (float(p1_sell2)/float(p2_sell2)-1)

        BUY_POSITIONS = new_buy
        SELL_POSITIONS = new_sell

        print("Return: ", r1)

        time = BUY_POSITIONS.loc[BUY_POSITIONS.index[0], 'E']

        results_df = pd.DataFrame({'time': [time], 'return': [r1]})
        #results_df.to_sql('return_data',con=CONN,if_exists ='append',index=False)


        wb = load_workbook(filename="data/Stat_Arb_Binance.xlsx")
        ws = wb['Stat Arb']
        for r in dataframe_to_rows(results_df, index=False, header=False):
            ws.append(r)
        wb.save("data/Stat_Arb_Binance.xlsx")

def on_message(ws, message):
    message = json.loads(message)
    df = pd.DataFrame(message)

    def run(*args):
        DataPreProcessing(df)

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
    quit()

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