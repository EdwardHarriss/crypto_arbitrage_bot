import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import signal
import sys
import threading
import websocket
from datetime import datetime
from datetime import timedelta
import json
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import load_workbook
from src.triangular_arbitrage.cex_data import CentralizedExchangeData as CEX_Data

START_TIME = datetime.now()
WRITING_TIME = datetime.now()

def arbitrage(date, data, quantity, volatility):
    date = datetime.fromtimestamp(date/1000).strftime('%Y-%m-%d %H:%M:%S')
    BinanceData.GetArbitrageRoutes(data)
    output = BinanceData.GetArbitrageReturns(data, date, quantity, volatility)
    TIMING_TABLE = BinanceData.timing_df

    #Excel writing
    wb = load_workbook(filename="data/Triangular_Arbitrage_Binance.xlsx")
    ws = wb['Binance']
    for r in dataframe_to_rows(output, index=False, header=False):
        sr = ' -> '.join([str(elem) for elem in r[3]])
        r[3] = sr
        ws.append(r)

    ws = wb['Binance Occ.']
    for r in dataframe_to_rows(output, index=False, header=False):
        occ = [date, 'Binance', len(output), abs(volatility['BTCUSDT'])]
        ws.append(occ)
    wb.save("data/Triangular_Arbitrage_Binance.xlsx")

    CheckTime(date)

def CheckTime(date):
    if START_TIME - datetime.strptime(date, '%Y-%m-%d %H:%M:%S') >= timedelta(minutes = 1):
        WriteTimingTable()
        print("Ending Collection")
        sys.exit(0)
    elif WRITING_TIME - datetime.strptime(date, '%Y-%m-%d %H:%M:%S') >= timedelta(seconds = 10):
        WriteTimingTable()
        WRITING_TIME = datetime.now()

def WriteTimingTable():
    opp_occ = []
    max_length = []
    average_time = []

    for col_name,data_df in TIMING_TABLE.iteritems():   #Finding times
        if col_name == 'Time':
            values_list = data_df.values.tolist()
            print(TIMING_TABLE.to_string())
            for route in values_list:
                list_times = route.split(", ")
                opp_occ.append(len(list_times))
                last_time = None
                i = []
                j = 0
                for x in list_times:
                    if last_time is None:
                        j = j+1
                        last_time = datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
                    elif (datetime.strptime(x, '%Y-%m-%d %H:%M:%S')-timedelta(0,1)) == last_time :
                        j = j+1
                        last_time = datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
                    else: 
                        j = 1
                    i.append(j)
                max_length.append(max(i))
                average_time.append(sum(i)/len(i))


    tmp_df = TIMING_TABLE
    tmp_df['Max Length of Time'] = max_length
    tmp_df['Ave Length of Time'] = average_time
    tmp_df['Occurances'] = opp_occ

    wb = load_workbook(filename="data/Triangular_Arbitrage_Binance.xlsx")
    ws = wb['Binance Timing']
    for r in dataframe_to_rows(tmp_df, index=False, header=False):
        ws.append(r)
    wb.save("data/Triangular_Arbitrage_Binance.xlsx")
    print("### closed ###")


def on_message(ws, message):
    message = json.loads(message)

    def run(*args):
        arbitrage(message[0]['E'], {m['s']: float(m['c']) for m in message}, {m['s']: float(m['v']) for m in message}, {m['s']: float(m['P']) for m in message})

    threading.Thread(target=run).start()


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print(close_status_code)
    print(close_msg)
    print("### closed ###")
    print("Trying to Reconnect")
    BinanceExchange(MIN_ARBITRAGE, FEE, BASE, TIMING_TABLE)

def handler(sig, frame):
    print("Closing")
    sys.exit(0)

signal.signal(signal.SIGINT, handler)

def BinanceExchange(minimum_arbitrage_allowance_perc, fee_per_transaction_percent, base_):
    global MIN_ARBITRAGE, FEE, BASE, BinanceData, TIMING_TABLE
    MIN_ARBITRAGE = minimum_arbitrage_allowance_perc
    FEE = fee_per_transaction_percent
    BASE = base_
    TIMING_TABLE = None

    BinanceData = CEX_Data('Binance', base_, minimum_arbitrage_allowance_perc, fee_per_transaction_percent, TIMING_TABLE)

    socket_spot = 'wss://stream.binance.com/ws/!ticker@arr'
    ws = websocket.WebSocketApp(socket_spot,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()