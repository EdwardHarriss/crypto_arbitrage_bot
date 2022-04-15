import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import numpy as np
import threading
import websocket
import datetime
import json
import pandas as pd

class BinanceExchange():

    def __init__(self, minimum_arbitrage_allowance_dollars: float, fee_per_transaction_percent: float, base_):
        #setting name and definitions for env
        self.min_arb = minimum_arbitrage_allowance_dollars
        self.fee = fee_per_transaction_percent
        self.base = base_

    def GetArbitrage():
        socket_spot = 'wss://stream.binance.com/ws/!ticker@arr'
        ws = websocket.WebSocketApp(socket_spot,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
        ws.run_forever()

    def on_error(ws, error):
        print(error)


    def on_close(ws, close_status_code, close_msg):
        print("~~~~~CLOSED~~~~~")

    def on_message(ws, message):
        message = json.loads(message)
        def run(*args):
        output = Arbitrage(message[0]['E'], {m['s']: float(m['c']) for m in message})
        ToExcel(output)
        threading.Thread(target=run).start()

    def Arbitrage(date, data):
        date = datetime.datetime.utcfromtimestamp(date/1000).strftime('%Y-%m-%d %H:%M:%S')
        routes = GetArbitrageRoutes(data)
        output = GetArbitrageReturns(routes, data, date)
        return output

    def GetArbitrageRoutes(self, data):
        listings = data.keys()
        routes = []
        for sym1 in listings:
            if sym1[-len(self.base):] == self.base:
                inter = sym1[:-len(self.base)]
                for sym2 in listings:
                    if (sym2[:-len(self.base)] == inter) and (sym2 != sym1):
                        end = sym2[-len(self.base):]
                        for sym3 in listings:
                            if (end == sym3[:-len(self.base)]) and (sym3[-len(self.base):] == self.base):
                                routes.append([sym1,sym2,sym3])
        return routes

    def GetArbitrageReturns(self, routes, data, date):
        output = pd.DataFrame(columns=['Time','Exchange','Arbitrage Direction','Cryptocurrency Pairs','Gain'])
        for route in routes:

            p1 = 100/data[route[0]] * (1- (self.fee/100))  #buy
            p2 = p1*data[route[1]] * (1- (self.fee/100))   #sell
            p3 = p2*data[route[2]] * (1- (self.fee/100))   #sell

            gain = (p3-100)*100
            if gain > self.min_arb:
                output_data = {'Time' : [date], 'Exchange' : ['binance'], 'Arbitrage Direction' : ['BUY -> SELL -> SELL'], 'Cryptocurrency Pairs' : [route], 'Gain' : [gain]}
                output_frame = pd.DataFrame(output_data)
                output = output.append(output_frame, ignore_index=False)
        return output

        def ToExcel(output):
            wb = load_workbook(filename="data/Triangular_Arbitrage.xlsx")
            ws = wb[self.exchange_name]
            for r in dataframe_to_rows(data_frame, index=False, header=False):
                ws.append(r)
            wb.save("data/Triangular_Arbitrage_Binance.xlsx")
