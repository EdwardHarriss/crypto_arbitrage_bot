import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import numpy as np
import threading
import websocket
import datetime
import json
import pandas as pd


min_arb = 0.5
base = 'USDT'
fee = 0.075

def GetArbitrageReturns(routes, data, date):
    output = pd.DataFrame(columns=['Time','Exchange','Arbitrage Direction','Cryptocurrency Pairs','Gain'])
    for route in routes:

        p1 = 100/data[route[0]] * (1- (fee/100))  #buy
        p2 = p1*data[route[1]] * (1- (fee/100))   #sell
        p3 = p2*data[route[2]] * (1- (fee/100))   #sell

        gain = (p3-100)*100
        if gain > min_arb:
            output_data = {'Time' : [date], 'Exchange' : ['binance'], 'Arbitrage Direction' : ['BUY -> SELL -> SELL'], 'Cryptocurrency Pairs' : [route], 'Gain' : [gain]}
            output_frame = pd.DataFrame(output_data)
            output = output.append(output_frame, ignore_index=False)
    return output



def arbitrage_opportunity(data, pair_id):
    try:
        return (1 - 3*fee) / data[pair.A[pair_id]] * data[pair.B[pair_id]] / data[pair.C[pair_id]], [pair.A[pair_id], pair.B[pair_id], pair.C[pair_id]]
    except:
        return np.nan, np.nan

def GetArbitrageRoutes(data):
    listings = data.keys()
    routes = []
    for sym1 in listings:
        if sym1[-len(base):] == base:
            inter = sym1[:-len(base)]
            for sym2 in listings:
                if (sym2[:-len(base)] == inter) and (sym2 != sym1):
                    end = sym2[-len(base):]
                    for sym3 in listings:
                        if (end == sym3[:-len(base)]) and (sym3[-len(base):] == base):
                            routes.append([sym1,sym2,sym3])
    return routes


def arbitrage(date, data):
    date = datetime.datetime.utcfromtimestamp(date/1000).strftime('%Y-%m-%d %H:%M:%S')
    routes = GetArbitrageRoutes(data)
    output = GetArbitrageReturns(routes, data, date)
    #print(output.to_markdown())

def on_message(ws, message):
    message = json.loads(message)
    #print(message)

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