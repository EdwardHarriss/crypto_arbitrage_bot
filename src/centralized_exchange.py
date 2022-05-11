from pickle import NONE
from datetime import datetime
import pandas as pd
from ext.excel import *  

class CentralizedExchange():

    def __init__(self, exchange_name_, base_, minimum_arbitrage_allowance_perc: float, fee_per_transaction_percent: float):
        #setting name and definitions for env
        self.exchange = exchange_name_
        self.min_arb = minimum_arbitrage_allowance_perc
        self.fee = fee_per_transaction_percent
        self.base = base_

    def GetArbitrageReturns(self, routes, data, date):
        output = pd.DataFrame(columns=['Time','Exchange','Arbitrage Direction','Cryptocurrency Pairs','Gain'])
        for route in routes:
            if route[0] == 'sell':
                p1 = 100/data[route[1]] * (1- (self.fee/100))  #buy
                p2 = p1*data[route[2]] * (1- (self.fee/100))   #sell
                p3 = p2*data[route[3]] * (1- (self.fee/100))   #sell
                arb_direction = 'BUY -> SELL -> SELL'

            if route[0] == 'buy':
                p1 = 100/data[route[1]] * (1- (self.fee/100))  #buy
                p2 = p1/data[route[2]] * (1- (self.fee/100))   #buy
                p3 = p2*data[route[3]] * (1- (self.fee/100))   #sell
                arb_direction = 'BUY -> BUY -> SELL'

            del route[0]
            gain = p3-100
            if gain > self.min_arb:
                output_data = {'Time' : [date], 'Exchange' : [self.exchange], 'Arbitrage Direction' : [arb_direction], 'Cryptocurrency Pairs' : [route], 'Gain' : [gain]}
                output_frame = pd.DataFrame(output_data)
                output = output.append(output_frame, ignore_index=False)
        print(len(output))
        return output

    def GetArbitrageRoutes(self, data):
        listings = data.keys()
        routes = []

        ## SELL ROUTE ####
        for sym1 in listings:
            if sym1[-len(self.base):] == self.base:
                inter = sym1[:-len(self.base)]
                for sym2 in listings:
                    if (sym2[:-len(inter)] == inter) and (sym2 != sym1):
                        end = sym2[-len(inter):]
                        for sym3 in listings:
                            if (end == sym3[:-len(end)]) and (sym3[-len(self.base):] == self.base):
                                routes.append(['sell',sym1,sym2,sym3])
        ## BUY ROUTE #####    
        for sym1 in listings:
            if sym1[-len(self.base):] == self.base:
                inter = sym1[:-len(self.base)]
                for sym2 in listings:
                    if (sym2[-len(inter):] == inter) and (sym2 != sym1):
                        end = sym2[:-len(inter)]
                        for sym3 in listings:
                            if (end == sym3[:-len(end)]) and (sym3[-len(self.base):] == self.base):
                                routes.append(['buy',sym1,sym2,sym3])
        return routes



    