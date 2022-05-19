from pickle import NONE
import pandas as pd

class CentralizedExchangeTrading():

    def __init__(self, base_, minimum_arbitrage_allowance_perc: float, fee_per_transaction_percent: float):
        #setting name and definitions for env
        self.min_arb = minimum_arbitrage_allowance_perc
        self.fee = fee_per_transaction_percent
        self.base = base_
        self.routes = []


    def GetArbitrage(self, data, date, quantity):
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
        
        self.routes = routes

        return self.GetArbitrageReturns(data, date, quantity)

    def GetArbitrageReturns(self, data, date, quantity):
        max_gain = self.min_arb
        for route in self.routes:
            if route[0] == 'sell':
                p1 = 100/data[route[1]] * (1- (self.fee/100))  #buy
                p2 = p1*data[route[2]] * (1- (self.fee/100))   #sell
                p3 = p2*data[route[3]] * (1- (self.fee/100))   #sell

                if (data[route[1]]*quantity[route[1]]) <= 100:
                    p3 = 0

            if route[0] == 'buy':
                p1 = 100/data[route[1]] * (1- (self.fee/100))  #buy
                p2 = p1/data[route[2]] * (1- (self.fee/100))   #buy
                p3 = p2*data[route[3]] * (1- (self.fee/100))   #sell

                if ((data[route[1]]*quantity[route[1]]) <= 100) or ((data[route[2]]*quantity[route[2]]) <= 100):
                    p3 = 0

            gain = p3-100
            if gain > max_gain:
                pricing = [data[route[1]], data[route[2]], data[route[3]]]
                output_route = route + pricing
                max_gain = gain

        if max_gain == self.min_arb:
            output_route = None

        return output_route, max_gain