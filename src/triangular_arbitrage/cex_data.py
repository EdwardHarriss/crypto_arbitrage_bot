import pandas as pd

class CentralizedExchangeData():

    def __init__(self, exchange_name_, base_, minimum_arbitrage_allowance_perc: float, fee_per_transaction_percent: float, TIMING_TABLE):
        #setting name and definitions for env
        self.exchange = exchange_name_
        self.min_arb = minimum_arbitrage_allowance_perc
        self.fee = fee_per_transaction_percent
        self.base = base_
        self.current_time = None

        if TIMING_TABLE is not None:
            self.timing_df = TIMING_TABLE
        else:
            self.timing_df = pd.DataFrame(columns=['route','direction', 'times'])


    def GetArbitrageReturns(self, data, date, quantity, volatility):
        output = pd.DataFrame(columns=['date','route','direction','gain','volatility'])
        for route in self.routes:
            if route[0] == 'sell':
                p1 = 100/data[route[1]] * (1- (self.fee/100))  #buy
                p2 = p1*data[route[2]] * (1- (self.fee/100))   #sell
                p3 = p2*data[route[3]] * (1- (self.fee/100))   #sell
                arb_direction = 'BUY -> SELL -> SELL'

                if (data[route[1]]*quantity[route[1]]) <= 100:
                    p3 = 0


            if route[0] == 'buy':
                p1 = 100/data[route[1]] * (1- (self.fee/100))  #buy
                p2 = p1/data[route[2]] * (1- (self.fee/100))   #buy
                p3 = p2*data[route[3]] * (1- (self.fee/100))   #sell
                arb_direction = 'BUY -> BUY -> SELL'

                if ((data[route[1]]*quantity[route[1]]) <= 100) or ((data[route[2]]*quantity[route[2]]) <= 100):
                    p3 = 0

            del route[0]
            gain = p3-100
            if gain > self.min_arb:
                
        
                vol1 = volatility[route[0]]
                vol2 = volatility[route[1]]
                vol3 = volatility[route[2]]
                ave_vol = (abs(vol1)+abs(vol2)+abs(vol3))/3

                sr = ' -> '.join([str(elem) for elem in route])
                output_data = {'date' : [date], 'route' : [route], 'direction' : [arb_direction], 'gain' : [gain], 'volatility' : [ave_vol], 'vol1' : [vol1], 'vol2' : [vol2], 'vol3' : [vol3]}
                output_frame = pd.DataFrame(output_data)
                output = output.append(output_frame, ignore_index=False)
                
                timing = pd.DataFrame({'route' : [sr], 'direction' : [arb_direction], 'times' : [date]})

                frames = [self.timing_df, timing]
                self.timing_df = pd.concat(frames)
                self.timing_df = self.timing_df.groupby(['route','direction'])['times'].apply(', '.join).reset_index()    

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
        self.routes = routes



    