from pickle import NONE
import ccxt
from datetime import datetime
import pandas as pd


class CentralizedExchange():

    def __init__(self, exchange_name_, investment_amount_dollars: float, minimum_arbitrage_allowance_dollars: float, fees_per_transaction_percent: float):
        #setting name and definitions for env
        self.exchange_name = exchange_name_
        self.exchange = getattr(ccxt, exchange_name_)()
        self.investment = investment_amount_dollars
        self.min_profit = minimum_arbitrage_allowance_dollars
        self.fees = fees_per_transaction_percent
        self.ArbData = pd.DataFrame(columns=['Time', 'Exchange', 'Arbitrage Direction', 'Cryptocurrencies', 'Profit/Loss'])

    def GetCurrentPrice(self, ticker):
        current_ticker_details = self.exchange.fetch_ticker(ticker)
        ticker_price = current_ticker_details['close'] if current_ticker_details is not None else None
        return ticker_price


    def GetArbitragePosibilities(self, base_coin):
        markets = self.exchange.fetchMarkets()
        market_symbols = [market['symbol'] for market in markets]
    
        self.combinations = []
        for sym1 in market_symbols:   
            sym1_token1 = sym1.split('/')[0]
            sym1_token2 = sym1.split('/')[1]   
            if (sym1_token2 == base_coin):
                for sym2 in market_symbols:
                    sym2_token1 = sym2.split('/')[0]
                    sym2_token2 = sym2.split('/')[1]
                    if (sym1_token1 == sym2_token2):
                        for sym3 in market_symbols:
                            sym3_token1 = sym3.split('/')[0]
                            sym3_token2 = sym3.split('/')[1]
                            if((sym2_token1 == sym3_token1) and (sym3_token2 == sym1_token2)):
                                combination = {
                                    'base':sym1_token2,
                                    'intermediate':sym1_token1,
                                    'ticker':sym2_token1,
                                }
                                self.combinations.append(combination)

    def GetArbitrage(self):
        for combination in self.combinations:
            base = combination['base']
            intermediate = combination['intermediate']
            ticker = combination['ticker']


            p1 = f'{intermediate}/{base}' 
            p2 = f'{ticker}/{intermediate}'  
            p3 = f'{ticker}/{base}'     

            #        TICKER
            #       ^     ^
            #      /       \
            #     v         v           
            #  BASE  <-->   INTERMEDIATE  
 
            try:
                self.TriangularArbitrage(p1, p2, p3, 'clockwise')
            except ZeroDivisionError:  # error thrown if ccxt not updated with value 
                continue
            
            try:
                self.TriangularArbitrage(p3, p2, p1, 'anticlockwise')
            except ZeroDivisionError: # error thrown if ccxt not updated with value 
                continue


    def TriangularArbitrage(self, pair1, pair2, pair3, arb_type):

        final_price = 0.0
        if(arb_type == 'clockwise'):
            final_price = self.CheckClockWiseArbitrage(pair1, pair2, pair3)
            
        elif(arb_type == 'anticlockwise'):
            final_price = self.CheckAnticlockWiseArbitrage(pair1, pair2, pair3)
            
        profit_loss = self.CheckProfit(final_price)

        if profit_loss>0:
            if(arb_type == 'clockwise'):
                output_str = "BUY -> BUY -> SELL"
            else:
                output_str = "BUY -> SELL -> SELL"
            data = {'Time' : [datetime.now().strftime('%H:%M:%S')], 'Exchange' : [self.exchange_name], 'Arbitrage Direction' : [output_str], 'Cryptocurrency Pairs' : [{pair1,pair2,pair3}], 'Profit/Loss' : [round(final_price-self.investment,4)]}
            data_frame = pd.DataFrame(data)
            self.ArbData.append(data_frame)
            print(data_frame.to_markdown())
            print("###########################################")

    def CheckClockWiseArbitrage(self, pair1, pair2, pair3):
        
        current_price1 = self.GetCurrentPrice(pair1)
        final_price = 0
        
        if current_price1 is not None:
            buy_quantity1 = round(self.investment / current_price1, 8)
               
            current_price2 = self.GetCurrentPrice(pair2)
            if current_price2 is not None:
                buy_quantity2 = round(buy_quantity1 / current_price2, 8)
                  
                current_price3 = self.GetCurrentPrice(pair3)
                if current_price3 is not None:
                    final_price = round(buy_quantity2 * current_price3,8)
        
        elif current_price1 or current_price2 or current_price3 is None:
            return 0
        return final_price

    def CheckAnticlockWiseArbitrage(self, pair1, pair2, pair3):
        
        current_price1 = self.GetCurrentPrice(pair1)
        final_price = 0
        
        if current_price1 is not None:
            buy_quantity1 = round(self.investment / current_price1, 8)
               
            current_price2 = self.GetCurrentPrice(pair2)
            if current_price2 is not None:
                buy_quantity2 = round(buy_quantity1 * current_price2, 8)
                  
                current_price3 = self.GetCurrentPrice(pair3)
                if current_price3 is not None:
                    final_price = round(buy_quantity2 * current_price3,8)
        
        elif current_price1 or current_price2 or current_price3 is None:
            return 0
        return final_price

    def CheckProfit(self, final_price):
        apprx_brokerage = self.fees * self.investment/100 * 3
        min_profitable_price = self.investment + apprx_brokerage + self.min_profit
        profit = round(final_price - min_profitable_price,8)
        return profit



    