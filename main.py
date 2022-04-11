from src.centralized_exchange import CentralizedExchange
from ext.excel import *

if __name__ == "__main__":
    investment_amount_dollars = 100.0
    minimum_arbitrage_allowance_dollars = 0.5
    fees_per_transaction_percent = 0.075

    reinvest = True
    while(True):
        Binance = CentralizedExchange('binance', investment_amount_dollars, minimum_arbitrage_allowance_dollars, fees_per_transaction_percent, reinvest)
        Binance.GetArbitragePosibilities('USDT')
        Binance.GetArbitrage()
