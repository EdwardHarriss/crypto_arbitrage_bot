from src.centralized_exchange import CentralizedExchange
from ext.excel import *
import src.binance as bi

if __name__ == "__main__":

    bi.BinanceExchange(minimum_arbitrage_allowance_perc = 0.0, fee_per_transaction_percent = 0.075, base_ = 'USDT')

    """
    investment_amount_dollars = 100.0
    minimum_arbitrage_allowance_dollars = 0.5
    fees_per_transaction_percent = 0.075

    reinvest = True
    while(True):
        Binance = CentralizedExchange('binance', investment_amount_dollars, minimum_arbitrage_allowance_dollars, fees_per_transaction_percent, reinvest)
        Binance.GetArbitragePosibilities('USDT')
        Binance.GetArbitrage()
        investment_amount_dollars = Binance.balance

    """
