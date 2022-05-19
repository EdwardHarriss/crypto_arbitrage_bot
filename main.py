#import src.binance_data as binance_data
import src.binance_trading as binance_trading

if __name__ == "__main__":

    binance_trading.BinanceExchange(minimum_arbitrage_allowance_perc = 0.5, fee_per_transaction_percent = 0.1, base_ = 'USDT')
    #binance_data.BinanceExchange(minimum_arbitrage_allowance_perc = 0.2, fee_per_transaction_percent = 0.1, base_ = 'USDT')

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
