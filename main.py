#import src.binance_data as binance_data
#import src.triangular_arbitrage.binance_trading as binance_trading

import src.futures.binance_per_futures as binance_per_futures
#import src.futures.kraken_futures as kraken_futures

if __name__ == "__main__":

    #binance_trading.BinanceExchange(minimum_arbitrage_allowance_perc = 0.5, fee_per_transaction_percent = 0.1, base_ = 'USDT')
    #binance_data.BinanceExchange(minimum_arbitrage_allowance_perc = 0.2, fee_per_transaction_percent = 0.1, base_ = 'USDT')
    binance_per_futures.BinancePerpetualFutures(minimum_funding_rate = 0, fee_per_transaction_percent = 0.1,)
    #kraken_futures.KrakenPerpetualFutures(minimum_funding_rate = 0, fee_per_transaction_percent = 0.1)
