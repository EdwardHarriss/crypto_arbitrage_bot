import src.triangular_arbitrage.binance_data as binance_tri_arb

from src.stat_arb.stat_arb import DataCollection

from src.futures.kraken import KrakenFutArb

if __name__ == "__main__":

    # Only have one section uncommented at once!

    # ----------------------------------------------------------------------- #

    # Triangular Arbitrage 

    #binance_tri_arb.BinanceExchange(minimum_arbitrage_allowance_perc = 0.1, fee_per_transaction_percent = 0.1, base_ = 'USDT', TIMING_TABLE=None)


    # ----------------------------------------------------------------------- #

    # Statistical Arbitrage

    #DataCollection()

    # ----------------------------------------------------------------------- #

    # Futures Arbitrage

    #KrakenFutArb(no_of_days_=365,ending_=192)
