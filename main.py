from centralized_exchange import CentralizedExchange

if __name__ == "__main__":
    investment_amount_dollars = 100
    minimum_arbitrage_allowance_dollars = 0.5
    fees_per_transaction_percent = 0.075


    Binance = CentralizedExchange('binance', investment_amount_dollars, minimum_arbitrage_allowance_dollars, fees_per_transaction_percent)
    Binance.GetArbitragePosibilities('USDT')
    Binance.GetArbitrage()