from src.centralized_exchange import CentralizedExchange
from ext.excel import *
import pandas as pd

if __name__ == "__main__":
    investment_amount_dollars = 100
    minimum_arbitrage_allowance_dollars = 0.5
    fees_per_transaction_percent = 0.075

    reinvest = True


    Binance = CentralizedExchange('binance', investment_amount_dollars, minimum_arbitrage_allowance_dollars, fees_per_transaction_percent, reinvest)
    ArbData = Binance.ArbData
    with pd.ExcelWriter("data/Triangular_Arbitrage.xlsx", mode = "a", engine="openpyxl", if_sheet_exists='overlay') as writer:
        ArbData.to_excel(writer, sheet_name="Binance", index=False)

    Binance.GetArbitragePosibilities('USDT')
    Binance.GetArbitrage()
