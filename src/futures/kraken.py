import requests
import pandas as pd
from matplotlib import pyplot as plt

#Want to expand for data collection to find optimal time when APR was at its best

NO_OF_DAYS = 365
ENDING = 139

LENGTH = NO_OF_DAYS*6
ENDING = ENDING*6

#Getting Historic Data

BTC_pi = requests.get("https://futures.kraken.com/derivatives/api/v4/historicalfundingrates?symbol=PI_XBTUSD")
ETH_pi = requests.get("https://futures.kraken.com/derivatives/api/v4/historicalfundingrates?symbol=PI_ETHUSD")
LTC_pi = requests.get("https://futures.kraken.com/derivatives/api/v4/historicalfundingrates?symbol=PI_LTCUSD")
XRP_pi = requests.get("https://futures.kraken.com/derivatives/api/v4/historicalfundingrates?symbol=PI_XRPUSD")
BCH_pi = requests.get("https://futures.kraken.com/derivatives/api/v4/historicalfundingrates?symbol=PI_BCHUSD")

#Converting to dictionary

btc_dict = BTC_pi.json()
eth_dict = ETH_pi.json()
ltc_dict = LTC_pi.json()
xrp_dict = XRP_pi.json()
bch_dict = BCH_pi.json()

print(btc_dict)

#converting to dataframe

btc_dict = pd.DataFrame(btc_dict['rates'])
eth_dict = pd.DataFrame(eth_dict['rates'])
ltc_dict = pd.DataFrame(ltc_dict['rates'])
xrp_dict = pd.DataFrame(xrp_dict['rates'])
bch_dict = pd.DataFrame(bch_dict['rates'])

#Using last LENGTH values

btc_dict = btc_dict.head(-ENDING).reset_index()
eth_dict = eth_dict.head(-ENDING).reset_index()
ltc_dict = ltc_dict.head(-ENDING).reset_index()
xrp_dict = xrp_dict.head(-ENDING).reset_index()
bch_dict = bch_dict.head(-ENDING).reset_index()

btc_dict = btc_dict.tail(LENGTH).reset_index()
eth_dict = eth_dict.tail(LENGTH).reset_index()
ltc_dict = ltc_dict.tail(LENGTH).reset_index()
xrp_dict = xrp_dict.tail(LENGTH).reset_index()
bch_dict = bch_dict.tail(LENGTH).reset_index()

df = btc_dict
df = df.drop("relativeFundingRate", axis = 1)
df = df.drop("index", axis = 1)
df["fundingRateETH"] = eth_dict["fundingRate"]
df["fundingRateLTC"] = ltc_dict["fundingRate"]
df["fundingRateXRP"] = xrp_dict["fundingRate"]
df["fundingRateBCH"] = bch_dict["fundingRate"]
df["fundingRateBTC"] = df["fundingRate"]
df = df.drop("fundingRate", axis=1)

#Finding Max Profit route

index = df.drop("timestamp", axis = 1)
maxValue = index.idxmax(axis  = 1).tolist()

maxValueRoute = list()
for i in maxValue:
    if i == 'fundingRateBTC':
        maxValueRoute.append('BTC')
    elif i == 'fundingRateETH':
        maxValueRoute.append('ETH')
    elif i == 'fundingRateXRP':
        maxValueRoute.append('XRP')
    elif i == 'fundingRateBCH':
        maxValueRoute.append('BCH')
    else:
        maxValueRoute.append('LTC')

#Finding Maximum Funding Rates

df["MaxFundingRate"] = df[["fundingRateETH", "fundingRateLTC", "fundingRateXRP", "fundingRateBCH", "fundingRateBTC"]].max(axis=1)
df = df.drop("fundingRateETH", axis=1)
df = df.drop("fundingRateLTC", axis=1)
df = df.drop("fundingRateXRP", axis=1)
df = df.drop("fundingRateBCH", axis=1)
df = df.drop("fundingRateBTC", axis=1)

#Finding Profit

df["MaxFundingRate"] = df["MaxFundingRate"] + 1

fundingRatesCumProd = df["MaxFundingRate"].cumprod().tolist()

x = list()

for i in range(0, len(fundingRatesCumProd)):
    x.append(i/6)

apr = (fundingRatesCumProd[-1]-1)*100

print("APR: ", apr, "%")

btc_df = pd.read_csv('binance_btc.csv')
btc_df = btc_df.iloc[::4, :]
btc_df = btc_df.iloc[::-1]
btc_df = btc_df.head(LENGTH)
btc_df = btc_df.drop("open", axis=1)
btc_df = btc_df.drop("high", axis=1)
btc_df = btc_df.drop("low", axis=1)
btc_df = btc_df.drop("Volume BTC", axis=1)
btc_df = btc_df.drop("Volume GBP", axis=1)
btc_df = btc_df.drop("tradecount", axis=1)
btc_df = btc_df.drop("unix", axis=1)
btc_df = btc_df.drop("symbol", axis=1)
btc_df["close"] = btc_df["close"].pct_change()
btc_df["close"] = btc_df["close"] + 1
btc = btc_df["close"].cumprod().tolist()

x_btc = list()

for i in range(len(btc)):
    x_btc.append(i/6)

#print(btc_df.to_string())

fig, ax1 = plt.subplots()

#ax2 = ax1.twinx()
ax1.plot(x,fundingRatesCumProd)
#ax2.plot(x_btc, btc, 'g-')

ax1.set_xlabel('Days since start')
ax1.set_ylabel('Arbitrage Returns', color='b')
#ax2.set_ylabel('BTC Returns', color='g')

plt.show()






