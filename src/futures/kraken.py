import json
import requests
import pandas as pd

#Want to expand for data collection to find optimal time when APR was at its best

NO_OF_DAYS = 365
ENDING = 730

LENGTH = NO_OF_DAYS*6

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

fundingRates = df["MaxFundingRate"].tolist()

balance = 100

for rf in fundingRates:
    balance = balance * (1+rf)

apr = balance - 100

print("APR: ", apr, "%")





