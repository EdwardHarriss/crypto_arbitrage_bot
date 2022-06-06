from sympy import Sum, summation
from import_data import DataImport
from plot_data import PlotData
from matplotlib import pyplot as plt
from tqdm import tqdm
import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import adfuller

def MovingAverage(sumation, x_values, window_size):
    sumation_df = pd.Series(sumation)
    windows = sumation_df.rolling(window_size)
    moving_averages_sum = windows.mean()

    moving_averages_list = moving_averages_sum.tolist()
    without_nans_sum = moving_averages_list[window_size - 1:]

    x_df = pd.Series(x_values)
    windows = x_df.rolling(window_size)
    moving_averages_x = windows.mean()

    moving_averages_list = moving_averages_x.tolist()
    without_nans_x = moving_averages_list[window_size - 1:]

    return without_nans_sum, without_nans_x

def SumDates(data, x_title, y_title):
    data['date'] = pd.to_datetime(data['date'])
    data.groupby(data['date'].dt.date)

    plt.plot( data['date'].to_list(), data['occ'].to_list())
    plt.xlabel(x_title)
    plt.ylabel(y_title)
    plt.show()


def SumColumnCross(data, column_base, column_sum, x_title, y_title, min, max, averaged):
    max_conv = int(max*10)

    sumation = []
    x_values = []

    for i in tqdm(range(min,max_conv,1)):
        k = i/1
        x_values.append(k)
        if averaged:
            occ = len(data[column_base] == k)
            if len(data[data[column_base] == k]) == 0:
                sumation.append(0)
            else:
                sumation.append(occ/len(data[data[column_base] == k]))
        else:
            sumation.append(len(data[data[column_base] == k]))

    plt.plot(x_values, sumation)
    plt.xlabel(x_title)
    plt.ylabel(y_title)
    plt.show()


def SumColumns(data, column_base, column_sum, x_title, y_title, min, max, averaged):

    max_conv = int(max*10)

    sumation = []
    x_values = []

    for i in tqdm(range(min,max_conv,1)):
        k = i/10
        x_values.append(k)
        if averaged:
            tmp = data.loc[data[column_base] == k, column_sum].sum()
            if len(data[data[column_base] == k]) == 0:
                sumation.append(0)
            else:
                sumation.append(tmp/len(data[data[column_base] == k]))
        else:
            sumation.append(data.loc[data[column_base] == k, column_sum].sum())
        
    #sumation, x_values = MovingAverage(sumation, x_values, window_size=100)

    plt.plot(x_values, sumation)
    plt.xlabel(x_title)
    plt.ylabel(y_title)

    return x_values, sumation

def LineOfBestFit(x, y, x_title, y_title):
    x = np.array(x)
    y = np.array(y)
    a, b = np.polyfit(x, y, 1)

    plt.plot(x, y, color='purple')

    plt.plot(x, a*x+b, color='steelblue', linestyle='--', linewidth=2)

    plt.text(1, 17, 'y = ' + '{:.2f}'.format(b) + ' + {:.2f}'.format(a) + 'x', size=14)

    plt.xlabel(x_title)
    plt.ylabel(y_title)

    plt.show()



if __name__ == "__main__":

    data = DataImport("tri_arb_data", "arb_routes_real")
    df_routes = data.GetTable()

    #data = DataImport("tri_arb_data", "arb_occ_real")
    #df_occ = data.GetTable()

    #df = df[~(df['gain'] < 4)]
    #df = df[~(df['gain'] > 9)]
    #df = df[~(df["btc_vol"] > 12)]

    #df_occ = df_occ.round({'btc_vol':1})

    df_routes = df_routes.round({'vol2':1})

    df_routes = df_routes[~(df_routes['vol2'] > 40)]


    min = int(df_routes['vol2'].min())
    max = df_routes['vol2'].max()

    #print(df_routes.head())

    #SumDates(df_occ, "Date", "Returns")

    x, y = SumColumns(df_routes, "vol2", "gain", "Pair 2 Volatility %", "Average Arbitrage Returns %", min, max, True)

    print(adfuller(x))

    plt.show()

    #LineOfBestFit(x, y,"BTC Volatility %", "Average Arbitrage Occurances",)

    #SumColumnCross(df_routes, "volatility", "gain", "BTC Volatility %", "Arbitrage Occurances", min, max, False)

    #PlotData.ScatterPlotTwoAxis(df, "gain", "volatility", "Arbitrage Occurances", "Volatility BTC %")

    
    #df = pd.read_excel('data/timing_data.xlsx', sheet_name='final')

    #df = df.round({'ave':5})

    #min = 1#int(df['ave'].min())
    #max = df['max'].max()

    #SumColumns(df, "max", "occ", "Max Arbitrage Oppotunity Time s", "Occurances", min, max, False)




