import matplotlib.pyplot as plt
import pandas as pd

class PlotData():
    def ScatterPlotTwoAxis(data, y_axis, x_axis, y_title, x_title):
        data.plot(kind='scatter', x = x_axis, y = y_axis, color = 'red')
        plt.xlabel(x_title)
        plt.ylabel(y_title)
        plt.show()

    def LineTwoAxis(data, y_axis, x_axis, y_title, x_title):
        data.plot(kind='line', x = x_axis, y = y_axis, color = 'red')
        plt.xlabel(x_title)
        plt.ylabel(y_title)
        plt.show()
    