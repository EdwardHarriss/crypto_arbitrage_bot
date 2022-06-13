import os
from dotenv import load_dotenv
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import metrics
import seaborn as sn
import matplotlib.pyplot as plt
import numpy as np
import sys
from statistics import mean

np.set_printoptions(threshold=sys.maxsize)

logistic_regression = LogisticRegression(C=10**12, penalty='l2')

BUY_BALANCE = 100
SELL_BALANCE = 100

def DataCollectionTesting(df):
    X = df.drop(columns=['E','s','P'])
    X = X.astype(float)

    #X_norm = (X-X.mean())/X.std() #mean norm
    X_norm = (X-X.min())/(X.max()-X.min()) #min-max norm

    return LogRegressionTesting(X_norm, df)

def DataCollectionTraining():
    load_dotenv()
    pw = os.getenv('pw')

    CONN = create_engine("mysql+pymysql://" + "root" + ":" + pw + "@" + "localhost" + "/" + "binance_data")


    sql_query = pd.read_sql_query ('''
                                SELECT
                                *
                                FROM raw_data
                                ''', CONN)

    df = pd.DataFrame(sql_query, columns = ['E', 's', 'P', 'c', 'o', 'h', 'l', 'v'])
    df['P'] = pd.to_numeric(df['P'], downcast="float")
    ave_p = df['P'].median()

    y_values = []
    for i,j in df.iterrows():
        p_change = df['P'][i]
        if p_change <= ave_p:
            y_values.append(0)
        else:
            y_values.append(1)

    y = pd.DataFrame({'y': y_values})
    X = df.drop(columns=['E','s','P'])
    X = X.astype(float)

    #X_norm = (X-X.mean())/X.std() #mean norm
    X_norm = (X-X.min())/(X.max()-X.min()) #min-max norm


    return X_norm,y

def LogRegressionTraining():
    global logistic_regression
    X, y = DataCollectionTraining()

    X_train,X_test,y_train,y_test = train_test_split(X,y,test_size=0.25,random_state=0)

    logistic_regression.fit(X_train,y_train.values.ravel())
    y_pred=logistic_regression.predict(X_test)
    confusion_matrix = pd.crosstab(y_test['y'], y_pred, rownames=['Actual'], colnames=['Predicted'])
    sn.heatmap(confusion_matrix, annot=True)

    print('Accuracy: ',metrics.accuracy_score(y_test, y_pred))
    plt.show()

def LogRegressionTesting(X, df):
    global logistic_regression
    y_prob = logistic_regression.predict_proba(X)
    y_pred = logistic_regression.predict(X)

    i = 0


    buy_coins = pd.DataFrame()
    sell_coins = pd.DataFrame()
    for x in y_pred:
        if x == 1:
            if y_prob[i][1] == 1.0:
                buy_coins = buy_coins.append(df.iloc[i])
        else:
            if y_prob[i][0] > y_prob[i][1]:
                 sell_coins = sell_coins.append(df.iloc[i])
        i = i + 1

    return buy_coins, sell_coins

