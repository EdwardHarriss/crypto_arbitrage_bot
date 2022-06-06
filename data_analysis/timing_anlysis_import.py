import pandas as pd
import pymysql
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
pw = os.getenv('pw')
CONN = create_engine("mysql+pymysql://" + "root" + ":" + pw + "@" + "localhost" + "/" + "tri_arb_data")

"""
df_1 = pd.read_sql("arb_timing_real", CONN)
df_2 = pd.read_sql("arb_timing_real_1", CONN)
df_3 = pd.read_sql("arb_timing_real_2", CONN)
df_4 = pd.read_sql("arb_timing_real_3", CONN)
df_5 = pd.read_sql("arb_timing_real_4", CONN)
df_6 = pd.read_sql("arb_timing_real_5", CONN)
df_7 = pd.read_sql("arb_timing_real_6", CONN)
df_8 = pd.read_sql("arb_timing_real_7", CONN)
df_9 = pd.read_sql("arb_timing_real_8", CONN)
df_10 = pd.read_sql("arb_timing_real_9", CONN)
df_11 = pd.read_sql("arb_timing_real_10", CONN)
df_12 = pd.read_sql("arb_timing_real_11", CONN)
df_13 = pd.read_sql("arb_timing_real_12", CONN)

df_1['route'] = df_1['route'] + df_1['direction']
df_2['route'] = df_2['route'] + df_2['direction']
df_3['route'] = df_3['route'] + df_3['direction']
df_4['route'] = df_4['route'] + df_4['direction']
df_5['route'] = df_5['route'] + df_5['direction']
df_6['route'] = df_6['route'] + df_6['direction']
df_7['route'] = df_7['route'] + df_7['direction']
df_8['route'] = df_8['route'] + df_8['direction']
df_9['route'] = df_9['route'] + df_9['direction']
df_10['route'] = df_10['route'] + df_10['direction']
df_11['route'] = df_11['route'] + df_11['direction']
df_12['route'] = df_12['route'] + df_12['direction']
df_13['route'] = df_13['route'] + df_13['direction']

df_1 = df_1.drop('direction', 1)
df_2 = df_2.drop('direction', 1)
df_3 = df_3.drop('direction', 1)
df_4 = df_4.drop('direction', 1)
df_5 = df_5.drop('direction', 1)
df_6 = df_6.drop('direction', 1)
df_7 = df_7.drop('direction', 1)
df_8 = df_8.drop('direction', 1)
df_9 = df_9.drop('direction', 1)
df_10 = df_10.drop('direction', 1)
df_11 = df_11.drop('direction', 1)
df_12 = df_12.drop('direction', 1)
df_13 = df_13.drop('direction', 1)

df = df_1.merge(df_2, on='route')
df = df.merge(df_3, on='route')
df = df.merge(df_4, on='route')
df = df.merge(df_5, on='route')
df = df.merge(df_6, on='route')
df = df.merge(df_7, on='route')
df = df.merge(df_8, on='route')
df = df.merge(df_9, on='route')
df = df.merge(df_10, on='route')
df = df.merge(df_11, on='route')
df = df.merge(df_12, on='route')
df = df.merge(df_13, on='route')



with pd.ExcelWriter('data/timing_data.xlsx') as writer:
    df_1.to_excel(writer, sheet_name='sheet1', index= False)
    df_2.to_excel(writer, sheet_name='sheet2', index= False)
    df_3.to_excel(writer, sheet_name='sheet3', index= False)
    df_4.to_excel(writer, sheet_name='sheet4', index= False)
    df_5.to_excel(writer, sheet_name='sheet5', index= False)
    df_6.to_excel(writer, sheet_name='sheet6', index= False)
    df_7.to_excel(writer, sheet_name='sheet7', index= False)
    df_8.to_excel(writer, sheet_name='sheet8', index= False)
    df_9.to_excel(writer, sheet_name='sheet9', index= False)
    df_10.to_excel(writer, sheet_name='sheet10', index= False)
    df_11.to_excel(writer, sheet_name='sheet11', index= False)
    df_12.to_excel(writer, sheet_name='sheet12', index= False)
    df_13.to_excel(writer, sheet_name='sheet13', index= False)
    df.to_excel(writer, sheet_name='final_2', index= False)

"""
