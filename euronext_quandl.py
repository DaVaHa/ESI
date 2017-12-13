'''
Get Euronext prices from Quandl.
'''

import quandl
import pandas as pd
import matplotlib.pyplot as plt
import time
import sqlite3 as lite
import datetime

startTime = time.time()

# create database
con = lite.connect('Euronext.db')


# create full database with codes.
url = 'https://www.quandl.com/data/EURONEXT-Euronext-Stock-Exchange/documentation/metadata'

def UpdateEuronextMetaData():
    con = lite.connect('Euronext.db')
    dfs = []
    for i in range(1,31):
        try:
            url = 'https://www.quandl.com/api/v3/datasets.csv?database_code=EURONEXT&per_page=100&sort_by=id&page=' + str(i) + '1&api_key=4s95ha21dSPT_WqV9uZz'
            df = pd.read_csv(url)
            dfs.append(df)
        except Exception as e:
            print("Problem with page {}".format(i))
            print("Error: {}".format(str(e)))

    df_all = pd.concat(dfs)
    df_all.to_sql('metadata', con, if_exists='replace')

    con.close()


def UpdateEuronextPrices():

    con = lite.connect('Euronext.db')
    conn = lite.connect('SummaryDB.db')
    curr = conn.cursor()
    
    # set key for 50+ api calls
    quandl.ApiConfig.api_key = '4s95ha21dSPT_WqV9uZz'

    # get quandl keys out of DB 
    curr.execute('select DISTINCT QUANDL_CODE from Issuers where QUANDL_CODE is not null and DELETED=0;')
    quandl_keys = [k[0] for k in curr.fetchall()]

    # loop over all keys and add dataframes to list
    dfs = []
    for key in quandl_keys:
        df = quandl.get(key)
        close = pd.DataFrame(df['Last'])
        close.columns = [key.replace('EURONEXT/', '')]
        dfs.append(close)

    # concatentate dataframes
    df_all = pd.concat(dfs, axis=1)
    df_all = df_all.fillna(method='ffill')
    
    #print(df_all.head())
    #print(df_all.info())

    # add date string YYYYMMDD to dataframe
    df_all['DateString'] = df_all.index
    df_all['DateString'] = df_all['DateString'].apply(lambda x : datetime.datetime.strftime(x, '%Y%m%d'))

    #print(df_all.head())
    #print(df_all.info())
    
    # df to sqlite
    df_all.to_sql('Prices', con, if_exists='replace')

    
    curr.close()
    conn.close()
    con.close()


    
if __name__ == '__main__':

    # update metadata
    #UpdateEuronextMetaData()

    # update all prices
    UpdateEuronextPrices()


    if con:
        con.close()
    
    endTime = time.time()
    print("Script runtime: {} sec".format(round(endTime - startTime)))

