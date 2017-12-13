'''
This script creates graphs of Total Short Interest.
'''

import matplotlib.pyplot as plt
import datetime
import sqlite3 as lite
import time
from functions import Logging
import sys
import pandas as pd

plt.style.use(['ggplot'])


# measure duration of run
startTime = time.time()

# parameters
try:
    mic = sys.argv[1].upper()
except:
    Logging("\nNo argument given, please provide MIC for creating graphs.\n")
    exit()
    

# function to create graph of total short interest
def GraphSI(issuer_tuple, mic):

    issuer = issuer_tuple[0]   #issuer
    title_name = issuer_tuple[1]  #pretty_name
    graph_name = issuer_tuple[2]    #graph_name
    quandl_code = issuer_tuple[3].replace('EURONEXT/', '')
    print(issuer)

    # connections
    db = "{}_ShortInterest.db".format(mic)
    con = lite.connect(db)
    cur = con.cursor()
    conn = lite.connect('Euronext.db')
    curr = conn.cursor()

    # get data  
    issuer_tbl = issuer.strip().lower().replace(' ', '_').replace('-', '_').replace('.','').replace("'","") #total_short_interest.py
    cur.execute('SELECT DATE, TOTAL FROM "{}";'.format(issuer_tbl))
    data = cur.fetchall()


    # ################# TEST #################
    df = pd.DataFrame(data)
    df.columns = ['Date', 'Interest']
    df['Date'] = df['Date'].apply(lambda x : datetime.datetime.strptime(x, '%Y%m%d'))
    #print(df.head())
    #print(df.info())
    
##    # get Euronext prices
##    curr.execute('SELECT Date, "{}" from Prices;'.format(quandl_code))
##    dff = pd.DataFrame(curr.fetchall())
##    dff.columns = ['Date','Price']
##    dff['Date'] = dff['Date'].apply(lambda x : datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
##    dff['DayOfWeek'] = dff.set_index('Date').index.dayofweek
##    #dff = dff[dff['DayOfWeek'].isin([0,5])]
##    #print(dff[dff['DayOfWeek'] == 1])
##    #print(dff.info())
##    #print(dff.head())
##    
##    # combine dataframes
##    combi = pd.merge(df, dff[['Date','Price']] , on='Date', how='left')
##    combi.set_index('Date', inplace=True)
##    #print(combi.info())
##    print(combi.head())
##    
##    # graph
##    #combi.plot()
##    #plt.show()
##    
##    #plt.show()
##    x = combi.index
##    y = combi['Interest']
##    y2 = combi['Price']
##
##    fig, ax1 = plt.subplots()
##    ax2 = ax1.twinx()
##    ax1.plot(x, y, linewidth=2.0)
##    ax2.plot(x, y2, linestyle='solid', color='k',linewidth=2.0)
##    
##    ax1.set_xlabel('Date')
##    ax1.set_ylabel('Short Interest (%)')
##    ax2.set_ylabel('Price', color='k')
##
##    ax1.fill_between(x, 0, y, alpha=0.6)
##    plt.grid(False)
##    plt.title('{} ({})'.format(title_name, mic))
##    plt.show()
##    plt.clf()


    # ################# TEST #################

    # closing connections
    cur.close()
    con.close()
    curr.close()
    conn.close()
    
    # creating x-axis and y-axis
    x = [datetime.datetime.strptime(d[0], "%Y%m%d") for d in data]
    y = [d[1] for d in data]
   
    # labeling axis
    plt.xlabel('Date')
    plt.ylabel('Short Interest (%)')
    plt.title('{} ({})'.format(title_name, mic))
    plt.grid(True)
    plt.fill_between(x, 0, y, alpha=0.6)

    # saving graph
    plt.plot(x,y, linewidth=2.0)
    #plt.show()
    
    #save_name = issuer.strip().lower().replace(' ', '_').replace('-','_').replace('.','').replace("'","")  #total_short_interest.py
    location = 'static/graphs/{}'.format(graph_name)
    plt.savefig(location, format='png')

    # close/refresh plot
    plt.close()
    


# create function to get all issuers
def GetIssuersMIC(mic):
    
    # connections
    db = "SummaryDB.db"
    con = lite.connect(db)
    cur = con.cursor()

    # get all issuers
    cur.execute('''SELECT DISTINCT ISSUER, PRETTY_NAME, GRAPH_NAME, QUANDL_CODE FROM Issuers WHERE MIC="{}" ORDER BY ISSUER;'''.format(mic))
    issuers = [(i[0],i[1],i[2],i[3]) for i in cur.fetchall()]
    #print(len(issuers))
    #print(issuers)

    # closing connections
    cur.close()
    con.close()
    
    return issuers



# only run when this is main script
if __name__ == "__main__":
    
    Logging("Creating Graphs for MIC: {}\n".format(mic))
    
    # get issuers for MIC
    issuers = GetIssuersMIC(mic)
    
    # run function for every issuer
    for iss in issuers:
        try:
            GraphSI(iss, mic)
        except Exception as e:
            print("Error with {}".format(iss))
            print("Error: {}".format(str(e)))

    # print duration of run
    endTime = time.time()
    Logging("\nScript ran for {} seconds.".format(round(endTime-startTime,2)))

