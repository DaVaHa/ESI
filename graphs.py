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
##mic = 'XBRU'

# function to create graph of total short interest
def GraphSI(issuer_tuple, mic):

    issuer = issuer_tuple[0]   #issuer
    title_name = issuer_tuple[1]  #pretty_name
    graph_name = issuer_tuple[2]    #graph_name
    if issuer_tuple[3] == None:
        return None
    quandl_code = issuer_tuple[3].replace('EURONEXT/', '')  #quandl_code

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

    # short interest
    df = pd.DataFrame(data)
    df.columns = ['Date', 'Interest']
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    #print(df.head())
    #print(df.info())
    
    # get Euronext prices
    curr.execute('SELECT Date, "{}" from Prices_incl_CA;'.format(quandl_code))
    dff = pd.DataFrame(curr.fetchall())
    dff.columns = ['Date','Price']
    dff['Date'] = pd.to_datetime(dff['Date'])
    dff.set_index('Date', inplace=True)
    #print(dff.info())
    #print(dff.head())
    
    # combine dataframes
    combi = pd.merge(df, dff, how='left', left_index=True, right_index=True)
    combi['Price'] = combi['Price'].fillna(method='ffill')
    #print(combi.head())
    #print(combi.info())

    # max short interest
    max_si = combi['Interest'].max()
    #print(max_si)
    
    # graphing
    color_si = [213/255,94/255,0]
    fig, ax1 = plt.subplots()
    ax1.plot(combi['Interest'],linewidth=2.0,color=color_si)
    ax1.set_ylabel('Short Interest (%)')
    ax1.fill_between(combi.index, 0, combi['Interest'],alpha=0.6, facecolor=color_si)
    #ax1.grid(False)
    if max_si > 15: ax1.set_ylim([-0.2,20.6])
    elif max_si > 10 : ax1.set_ylim([-0.2,15.6])
    elif max_si > 5 : ax1.set_ylim([-0.2,10.6])
    else: ax1.set_ylim([-0.2,5.3])

    
    color_price = [0,114/255,178/255]
    ax2 = ax1.twinx()
    ax2.plot(combi['Price'], ##color='#0033cc',
             linewidth=1.3, color=color_price)
    ax2.set_ylabel('Price')
    ax2.grid(False)
    
    # graph
    plt.title('{} ({})'.format(title_name, mic), fontsize=17)
    plt.suptitle('Short Interest vs Stock Price', y=0.87, fontsize=11)
    #plt.show()

    #saving graph
    location = 'static/graphs/{}'.format(graph_name)
    plt.savefig(location, format='png')

    # close/refresh plot
    plt.close()

    # closing connections
    cur.close()
    con.close()
    curr.close()
    conn.close()
    

# create function to get all issuers
def GetIssuersMIC(mic):
    
    # connections
    db = "SummaryDB.db"
    con = lite.connect(db)
    cur = con.cursor()

    # get all issuers
    cur.execute('''SELECT DISTINCT ISSUER, PRETTY_NAME, GRAPH_NAME, QUANDL_CODE FROM Issuers WHERE MIC="{}" and DELETED=0 ORDER BY ISSUER;'''.format(mic))
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

