'''
This script calculates the total short interest per issuer.
'''

from functions import Logging, TimeStamp
import sqlite3 as lite
import pandas as pd
import time
import sys
import datetime

# measure duration of run
startTime = time.time()

# parameters
try:
    mic = sys.argv[1].upper()
    db_run = "{}.db".format(mic)
    
except:
    Logging("\nNo argument given, please provide MIC for adding corrections to database..\n")
    exit()

# function to calculate total short interest
def TotalShortInterest(issuer, mic, tbl_name="SourceData"):

    # connections
    db = "{}.db".format(mic)
    con = lite.connect(db)
    cur = con.cursor()

    # get all holders for issuer
    cur.execute('''SELECT DISTINCT UPPER(HOLDER) FROM "{}" WHERE ISSUER = "{}"
                ORDER BY HOLDER;'''.format(tbl_name, issuer))
    holders = [h[0].upper() for h in cur.fetchall()]
    #Logging(holders)

    ### create dataframe for issuer with all notifications

    # create empty dataframe
    today = TimeStamp('%m/%d/%Y')
    #Logging(today)
    df_issuer = pd.DataFrame(data=None, index=pd.date_range(start='09/30/2012', end=today, freq='D'))
    df_issuer.index = df_issuer.index.strftime('%Y%m%d') # change index to YYYYMMDD
    #Logging(df_issuer)

    # create columns for every holder
    for hld in holders:
        #Logging(holder)
        holder = hld.replace(' ', '_')
        # get notifications
        cur.execute('''SELECT INTEREST, POSITION_DATE FROM SourceData
                    WHERE ISSUER="{}" AND UPPER(HOLDER)="{}";'''.format(issuer, hld))
        notifications = cur.fetchall()
        #Logging(notifications)

        # add column for every holder + 0% for 30/09/2012
        df_issuer[holder] = ''
        df_issuer.loc['20120930', holder] = 0.00

        # fill in notifications
        for ntf in notifications: 
            df_issuer.loc[ntf[1], holder] = ntf[0]
        
        # complete columns with previous SI
        list_of_dates = df_issuer.index.tolist()
        ntf_dates = [n[1] for n in notifications]
        check_dates = ['2012-09-30']
        for date in list_of_dates:
            if df_issuer.at[date, holder] != '':
                check_dates.append(date)
            else:
                df_issuer.at[date, holder] = df_issuer.at[check_dates[-1], holder]

    # calculate total SI
    df_issuer['TOTAL'] = ''
    list_of_dates = df_issuer.index.tolist()
    for date in list_of_dates:
        total_sum = 0.0
        for clm in df_issuer.columns[:-1]:
            total_sum += df_issuer.at[date, clm]
        df_issuer.at[date, 'TOTAL'] = round(total_sum, 2)

    # dataframe to database
    si_db = "{}_ShortInterest.db".format(mic)
    conn = lite.connect(si_db)
    issuer_tbl = issuer.strip().lower().replace(' ', '_').replace('-', '_').replace('.','').replace("'","") #graphs.py
    df_issuer.index.names = ['DATE']
    df_issuer.to_sql(issuer_tbl, conn, if_exists='replace')
    
    #Logging(df_issuer.head())
    #Logging(df_issuer.tail(250))
    
    #closing connections
    con.commit()
    cur.close()
    con.close()
    conn.commit()
    conn.close()


# function to run all issuers
def RunAllIssuers(mic,tbl_name="SourceData"):
    
    # connections
    db = "{}.db".format(mic)
    con = lite.connect(db)
    cur = con.cursor()

    # get all issuers
    cur.execute('''SELECT DISTINCT ISSUER FROM "{}" ORDER BY ISSUER;'''.format(tbl_name))
    issuers = [i[0] for i in cur.fetchall()]
    #print(issuers)

    # run function
    errors = {}
    for issuer in issuers:
        try:
            Logging("Issuer: {}".format(issuer))
            TotalShortInterest(issuer, mic)
        except Exception as e:
            errors[issuer] = str(e)

    # print errors
    Logging("\nErrors:")
    for k,v in errors.items():
        Logging("{} : {}".format(k,v))

    Logging("\nDone.\n")
    #closing connections
    con.commit()
    cur.close()
    con.close()



# only run when this is main script
if __name__ == "__main__":
    
    Logging("Calculating total short interest for : {}\n".format(mic))
    
    # functions to run
    RunAllIssuers(mic)

    # print duration of run
    endTime = time.time()
    duration = endTime - startTime
    minutes = round(duration // 60)
    seconds = round(duration % 60)
    Logging("Script ran {} min and {} sec".format(minutes, seconds))

