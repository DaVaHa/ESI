'''
This script will add corrections  to the short interest notifications.
=> If more than 365 days no short interest notifications: next day 0%
=> If short interest < 0.5%: next day 0%
'''

from functions import Logging, TimeStamp
from update_deduplicate import Deduplicate
import sqlite3 as lite
import pandas as pd
import datetime
import time
from time import strftime
import sys

# measure duration of run
startTime = time.time()

# parameters
try:
    mic = sys.argv[1].upper()
    db_run = "{}.db".format(mic)
    tbl_name_run = 'SourceData'
    
except:
    Logging("\nNo argument given, please provide MIC for adding corrections to database..\n")
    exit()


# function to add 0% notifications when short interest is lower than 0.5%
def HalfPercent(db):
    
    Logging("This will add 0% corrections when SI < 0.5%\n")
    
    # connections
    con = lite.connect(db)
    cur = con.cursor()

    # get all notifications below 0.5%
    cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC
                FROM SourceData WHERE INTEREST < 0.5 AND INTEREST > 0.0;''') # to avoid already added 0% notifications
    records = cur.fetchall()

    # corrections that already exist in Corrections table
    cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC
                     FROM Corrections;''')
    rows_comp = tuple(cur.fetchall())
    
    # replace interest with 0% and date with date+1
    cnt=0
    stamp = TimeStamp("%Y%m%d_%Hh%M")
    for rec in records:
        
        # change date and interest
        dt = datetime.datetime.strptime(rec[4], '%Y%m%d') + datetime.timedelta(days=1)
        tmrw = datetime.datetime.strftime(dt, '%Y%m%d')
        cor = rec[:3] + (0.0, tmrw) + (rec[5],) + ('HALF_PERCENT', stamp)

        # check if correction already exists
        if cor[:6] in rows_comp:  # without COMMENT & UPDATE_DATE
            continue
            
        # check if record exists for new date
        cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC, COMMENT
                    FROM SourceData
                    WHERE HOLDER = "{}" AND ISSUER = "{}" AND ISIN = '{}'
                    AND POSITION_DATE = '{}';'''.format(cor[0], cor[1], cor[2], tmrw))
        data = cur.fetchall()
        if data:
            #Logging("\nA notification exists for the next date:")
            #Logging(rec)
            #Logging("{}\n".format(data))
            continue

        # add correction
        Logging(rec)
        Logging(cor)
        cur.execute('''INSERT INTO Corrections (HOLDER, ISSUER, ISIN, INTEREST,
                    POSITION_DATE, MIC, COMMENT, UPDATE_DATE) VALUES {};'''.format(cor))
        cnt+=1

    Logging("\nRows added: {}".format(cnt))
    Logging("\nDone.\n")

    #closing connections
    con.commit()
    cur.close()
    con.close()



# function to add 0% notifications when no notification for one year
def OneYearGap(db):

    Logging("\nThis will add 0% corrections in case of one year gap\n")
    
    # connections
    con = lite.connect(db)
    cur = con.cursor()

    # get all issuers
    cur.execute("SELECT DISTINCT ISSUER FROM SourceData;")
    issuers = [i[0] for i in cur.fetchall()]
    #print(issuers)
    #print(len(issuers))

    # corrections that already exist in Corrections table
    cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC
                 FROM Corrections;''')
    gaps_comp = tuple(cur.fetchall())

    # determine 1Y gaps for every holder per issuer
    cnt=0
    stamp = TimeStamp("%Y%m%d_%Hh%M")
    for issuer in issuers:

        # get all holders for issuer
        cur.execute('''SELECT DISTINCT HOLDER FROM SourceData
                    WHERE ISSUER="{}";'''.format(issuer))
        holders = [h[0] for h in cur.fetchall()]
        #print(holders)
        #print(len(holders))

        gaps_lines = ()
        for holder in holders:
            cur.execute('''SELECT INTEREST, POSITION_DATE FROM SourceData
                        WHERE HOLDER="{}" AND ISSUER="{}"
                        ORDER BY POSITION_DATE;'''.format(holder, issuer))
            notifications = cur.fetchall()
            #print(notifications[:3])

            # append list with today (only for comparison, so not to be added to db!)
            today = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
            notifications.append((0.00, today))

            # check for notifications with one year gap (incl today)
            for i in range(1,len(notifications)):
                date = datetime.datetime.strptime(notifications[i][1], '%Y%m%d')
                date_prev = datetime.datetime.strptime(notifications[i-1][1], '%Y%m%d')
                if date > date_prev + datetime.timedelta(days=365) and notifications[i-1][0] != 0.0:
                    #Logging(date_prev)
                    # notification before gap
                    cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC
                                 FROM SourceData WHERE ISSUER="{}" AND HOLDER="{}" AND POSITION_DATE="{}";
                                 '''.format(issuer, holder, notifications[i-1][1]))
                    line = cur.fetchall()
                    if len(line) > 1:
                        Logging("ALERT! Something wrong with 'unique' query.")
                    line = line[0]

                    # create correction record
                    dt = datetime.datetime.strptime(line[4], '%Y%m%d') + datetime.timedelta(days=1)
                    tmrw = datetime.datetime.strftime(dt, '%Y%m%d')          
                    line = line[0:3] + (0.0,tmrw) + (line[5],) + ('ONE_YEAR_GAP', stamp)
                    
                    # check if record exists for new date
                    cur.execute('''SELECT * FROM SourceData WHERE HOLDER = "{}" AND ISSUER = "{}"
                               AND ISIN = '{}' AND POSITION_DATE = '{}';'''.format(holder, issuer, line[2], tmrw))
                    data = cur.fetchall()
                    if data:
                        Logging("\nA notification exists for the next date:")
                        Logging(line)
                        Logging("{}\n".format(data))
                        continue
                    
                    #Logging(line)
                    gaps_lines += (line,)
                    
        
        for gap in gaps_lines:
            # skip if already exists in Corrections
            if gap[:6] in gaps_comp: # without COMMENT & UPDATE_DATE
                #Logging("Already in Corrections:\n{}".format(gap[:6])) 
                continue

            # add correction
            Logging("\nEquity {} - Holder: {}".format(issuer, holder))
            Logging(gap)
            cur.execute('''INSERT INTO Corrections (HOLDER, ISSUER, ISIN, INTEREST,
                    POSITION_DATE, MIC, COMMENT, UPDATE_DATE) VALUES {};'''.format(gap))
            cnt+=1
            
    #print(gaps_lines)

    Logging("\nRows added: {}".format(cnt))
    Logging("\nDone.\n")

    #closing connections
    con.commit()
    cur.close()
    con.close()



# add corrections to .
def AddCorrections(db, tbl_name):

    Logging("\nThis will add all the corrections to : {}\n".format(tbl_name))
    
    # connections
    con = lite.connect(db)
    cur = con.cursor()

    # get all corrections already available in target table
    cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC
                FROM {} WHERE COMMENT IS NOT NULL;'''.format(tbl_name))
    cor_comps = tuple(cur.fetchall())
    #print(cor_comps)

    # get all corrections
    cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC,
                COMMENT, UPDATE_DATE FROM Corrections;''')
    corrections = tuple(cur.fetchall())
    
    # add if doesn't exist yet
    cnt=0
    for cor in corrections:
        if cor[:6] in cor_comps:
            #Logging("Already in table {}:\n{}".format(tbl_name, cor))
            continue
    
        Logging(cor)
        cur.execute('''INSERT INTO {} (HOLDER, ISSUER, ISIN, INTEREST,
                    POSITION_DATE, MIC, COMMENT, UPDATE_DATE) VALUES {};
                    '''.format(tbl_name,cor))
        cnt+=1
  
    Logging("\nRows added: {}".format(cnt))
    Logging("\nDone.\n")

    #closing connections
    con.commit()
    cur.close()
    con.close()



# only run when this is main script
if __name__ == "__main__":
    
    Logging("Adding corrections to: {}\n".format(db_run))
    
    # functions to run
    HalfPercent(db_run)  # find notifications < 0.5% + add to Corrections
    OneYearGap(db_run)   # find notifications with one year gap + add to Corrections
    Deduplicate(db_run, "Corrections")  #deduplicate Corrections
    
    AddCorrections(db_run, tbl_name_run) # add corrections to SourceData
    Deduplicate(db_run, "SourceData")  #deduplicate SourceData

    # print duration of run
    endTime = time.time()
    Logging("Script ran for {} seconds.".format(round(endTime-startTime,2)))

