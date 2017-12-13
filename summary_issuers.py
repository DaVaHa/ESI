'''
This script will update the SummaryDB.
'''

import sqlite3 as lite
from functions import Logging, TimeStamp
import sys
import time

# measure duration of run
startTime = time.time()

# parameters
try:
    mic = sys.argv[1].upper()
except:
    print("\nNo argument given, please provide MIC for updating & deduplicating database..\n")
    exit()



# function to update SummaryDB for given MIC
def UpdateSummaryIssuers(mic):

    db = "{}.db".format(mic)
    con = lite.connect(db)
    cur = con.cursor()
    
    si_db = "{}_ShortInterest.db".format(mic)
    conn = lite.connect(si_db)
    curr = conn.cursor()

    sum_db = "SummaryDB.db"
    sum_con = lite.connect(sum_db)
    sum_cur = sum_con.cursor()

    Logging("\nUpdating Issuers in SummaryDB for MIC: {}".format(mic))

    # delete all data for mic
    #sum_cur.execute('''delete from Issuers where MIC="{}";'''.format(mic))
    #sum_con.commit()

    # get all current issuers
    sum_cur.execute('select ISSUER, MIC from Issuers')
    comp_issuers = [(i[0],i[1]) for i in sum_cur.fetchall()]
    
    # get issuers
    cur.execute('''select distinct ISSUER from SourceData ORDER BY ISSUER;''')
    issuers = [i[0] for i in cur.fetchall()]
    #print(issuers)
    
    for issuer in issuers:

        tbl_name = issuer.strip().lower().replace(' ', '_').replace('-', '_').replace('.','').replace("'","")  #graphs.py // total_short_interest.py

        # max & latest si
        curr.execute('''select TOTAL from "{}" order by DATE desc limit 1;'''.format(tbl_name))
        latest_si = curr.fetchall()[0][0]
        curr.execute('''select DISTINCT TOTAL, DATE from "{0}" WHERE TOTAL = (SELECT MAX(TOTAL) from "{0}");'''.format(tbl_name))
        max_si = curr.fetchall()[0][0]

        # changes in si
        stamp_1m = TimeStamp(back_months=1)
        curr.execute('''select TOTAL from "{0}" where DATE = "{1}";'''.format(tbl_name, stamp_1m))
        change_1m = curr.fetchall()[0][0]

        stamp_3m = TimeStamp(back_months=3)
        curr.execute('''select TOTAL from "{0}" where DATE = "{1}";'''.format(tbl_name, stamp_3m))
        change_3m = curr.fetchall()[0][0]
        
        stamp_6m = TimeStamp(back_months=6)
        curr.execute('''select TOTAL from "{0}" where DATE = "{1}";'''.format(tbl_name, stamp_6m))
        change_6m = curr.fetchall()[0][0]

        #print(change_1m,change_3m,change_6m)
        
        if max_si < latest_si:
            Logging('\n   ########    \nDID YOU SEE THAT?! LATEST SI > MAX_SI for : {}\n   ########    \n'.format(issuer))
            
        if (issuer, mic) not in comp_issuers:
            Logging('{} not yet in db'.format(issuer))
            graph_name = '{}_{}.png'.format(tbl_name, mic.upper())       
            pretty_name = issuer.capitalize()
                         
            sum_cur.execute('''INSERT INTO Issuers (ISSUER, MIC, PRETTY_NAME, LATEST_INTEREST, MAX_SI, GRAPH_NAME, UPDATE_DATE, CHANGE_1M, CHANGE_3M, CHANGE_6M, DELETED)
                           VALUES ("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}",0);'''.format(
                               issuer, mic, pretty_name, latest_si, max_si, graph_name, TimeStamp(), change_1m, change_3m, change_6m))
            sum_con.commit()
        else:
            #print('{} already in SummaryDB'.format(issuer))
            sum_cur.execute('''UPDATE Issuers SET LATEST_INTEREST="{}", MAX_SI="{}", UPDATE_DATE="{}", CHANGE_1M="{}", CHANGE_3M="{}", CHANGE_6M="{}"
                                WHERE ISSUER = "{}";'''.format(
                                latest_si, max_si, TimeStamp(),change_1m, change_3m, change_6m, issuer))
            sum_con.commit()


    # close connections
    sum_con.commit()
    sum_cur.close()
    sum_con.close()
    con.commit()
    cur.close()
    con.close()
    conn.commit()
    curr.close()
    conn.close()
    
    print("Done.")

    

# only run when this is main script
if __name__ == "__main__":

    # functions to run
    UpdateSummaryIssuers(mic)

    # print duration of run
    endTime = time.time()
    Logging("Script ran for {} seconds.".format(round(endTime-startTime,2)))


