'''
This script will update the latest notification by issuer in SummaryDB.
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
def UpdateSummaryNotifications(mic):

    #connections
    db = "{}.db".format(mic)
    con = lite.connect(db)
    cur = con.cursor()
    
    sum_db = "SummaryDB.db"
    sum_con = lite.connect(sum_db)
    sum_cur = sum_con.cursor()


    Logging("\nUpdating Notifications in SummaryDB for MIC: {}".format(mic))

    # delete all previous notifications
    sum_cur.execute('DELETE FROM Latest_Notifications WHERE MIC="{}";'.format(mic))
    sum_con.commit()
    
    # get issuers
    sum_cur.execute('''select distinct ISSUER from Issuers WHERE MIC="{}" ORDER BY ISSUER;'''.format(mic))
    issuers = [i[0] for i in cur.fetchall()]

    # take all latest notifications
    cur.execute('''SELECT HOLDER, ISSUER, MAX(POSITION_DATE)
               from SourceData group by HOLDER, ISSUER;''')
    max_notifs = [(d[0],d[1],d[2]) for d in cur.fetchall()]

    # take all notifications
    cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC from SourceData;''')
    notifs = [(n[0],n[1],n[2],n[3],n[4],n[5]) for n in cur.fetchall()]

    # loop over notifications and add to list if SI > 0
    latest_notifs = []
    for n in notifs:
        check = (n[0],n[1],n[4])
        if check in max_notifs and n[3] > 0:
            latest_notifs.append(n)
    
    # add to database
    notifications = tuple(latest_notifs)
    sum_cur.executemany("INSERT INTO Latest_Notifications VALUES (?,?,?,?,?,?)", notifications)
    

    # close connections
    sum_con.commit()
    sum_cur.close()
    sum_con.close()
    con.commit()
    cur.close()
    con.close()
    
    print("Done.")



# only run when this is main script
if __name__ == "__main__":

    # functions to run
    UpdateSummaryNotifications(mic)

    # print duration of run
    endTime = time.time()
    Logging("Script ran for {} seconds.".format(round(endTime-startTime,2)))
