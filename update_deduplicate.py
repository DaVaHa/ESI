'''
This script updates the SourceData table and deduplicates the records.
'''

import sqlite3 as lite
import pandas as pd
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

db_run = "{}.db".format(mic)
tbl_name_run = 'SourceData'


# function to extract the raw source data into MIC.db
def AddUpdates(mic,back_months=60): 

    db = "{}.db".format(mic)
    
    Logging("\nImporting updates of last {} months into SourceData : {}.db.".format(back_months,mic))
    # get RawSourceData
    con = lite.connect(db)
    cur = con.cursor()
    stamp_months = TimeStamp("%Y%m%d",back_months=back_months)
    #print(stamp_months)
    cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC
                FROM RawSourceData
                WHERE POSITION_DATE > "{}";'''.format(stamp_months)) #get all raw source data of last 3 months
    data = tuple(cur.fetchall())

    Logging("Checking updates from {} and adding to {}.db...\n".format(stamp_months,mic))

    # import into MIC.db
    cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC
                FROM SourceData;''') #get all available data
    rows_comp = tuple(cur.fetchall())

    # add changes to MIC.db
    cnt=0
    for row in data:
        if row not in rows_comp:  #only add updates
            row += (TimeStamp("%Y%m%d_%Hh%M"),)
            cur.execute('''INSERT INTO SourceData(
                        HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC, UPDATE_DATE)
                        VALUES {};'''.format(row))
            Logging(row)
            cnt+=1
    
    Logging("\nRows added: {}".format(cnt))
    Logging("\nDone.\n")

    # close connections
    con.commit()
    cur.close()
    con.close()


# run function
#AddUpdates(mic)


# function to remove the double
def Deduplicate(db, tbl_name):

    Logging("Deduplicating {} from {} ...".format(tbl_name, db))
    # get all duplicates
    con = lite.connect(db)
    cur = con.cursor()
    q = """SELECT holder, issuer, interest, position_date FROM '{}'
        GROUP BY holder, issuer, position_date
        HAVING count(*) > 1
        ORDER BY issuer, holder, position_date
        ;""".format(tbl_name)
    cur.execute(q)
    duplicates = cur.fetchall()
    Logging("\nDuplicates:\n{}".format(duplicates))

    # count before deleting
    cur.execute('SELECT COUNT(*) FROM "{}";'.format(tbl_name))
    cnt_before = cur.fetchall()[0][0]
    Logging("\nRows before: {}\n".format(cnt_before))

    # to add one line again for rows deleted too much (with same SI)
    records_same_si = []
    
    # delete highest SI
    def DeleteDouble(record):
        
        # assuming record contains: holder, issuer, interest & position_date
        holder = record[0]
        issuer = record[1]
        date = record[3]
        Logging("holder: {} // issuer: {} // date: {}".format(holder, issuer, date))
        
        cur.execute('''SELECT HOLDER, ISSUER, INTEREST, POSITION_DATE, ISIN, MIC, COMMENT, UPDATE_DATE
                    FROM "{}"
                    WHERE HOLDER = "{}"
                    AND ISSUER = "{}"
                    AND POSITION_DATE = "{}";'''.format(
                        tbl_name,holder, issuer, date))
        doubles = cur.fetchall()

        # error if not 2 records
        if len(doubles) != 2:
            Logging("\nERROR: Record is not a double!!\nLength: {}\n{}".format(len(doubles),record))
    
        # keeping record with highest short interest
        si_1 = doubles[0][2]
        si_2 = doubles[1][2]
        Logging("Short interest: {} vs {}".format(si_1, si_2))
        if si_1 == 0:
            delete_si = si_1
        elif si_2 == 0:
            delete_si = si_2
        elif si_1 > si_2:
            delete_si = si_1
        elif si_1 == si_2:
            Logging("Alert: Same SI!! Only delete 1..")
            delete_si = si_1
            records_same_si.append(doubles[0])
            #print(doubles[0])
            #causing_error_on_purpose ##
        else:
            delete_si = si_2
            
        Logging("Deleting SI: {}".format(delete_si))
        # deleting highest short interest
        cur.execute('''DELETE FROM '{}'
                WHERE HOLDER="{}" AND ISSUER="{}"
                AND INTEREST="{}" AND POSITION_DATE="{}";'''.format(
                tbl_name,holder,issuer,delete_si,date))
        con.commit()

    # looping over duplicates
    for dupl in duplicates:
        try:
            DeleteDouble(dupl)
        except Exception as e:
            Logging("Something went wrong with deduplicating record:\n{}".format(dupl))
            Logging("ERROR: {}".format(e.args))

    # adding records with same SI again
    if records_same_si:
        print("\nAdding one row for records with same SI again:")
        print(records_same_si)
        for rec in records_same_si:
            cur.execute('''INSERT INTO "{}" (HOLDER, ISSUER, INTEREST, POSITION_DATE, ISIN, MIC, COMMENT, UPDATE_DATE)
                           VALUES ("{}","{}","{}","{}","{}","{}","{}","{}");'''.format(
                               tbl_name,rec[0],rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7]) )
        
    # count after deleting
    cur.execute('SELECT COUNT(*) FROM "{}";'.format(tbl_name))
    cnt_after = cur.fetchall()[0][0]
    Logging("\nRows before: {}".format(cnt_before))
    Logging("\nRows deleted: {}".format(cnt_before - cnt_after))
    Logging("\nRows after: {}".format(cnt_after))

    # closing
    Logging("\nDone.\n")
    con.commit()
    cur.close()
    con.close()


# run function
#Deduplicate(db_run, tbl_name_run)



# only run when this is main script
if __name__ == "__main__":

    # functions to run
    AddUpdates(mic)
    Deduplicate(db_run, tbl_name_run)
    

    # print duration of run
    endTime = time.time()
    Logging("Script ran for {} seconds.".format(round(endTime-startTime,2)))
























    
    
