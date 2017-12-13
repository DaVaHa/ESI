'''
Adds PDF XPAR notifications to XPAR.db
'''
import sqlite3 as lite
from functions import Logging, TimeStamp
import time

# measure duration of run
startTime = time.time()

Logging("\nImporting updates of XPAR_PDF.db into XPAR.db\n")

    
# connections
con = lite.connect('XPAR_PDF.db')
cur_from = con.cursor()

conn = lite.connect('XPAR.db')
cur_to = conn.cursor()


# get comparison from XPAR.db
cur_to.execute('''SELECT HOLDER, ISSUER, INTEREST, POSITION_DATE
                FROM SourceData;''') #get all available data
rows_comp = tuple(cur_to.fetchall())

# get most recent notifications
stamp_months = TimeStamp("%Y%m%d",back_months=60)
#print(stamp_months)
cur_from.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC, UPDATE_DATE
                FROM SourceData
                WHERE POSITION_DATE > "{}";'''.format(stamp_months)) #get all raw source data of last 3 months
data = tuple(cur_from.fetchall())


# add changes to MIC.db
for r in data:
    row = (r[0],r[1],r[3],r[4])
    r += ('XPAR_PDF',)
    if row not in rows_comp:  #only add updates
        cur_to.execute('''INSERT INTO SourceData (
                    HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC, UPDATE_DATE, COMMENT)
                    VALUES {};'''.format(r))
        Logging(row)

conn.commit()



Logging("\nDone.\n")
# closing connections
con.commit()
conn.commit()
cur_from.close()
cur_to.close()
con.close()
conn.close()

# print duration of run
endTime = time.time()
Logging("Script ran for {} seconds.".format(round(endTime-startTime,2)))
