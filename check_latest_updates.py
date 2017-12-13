'''
This script will check the latest short notification per source.
'''

import sqlite3 as lite


# all sources
sources = ['XBRU', 'XAMS', 'XPAR', 'XLIS']


# loop over sources
for src in sources:
    
    # connect to db
    db = "{}.db".format(src)
    con = lite.connect(db)
    cur = con.cursor()

    # find latest notification & update_date
    cur.execute('SELECT MAX(POSITION_DATE) FROM SourceData;')
    data = cur.fetchone()[0]

    cur.execute('SELECT MAX(UPDATE_DATE) FROM SourceData;')
    update = cur.fetchone()[0]

    # print last position_date & update_date
    print("\nLast position date for {} : {}".format(src, data))
    print("Last update for {} : {}".format(src, update))
    
    # close connections
    cur.close()
    con.close()
    
