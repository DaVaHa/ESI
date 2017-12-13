'''
This script defines functions to create the databases.
This databases will be used for extraction, cleaning, transformations, querying,...
'''

import sqlite3 as lite
from functions import Logging
import sys

# parameters
try:
    create_db_mic = sys.argv[1].upper()
    print(create_db_mic)
except:
    print("\nNo argument given, please provide MIC for creating database..\n")
    #exit()

# function
def CreateSourceDB(mic):
    
    Logging('Creating database for MIC: {}'.format(mic))

    con = None
    # create MIC.db
    mic_db = "{}.db".format(mic)
    try:
        con = lite.connect(mic_db)
        cur = con.cursor()

        # raw source data table
        cur.execute('''CREATE TABLE RawSourceData(
                    HOLDER TEXT NOT NULL,
                    ISSUER TEXT NOT NULL,
                    ISIN TEXT,
                    INTEREST REAL NOT NULL,
                    POSITION_DATE TEXT NOT NULL,
                    MIC TEXT
                   )''')


        # source data
        cur.execute('''CREATE TABLE SourceData (
                    HOLDER TEXT NOT NULL,
                    ISSUER TEXT NOT NULL,
                    ISIN TEXT,
                    INTEREST REAL NOT NULL,
                    POSITION_DATE TEXT NOT NULL,
                    MIC TEXT,
                    COMMENT TEXT,
                    UPDATE_DATE TEXT
                    )''')

        # corrections table
        cur.execute('''CREATE TABLE Corrections (
                    HOLDER TEXT NOT NULL,
                    ISSUER TEXT NOT NULL,
                    ISIN TEXT,
                    INTEREST REAL NOT NULL,
                    POSITION_DATE TEXT NOT NULL,
                    MIC TEXT,
                    COMMENT TEXT,
                    UPDATE_DATE TEXT
                    )''')

##        # one year gap table
##        cur.execute('''CREATE TABLE OneYearGap (
##                    HOLDER TEXT NOT NULL,
##                    ISSUER TEXT NOT NULL,
##                    ISIN TEXT,
##                    INTEREST REAL NOT NULL,
##                    POSITION_DATE TEXT NOT NULL,
##                    MIC TEXT,
##                    COMMENT TEXT,
##                    UPDATE_DATE TEXT
##                    )''')


##        # cleaned source data   
##        cur.execute('''CREATE TABLE Notifications (
##                       HOLDER TEXT NOT NULL,
##                       ISSUER TEXT NOT NULL,
##                       ISIN TEXT,
##                       INTEREST REAL NOT NULL,
##                       POSITION_DATE TEXT NOT NULL,
##                       MIC TEXT NOT NULL,
##                       COMMENT TEXT,
##                       UPDATE_DATE TEXT NOT NULL
##                       )''')

        Logging(mic_db)

        Logging("\nDone.\n")
        
    except Exception as e:
        Logging("Error: {}".format(e))

    finally:
        if cur:
            cur.close()
        if con:
            con.close()  


def CreateShortInterestDB(mic):
    
    Logging('Creating database for MIC: {}'.format(mic))

    con = None
    # create MIC.db
    mic_db = "{}_ShortInterest.db".format(mic)
    try:
        con = lite.connect(mic_db)
        cur = con.cursor()

##        # raw source data table
##        cur.execute('''CREATE TABLE RawSourceData(
##                    HOLDER TEXT NOT NULL,
##                    ISSUER TEXT NOT NULL,
##                    ISIN TEXT,
##                    INTEREST REAL NOT NULL,
##                    POSITION_DATE TEXT NOT NULL,
##                    MIC TEXT,
##                    COMMENT TEXT,
##                    UPDATE_DATE TEXT
##                   )''')


        Logging(mic_db)

        Logging("\nDone.\n")
        
    except Exception as e:
        Logging("Error: {}".format(e))

    finally:
        if cur:
            cur.close()
        if con:
            con.close()  


def CreateSummaryDB():
    
    Logging('Creating database: SummaryDB')

    con = None
    # create MIC.db
    db = 'SummaryDB.db'
    try:
        con = lite.connect(db)
        cur = con.cursor()

        # raw source data table
##        cur.execute('''CREATE TABLE Issuers (
##                    ISSUER TEXT NOT NULL,
##                    MIC TEXT,
##                    PRETTY_NAME TEXT,
##                    LATEST_INTEREST REAL NOT NULL,
##                    MAX_SI REAL,
##                    MAX_DATE TEXT,
##                    GRAPH_NAME TEXT,
##                    UPDATE_DATE TEXT,
##                    QUANDL_CODE TEXT
##                   )''')

        cur.execute('''CREATE TABLE Latest_Notifications (
                  HOLDER TEXT NOT NULL,
                  ISSUER TEXT NOT NULL,
                  ISIN TEXT,
                   INTEREST REAL NOT NULL,
                   POSITION_DATE TEXT NOT NULL,
                   MIC TEXT )
                  ;''')
    
        Logging(db)

        Logging("\nDone.\n")
        
    except Exception as e:
        Logging("Error: {}".format(e))

    finally:
        if cur:
            cur.close()
        if con:
            con.close()  


def CreateContactDB():
    
    Logging('Creating database: ContactDB')

    con = None
    # create MIC.db
    db = 'Contact.db'
    try:
        con = lite.connect(db)
        cur = con.cursor()

        cur.execute("""CREATE TABLE Comments (
                  PERSON TEXT,
                  EMAIL TEXT,
                  SUBJECT TEXT,
                  COMMENT TEXT
                  );""")
    
        Logging(db)
        Logging("\nDone.\n")
        
    except Exception as e:
        Logging("Error: {}".format(e))

    finally:
        if cur:
            cur.close()
        if con:
            con.close()  


# run
#CreateSourceDB(create_db_mic)
#CreateShortInterestDB(create_db_mic)
#CreateSummaryDB()
CreateContactDB()
