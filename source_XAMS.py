'''
This script will extract the notifications for Euronext Amsterdam.
'''
from functions import Logging, TimeStamp
import time
import pandas as pd
import sqlite3 as lite
import xlsxwriter
import re
import numpy as np
import datetime
import os

# measure duration of script
startTime = time.time()

# paramaters
mic = 'XAMS'
stamp = time.strftime("%Y%m%d")
xlsx_name = '{}_{}.xlsx'.format(mic,stamp)

# get source file
files = []
for f in os.listdir('/home/daniel/Desktop/Scripts/_ShortNotifications/'):
    file_name = 'XAMS_{}'.format(TimeStamp())
    if f.startswith(file_name):
        files.append(f)
file = files[0]



Logging("Loading Excel file from AFM website...\n")
# create ExcelFile object
xlsx = pd.ExcelFile(file)
sheets = xlsx.sheet_names
writer = pd.ExcelWriter(xlsx_name, engine='xlsxwriter')

# connect to database
db = "{}.db".format(mic)
conn = lite.connect(db)

Logging("Importing worksheets:")
# import all sheets
for sht in sheets:
    Logging("\n=> {}".format(sht))
    try:
        # import and save worksheet
        df = pd.read_excel(xlsx, sheetname=sht)
        df.to_excel(writer, sheet_name=sht)
        
        # renaming columns
        for clm in df.columns:
            if re.search('ISIN', clm.upper()):
                df.rename(columns={clm : 'ISIN'}, inplace=True)
            elif re.search('NETTO', clm.upper()) or re.search('CAPITAL', clm.upper()):
                df.rename(columns={clm : 'INTEREST'}, inplace=True)
            elif re.search('DATUM', clm.upper()):
                df.rename(columns={clm : 'POSITION_DATE'}, inplace=True)
            elif re.search('HOUDER', clm.upper()):
                df.rename(columns={clm : 'HOLDER'}, inplace=True)
            elif re.search('EMITTENT', clm.upper()):
                df.rename(columns={clm : 'ISSUER'}, inplace=True)

        # delete 6th column if available
        while len(df.columns) > 5:
            df.drop(df.columns[-1], axis=1, inplace=True)

        # checking if INTEREST columns contain percentages
        df['INTEREST'] = df['INTEREST'].apply(lambda x : float(str(x).replace(',','.')))
        if np.mean(df['INTEREST']) < 0.50:
            df['INTEREST'] = df['INTEREST'].apply(lambda x: x*100)

        # adding MIC column
        df["MIC"] = 'XAMS'

        # dropping all rows with any empty cells
        df.dropna(axis=0, how='any', inplace=True)

        # deleting duplicates
        df.drop_duplicates(inplace=True)

        # holder & issuer to uppercase
        df['ISSUER'] = df['ISSUER'].apply(lambda x : x.upper())
        df['HOLDER'] = df['HOLDER'].apply(lambda x : x.upper())
        
        # change date into yyyymmdd
        def TransformDate(string):
            date_stamp = datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
            date_str = datetime.datetime.strftime(date_stamp, "%Y%m%d")
            return date_str
        
        df['POSITION_DATE'] = df['POSITION_DATE'].apply(lambda x : TransformDate(x))
        
        # starting index from 1 instead of 0
        df.index += 1
        
        # creating table in database
        table_name = sht.replace('-', '_').replace(' ', '')
        df.to_sql(table_name, conn, if_exists="replace")

        # counting rows added
        count = pd.read_sql_query("select count(*) from {};".format(table_name), conn).iloc[0,0]
        Logging("Rows added: {}".format(count))
        
    except Exception as e:
        Logging("Something went wrong with importing sheet {}.".format(sht))
        Logging("ERROR: {}".format(e.args))


# create complete table
df_all = pd.DataFrame()
for sht in sheets:
    try:
        # pick up all table_names
        table_name = sht.replace('-', '_').replace(' ', '')
        df_temp = pd.read_sql_query("SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC FROM {};".format(table_name), conn)

        # append to df_all
        df_all = df_all.append(df_temp)
        #print(df_all.head())

    except Exception as e:
        Logging("Something went wrong with adding sheet {} to table ALL.".format(sht))
        Logging("ERROR: {}".format(e.args))


# add DataFrame to RawSourceData table in MIC.db:
try:
    # remove all duplicates
    df_all.drop_duplicates(inplace=True)

    # starting index from 1 instead of 0
    df_all.index += 1

    # rounding short interest
    df_all['INTEREST'] = df_all['INTEREST'].apply(lambda x: round(x,2))
    
    # add to database
    df_all.to_sql("RawSourceData", conn, if_exists="replace", index=False)

    # counting rows added
    count = pd.read_sql_query("select count(*) from 'RawSourceData';", conn).iloc[0,0]
    Logging("\n=> ALL\nRows added: {}".format(count))

except Exception as e:
    Logging("Something went wrong with exporting table RawSourceData to database.")
    Logging("ERROR: {}".format(e.args))


# closing connections
writer.save()
try:
    os.rename(xlsx_name, 'Files/{}'.format(xlsx_name)) #move file
except: #if exists: OSError
    os.remove('Files/{}'.format(xlsx_name)) #delete file
    os.rename(xlsx_name, 'Files/{}'.format(xlsx_name)) #move file
    print("File overwritten..")

conn.commit()
conn.close()


# move source file to Files
oldFileName = '/home/daniel/Desktop/Scripts/_ShortNotifications/{}'.format(file)
newFileName = '/home/daniel/Desktop/Scripts/_ShortNotifications/Files/{}'.format(file)
os.rename(oldFileName, newFileName)


Logging("\nDone.\n")
# print duration of script
endTime = time.time()
Logging("Script ran for {} seconds.".format(round(endTime - startTime,2)))
