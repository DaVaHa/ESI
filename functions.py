'''
Script to define functions: Logging, TimeString & SearchWord
'''

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import __main__
import numpy as np
import sys
import re
import sqlite3 as lite

# query a database
def QueryDB(query, db):

    con = lite.connect(db)
    cur = con.cursor()

    cur.execute(query)
    data = cur.fetchall()

    cur.close()
    con.close()

    return data


# inserting data into database
def InsertDB(query, db):

    con = lite.connect(db)
    cur = con.cursor()

    cur.execute(query)
    con.commit()

    cur.close()
    con.close()

    

# function to create timestamp-string: default yyyymmdd
def TimeStamp(fmt_str="%Y%m%d", precision=1, back_months=0, back_days=0):
    date = datetime.today()
    approx = int(np.floor(date.minute/precision)) * precision  #round down to precision minutes
    date = date.replace(minute=approx) #replace minutes
    date = date - relativedelta(months=back_months) - timedelta(days=back_days)
    stamp = datetime.strftime(date, fmt_str) #creating string in given format
    return stamp



# function to create logging file    
def Logging(text, path=None):

    # setting parameters
    if path is None:
        #path = "C:\\Users\Administrator\Desktop\Scripts\_ShortNotifications\Logs\\" #Windows
        path = "/home/daniel/Desktop/Scripts/_ShortNotifications/Logs/"  #Ubuntu

    file = os.path.basename(__main__.__file__)
    stamp = TimeStamp("%Y%m%d_%Hh%M", 5)

    # creating/appending to log file
    logs = open("{}{}_{}.txt".format(path,file,stamp), 'a')

    # writing/printing text
    logs.write('{}\n'.format(text))
    print(text)

    # closing file
    logs.close()


    
# function to look for words in python scripts and text files, NOT case sensitive
def LookForWord(word='Daniel', path=None):
    
    # setting parameters
    if path is None:
        #path = "C:\\Users\Administrator\Desktop\Scripts\_ShortNotifications\\" #Windows
        path = "/home/daniel/Desktop/Scripts/_ShortNotifications/"  #Ubuntu
    word = word.upper() # case unsensitive

    # print little heading
    text = "\n Wanted: {} ".format(word)
    print(text)
    print("{}\n".format("-"*len(text)))

    # find all text files and python scripts
    scripts = []
    for f in os.listdir(path):
        if f.endswith('.py') or f.endswith('.txt'):
            scripts.append(f)
    
    # find lines & files where word is found
    word_dict = {}
    for script in scripts:
        lines = []
        f = open("{}{}".format(path, script), 'r')
        for i,line in enumerate(f.readlines()):
            if re.match(r'.*{}.*'.format(word), line.upper()):
                lines.append("Ln {}: {}".format(i+1,line.strip('\n')))
        if lines: #not empty
            word_dict[script] = lines            
    
    # print all results
    for key, values in word_dict.items():
        print("Script: {}\n".format(key))
        for i in range(len(values)):
            print(values[i])
        print("\n")



# only run when this is main script
if __name__ == "__main__":
    
    #LookForWord('xbru')
    #print(TimeStamp("%Y%m%d",back_months=3))
    pass




