'''
This script processes corporate actions for Euronext.
'''

import sqlite3 as lite
import pandas as pd
from datetime import datetime, timedelta
import time
from functions import Logging

### Corporate Actions ###
corp_acts = [
    ('BRNL','2014-06-04',2),
    ('NYR','2016-06-07', 0.1),
    ('CGG','2016-07-20',1/32),
    ('BCP','2016-10-24',1/75),
    ('FR', '2016-06-06', 3)
]


startTime = time.time()
Logging("\nProcessing all Corporate Actions for Euronext...\n")
# connection
con = lite.connect('Euronext.db')

# get data
df = pd.read_sql_query('SELECT * FROM Prices', con)
issuers = [i for i in df.columns if 'DATE' not in i.upper()]
df = df.set_index(pd.DatetimeIndex(df['Date']))


# function to process the corporate action
def ProcessCorpAct(corp_act):

    # unpack list
    issuer = corp_act[0]
    date_str = corp_act[1]
    split = corp_act[2]

    Logging("Processing CA: {} - {} - {}".format(issuer, date_str, split))
    # date before corp act
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    dt = dt - timedelta(days=1)
    day_before = datetime.strftime(dt, '%Y-%m-%d')

    # select column
    iss = df[['Date', issuer]]
    iss = iss.set_index(pd.DatetimeIndex(iss['Date']))
    iss.drop('Date', axis=1, inplace=True)

    # process corporate action
    iss['CA'] = iss[issuer].apply(lambda x : x/float(split))

    # create new column with corrected data
    first = iss.loc[:day_before,'CA'].to_frame()
    first.columns = [issuer]
##    print(first.tail())
    second = iss.loc[date_str:,issuer].to_frame()
##    print(second.head())
    
    iss_new = first.append(second)

##    print(df[issuer].head())
##    print(df[issuer].tail())

    df[issuer] = iss_new[issuer]

##    print(df[issuer].head())
##    print(df[issuer].tail())



# loop over all corporate actions
for ca in corp_acts:
    try:
        ProcessCorpAct(ca)
    except Exception as e:
        Logging("Error: {}".format(str(e)))
    
# send dataframe back to database
df.to_sql('Prices_incl_CA', con, if_exists='replace', index=False)


if con:
    con.commit()
    con.close()


Logging("\nDone.")
Logging("Script runtime: {}s".format(round(time.time()-startTime)))





