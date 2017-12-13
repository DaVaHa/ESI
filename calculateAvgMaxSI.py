'''
This script will calculate averages and max SI by quarter.
'''
import sqlite3 as lite
import pandas as pd
import numpy as np

# path to databases
path = '/home/daniel/Desktop/Scripts/_ShortNotifications//'

# define calculation function
def CalculateAVGMAX(mic):
    global totals_avg
    global totals_mx

    print(mic)
    # connection
    con = lite.connect(path+ mic +'_ShortInterest.db')
    cur = con.cursor()

    # get all tables
    cur.execute('select name from sqlite_master where type = "table";')
    tmp = cur.fetchall()
    tables = [t[0] for t in tmp]
    nr_of_tbls = len(tables)
    print(nr_of_tbls)
    #print("{}: {}".format(mic,nr_of_tbls))
    #print(tables[:5])

    # loop over tables
    list_of_avg = []
    list_of_avg_excl_0 = []
    list_of_max = []
    for tbl in tables:
        #print(tbl)
        # get quarterly data
        try:
            cur.execute('select DATE, TOTAL from "{}";'.format(tbl))
        except:
            cur.execute('select "index", TOTAL from "{}";'.format(tbl))
        df = pd.DataFrame(data=cur.fetchall(), columns=['DATE', tbl])
        df.set_index(pd.DatetimeIndex(df['DATE']), inplace=True)
        df = df[df.index.dayofweek < 5]
        
        avg = df.resample('Q').mean()
        avg_exl_0 = df.replace(0.00, np.nan).resample('Q').mean()
        mx = df.resample('Q').max()
        
        list_of_avg.append(avg)
        list_of_avg_excl_0.append(avg_exl_0)
        list_of_max.append(mx[[tbl]])

    totals_avg = pd.concat(list_of_avg, axis=1)
    totals_avg['AVG'] = totals_avg.apply(lambda x: np.sum(x), axis=1)  ## SUM!!
    #print(totals_avg['AVG'])

    totals_avg_exl_0 = pd.concat(list_of_avg_excl_0, axis=1)
    totals_avg_exl_0['AVG'] = totals_avg_exl_0.apply(lambda x: np.sum(x), axis=1)  ## SUM!!
    #print(totals_avg_exl_0['AVG'])
    
    totals_mx = pd.DataFrame(index=totals_avg.index)
    totals_mx = pd.concat(list_of_max, axis=1, ignore_index=True)
    totals_mx['MX'] = totals_mx.apply(lambda x: np.max(x), axis=1)
    #print(totals_mx['MX'])
    
    return mic, nr_of_tbls, totals_avg['AVG'],totals_avg_exl_0['AVG'], totals_mx['MX']



# combine all markets
mics = ['XAMS', 'XBRU', 'XPAR', 'XLIS']
divisor = 0
list_avg = []
list_avg_excl = []
list_mx = []

for m in mics:
    mic, nr, avg, avg_excl, mx = CalculateAVGMAX(m)
    divisor += nr
    list_avg.append(avg)
    list_avg_excl.append(avg_excl)
    list_mx.append(mx)



# print results

print("\n#####################\n")
print(divisor)
print("\n#####################\n")

print("AVERAGES")
avg_tot = pd.concat(list_avg, axis=1)
avg_tot['AVG_ALL'] = avg_tot.apply(lambda x: np.sum(x), axis=1)
avg_tot['AVG_TOT'] = avg_tot.apply(lambda x: np.sum(x)/divisor, axis=1)
print(avg_tot['AVG_TOT'])

print("\n#####################\n")

print("AVERAGES (Excl Zeroes)")
avg_exl_tot = pd.concat(list_avg_excl, axis=1)
avg_exl_tot['AVG_ALL_EXCL'] = avg_exl_tot.apply(lambda x: np.sum(x), axis=1)
avg_exl_tot['AVG_TOT_EXCL'] = avg_exl_tot.apply(lambda x: np.sum(x)/divisor, axis=1)
print(avg_exl_tot['AVG_TOT_EXCL'])

print("\n#####################\n")

print("MAXIMA")
mx_tot = pd.concat(list_mx, axis=1)
mx_tot['MX_TOT'] = mx_tot.apply(lambda x: np.max(x), axis=1)
print(mx_tot['MX_TOT'])

print("\n#####################\n")


# export results
writer = pd.ExcelWriter('CalculationsAvgMax.xlsx')
avg_tot.to_excel(writer, 'Averages')
avg_exl_tot.to_excel(writer, 'AveragesExclZeroes')
mx_tot.to_excel(writer, 'Maxima')
writer.save()




