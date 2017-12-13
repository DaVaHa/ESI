'''
This script scrapes all short interest notifications for Euronext Portugal.
'''

from functions import Logging
import bs4
import requests
import sys
import time
import re
import sqlite3 as lite
import datetime


startTime = time.time()

# variables
list_url = 'http://web3.cmvm.pt/english/sdi/emitentes/shortselling/index.cfm'
source_url = 'http://web3.cmvm.pt/english/sdi/emitentes/shortselling/'
mic = 'XLIS'

Logging("Scraping all issuers from CMVM index page...")
# get all issuers and URLs
html = requests.get(list_url).text
list_soup = bs4.BeautifulSoup(html, 'lxml')
list_tag = list_soup.find('section', class_='bloco10 first')

# check if parsing succeeded
if list_tag == None:
    Logging("ALERT: No tags found! Check script!")
    sys.exit()

# get link for every issuer
issuers_links = {}
for tag in list_tag.find_all('a'):
    title = tag.get('title')
    href = tag.get('href')
    issuers_links[title] = href
    #print(title, ':', href)


# function to delete previous data in RawSourceData
def TruncateTable(db, table_name):

    con = lite.connect(db)
    cur = con.cursor()
    cur.execute('DELETE FROM "{}";'.format(table_name))
    con.commit()
    cur.close()
    con.close()


# function to get historical notifications (used in later function)
def GetHistoricalNotifications(issuer, hist_link):

    hist_notifs = []
    
    # get html
    html = requests.get(source_url + hist_link).text
    soup = bs4.BeautifulSoup(html, 'lxml')
    tag = soup.find('section', class_='WTabela')
    
    # read table
    #headings = [t.text for t in tag.find_all('span', class_='alLeft')]
    tds = tag.find_all('td')
    for td in tds:
        if td.get('style') == 'text-align:left':
            active = [issuer.upper()]
            active.append(td.text.upper())
            siblings = td.find_next_siblings('td')[:3]
            for s in siblings:
                active.append(s.text.upper())
            #transform date
            dt = datetime.datetime.strptime(active[4], '%d/%m/%Y')
            active[4] = datetime.datetime.strftime(dt, '%Y%m%d')
            active = [active[i] for i in [0,1,2,4]]
            hist_notifs.append(active)
            #print(active)

    return hist_notifs


# function
def GetXLISNotifications(issuer, link):
    
    # get html
    html = requests.get(source_url + link).text
    soup = bs4.BeautifulSoup(html, 'lxml')
    tag = soup.find('section', class_='WTabela')
    
    # break function in case no notifications
    tag_bool = len(str(tag)) < 1000
    str_bool = 'no communications' in str(tag).lower()
    a_count = len(tag.find_all('a'))
    
    if tag_bool == True and str_bool == True and a_count == 0:
        return 
##    else:
##        print("{} :\n>>> Length < 1000 = {} // String? = {} // Count a-tags = {}".format(issuer.upper(), tag_bool, str_bool, a_count))
    
    # read first page notifications
    #headings = [t.text for t in tag.find_all('span', class_='alLeft')]
    notifications = []
    for t in tag.find_all('td'):
        if t.get('style') == 'text-align:left':
            active = [issuer.upper()]
            active.append(t.text.upper())
            #print(t.text)
            siblings = t.find_next_siblings('td')[:3]
            for s in siblings:
                active.append(s.text.upper())
                #print(l.text)
            #transform date
            dt = datetime.datetime.strptime(active[4], '%d/%m/%Y')
            active[4] = datetime.datetime.strftime(dt, '%Y%m%d')
            notifications.append([active[i] for i in [0,1,2,4]])

    if notifications == [] and a_count == 0:
        return
##    else:
##        for notification in notifications:
##            print(notification)

    # ISIN
    full_text = str(tag)
    i = full_text.find('ISIN')
    isin = re.findall('PT[a-zA-Z0-9]{10}', full_text[i:i+100])[0]
    #print(isin)
    
    # read links of historical notifications
    a_tags = tag.find_all('a')
    cnt = 0
    for a in a_tags:
        if 'COMMUNICATIONS' in a.text.upper():
            hist_link = a.get('href')
            cnt +=1
            #print(hist_link)

    # check if not overwritten
    if cnt > 1:
        Logging('ALERT! Link overwritten! Check script!')

    #hist_notifs
    hist_notifications = GetHistoricalNotifications(issuer, hist_link)
    full_notifications = hist_notifications + notifications
##    full_notifications.sort()
##    for ntf in full_notifications:
##        print(ntf)
    
    # add all to database
    all_notifications = ()
    for ntf in full_notifications:
        tpl = tuple(ntf) + (mic, isin)
        all_notifications += (tpl,)
    
    con = lite.connect('XLIS.db')
    with con:
        cur = con.cursor()
        cur.executemany("""INSERT INTO RawSourceData
                (ISSUER, HOLDER, INTEREST, POSITION_DATE, MIC, ISIN)
                VALUES (?,?,?,?,?,?);""", all_notifications)
        con.commit()
        
    if con:
        con.close()


# delete previous data
TruncateTable('XLIS.db', 'RawSourceData')

# loop over all issuers
Logging("Looping over all issuers...")
for issuer, link in issuers_links.items():
    try:
        GetXLISNotifications(issuer, link)
    except Exception as e:
        Logging("Error {}: {}".format(issuer,str(e)))



# finish up
Logging("Done.\nRunTime: {} seconds".format(round(time.time() - startTime)))








