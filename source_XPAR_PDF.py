'''
Downloads the PDFs for XPAR.
'''

import requests
import urllib.request
from bs4 import BeautifulSoup
import PyPDF2
import re
import os
import time
from functions import TimeStamp, Logging
import sqlite3 as lite
import datetime

startTime = time.time()

# function to create database
def CreateDBForParis():

    con = lite.connect('XPAR_PDF.db')
    cur = con.cursor()
    cur.execute('''CREATE TABLE RawSourceData (
                    HOLDER TEXT,
                    ISSUER TEXT,
                    ISIN TEXT,
                    INTEREST REAL,
                    POSITION_DATE TEXT,
                    MIC TEXT,
                    COMMENT TEXT,
                    UPDATE_DATE TEXT
                    );''')

    cur.execute('''CREATE TABLE SourceData (
                    HOLDER TEXT NOT NULL,
                    ISSUER TEXT NOT NULL,
                    ISIN TEXT,
                    INTEREST REAL NOT NULL,
                    POSITION_DATE TEXT NOT NULL,
                    MIC TEXT,
                    COMMENT TEXT,
                    UPDATE_DATE TEXT
                    );''')
    
    cur.close()
    con.close()


# function to truncate RawSourceData to database
def TruncateRawSourceData():

    con = lite.connect('XPAR_PDF.db')
    cur = con.cursor()
    cur.execute("DELETE FROM RawSourceData;")
    con.commit()
    cur.close()
    con.close()


    
# function to add to database
def AddToDB(tpl, cancellation=False):

    con = lite.connect('XPAR_PDF.db')
    cur = con.cursor()
    
    if cancellation != True:
        holder, issuer, isin, interest, position_date = tpl
        position_date = position_date.replace('-', '')
        cur.execute('''INSERT INTO RawSourceData (HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC, UPDATE_DATE)
                   VALUES ("{}","{}","{}","{}","{}","{}","{}");'''.format(holder, issuer, isin, interest, position_date, 'XPAR', TimeStamp("%Y%m%d_%Hh%M")))
    else:
        holder, issuer, isin, position_date = tpl
        position_date = position_date.replace('-', '')
        cur.execute('''INSERT INTO RawSourceData (HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC, UPDATE_DATE, COMMENT)
                   VALUES ("{}","{}","{}","{}","{}","{}","{}","{}");'''.format(holder, issuer, isin, '0.0', position_date, 'XPAR', TimeStamp("%Y%m%d_%Hh%M"),'XPAR_PDF'))
    con.commit()
    cur.close()
    con.close()


    
# function to get links to PDFs
def GetLinksToPDFs(url):
    
    #get HTML
    r = requests.get(url)
    html = r.text
    soup = BeautifulSoup(html, 'lxml')

    links_dict = {}
    #find all HREFs
    for a_tag in soup.find_all('a'):
        if 'VIEW DOCUMENT' in a_tag.text.upper():
            #print(link.text)
            txt = a_tag['title'].upper()
            t = txt.find('EXAMINATIONS')
            issuer = txt[t+12:].strip()
            link = a_tag.get('href')
            #print(issuer)
            #print(link)
            links_dict[link] = issuer

    return links_dict


def GetPDF(url):

    site = 'http://www.amf-france.org'
    url = site + url
    #get HTML
    r = requests.get(url)
    html = r.text
    soup = BeautifulSoup(html, 'lxml')

    #find all HREFs
    for a_tag in soup.find_all('a'):
        if 'VIEW DOCUMENT' in a_tag.text.upper():
            link = a_tag.get('href')
            pdf_link = site + link
            #print(pdf_link)

    return pdf_link


def ReadPDF(pdf_link, issuer):

    # get PDF
    stamp = TimeStamp("%Y%m%d_%Hh%Mm%Ss%f")
    pdf_name = "/home/daniel/Desktop/Scripts/_ShortNotifications/Files/{}_{}.pdf".format(stamp, issuer)
    pdf_file = urllib.request.urlretrieve(pdf_link, pdf_name)

    #create pdf object of first page
    pdfObj = open(pdf_name, 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfObj)
    pageObj = pdfReader.getPage(0)

    text = pageObj.extractText().upper()
    #print(text)
    Logging('\n')

    if text.find('CANCELLATION') < 0:
        alt = text.find('@')
        line = text[alt:].find('AMF.')
        #print(text[alt+line:])
        notification = text[alt+line+15:]
        Logging(notification)

        Logging("Issuer: {}".format(issuer))
        
        position_date = notification[-10:]
        Logging("Position Date : {}".format(position_date))

        isin = notification[-34:-22]
        Logging("ISIN : {}".format(isin))

        start = notification.find(isin)
        string = re.findall('\d{4}-\d{2}-\d{2}', notification)[0]
        end = notification.find(string)
        interest = notification[start+12:end]
        Logging("Short Interest : {}".format(interest))

        h = notification.find(issuer)
        holder = notification[:h]
        Logging("Holder: {}".format(holder))

    else:
        Logging('### CANCELLATION ###')
        alt = text.find('@')
        line = text[alt:].find('AMF.')
        #print(text[alt+line:])
        notification = text[alt+line+19:]
        #print(notification)

        Logging("Issuer: {}".format(issuer))

        s = notification.find(';') + 1
        i = notification.find(issuer)
        holder = notification[s:i]
        Logging("Holder: {}".format(holder))

        l = len(issuer)
        isin = notification[i+l:i+l+12]
        Logging("ISIN : {}".format(isin))

        p = notification.find('ANNULATION')
        position_date = notification[p-10:p]
        Logging("Position Date : {}".format(position_date))
        
        cancellation_date = notification[-10:]
        Logging("Cancellation Date : {}".format(cancellation_date))
    
        start = notification.find(isin)
        string = re.findall('\d{4}-\d{2}-\d{2}', notification)[0]
        end = notification.find(string)
        interest = notification[start+12:end]
        Logging("Short Interest : {}".format(interest))

        # add cancellation to database
        tpl = (holder, issuer, isin, cancellation_date)
        AddToDB(tpl,cancellation=True)
        
        
    #print('\n')
    pdfObj.close()

    return (holder, issuer, isin, interest, position_date)


if __name__ == "__main__":

    #create database
    #CreateDBForParis()

    # truncate RawSourceData
    TruncateRawSourceData()

    #url = "http://www.amf-france.org/en_US/Resultat-de-recherche-BDIF?isSearch=true&DOC_TYPE=BDIF&TEXT=&REFERENCE=&RG_NUM_ARTICLE=&RG_LIVRE=&DATE_PUBLICATION=01%2F03%2F2017&DATE_OBSOLESCENCE=&DATE_VIGUEUR_DEBUT=&DATE_VIGUEUR_FIN=&LANGUAGE=en&INCLUDE_OBSOLESCENT=false&subFormId=dij&BDIF_TYPE_INFORMATION=&BDIF_RAISON_SOCIALE=&bdifJetonSociete=&BDIF_TYPE_DOCUMENT=&BDIF_TYPE_OPERATION=&BDIF_MARCHE=&BDIF_INSTRUMENT_FINANCIER=&BDIF_NOM_PERSONNE=&ORDER_BY=PERTINENCE&bdiftypedocument=BdifTypeDocument%3Bsourcestr11%3Bnotification+of+net+short+positions%3BNotification+of+net+short+positions"
    for i in range(1,50):
        
        Logging("\n##### Page {} #####\n".format(str(i)))
        
        date_obj = datetime.date.today() + datetime.timedelta(-60)
        m = datetime.datetime.strftime(date_obj, "%m")
        
        try:
            # url for all (from 01/01/2012)
            #url = "http://www.amf-france.org/en_US/Resultat-de-recherche-BDIF?PAGE_NUMBER=" + str(i) +"&LANGUAGE=en&BDIF_NOM_PERSONNE=&TEXT=&RG_LIVRE=&DATE_OBSOLESCENCE=&DATE_VIGUEUR_DEBUT=&BDIF_TYPE_DOCUMENT=&BDIF_TYPE_INFORMATION=&bdifJetonSociete=&BDIF_INSTRUMENT_FINANCIER=&ORDER_BY=PERTINENCE&subFormId=dij&DOC_TYPE=BDIF&BDIF_RAISON_SOCIALE=&isSearch=true&REFERENCE=&INCLUDE_OBSOLESCENT=false&DATE_VIGUEUR_FIN=&BDIF_TYPE_OPERATION=&bdiftypedocument=BdifTypeDocument%3Bsourcestr11%3Bnotification+of+net+short+positions%3BNotification+of+net+short+positions&RG_NUM_ARTICLE=&DATE_PUBLICATION=01%2F03%2F2012&BDIF_MARCHE="

            # url from 2 months back
            url = "http://www.amf-france.org/en_US/Resultat-de-recherche-BDIF?PAGE_NUMBER=" + str(i) +"&LANGUAGE=en&BDIF_NOM_PERSONNE=&TEXT=&RG_LIVRE=&DATE_OBSOLESCENCE=&DATE_VIGUEUR_DEBUT=&BDIF_TYPE_DOCUMENT=&BDIF_TYPE_INFORMATION=&bdifJetonSociete=&BDIF_INSTRUMENT_FINANCIER=&ORDER_BY=PERTINENCE&subFormId=dij&DOC_TYPE=BDIF&BDIF_RAISON_SOCIALE=&isSearch=true&REFERENCE=&INCLUDE_OBSOLESCENT=false&DATE_VIGUEUR_FIN=&BDIF_TYPE_OPERATION=&bdiftypedocument=BdifTypeDocument%3Bsourcestr11%3Bnotification+of+net+short+positions%3BNotification+of+net+short+positions&RG_NUM_ARTICLE=&DATE_PUBLICATION=01%2F" + str(m) + "%2F2017&BDIF_MARCHE="
            links = GetLinksToPDFs(url)

            for l,i in links.items():
                pdf = GetPDF(l)
                tpl = ReadPDF(pdf, i)
                AddToDB(tpl)
                
        except Exception as e:
            Logging("Error: {}".format(str(e)))


    # closing
    endTime = time.time()
    Logging("Script ran for: {}sec.".format(round(endTime - startTime,2)))



    
