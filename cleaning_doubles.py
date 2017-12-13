'''
This script will clean the data after extraction.
To clean doubles with respect to name, based on same ISIN.
'''

import sqlite3 as lite
import time

# list of tuples: MIC, ISIN & New name
cleaning_issuers = [('XPAR', 'FR0006174348', 'BUREAU VERITAS'),
                 ('XPAR', 'FR0000120164', 'CGG - VERITAS'),
                 ('XPAR', 'FR0000050809', 'SOPRA STERIA GROUP'),
                 ('XPAR', 'FR0000060873', 'MARIE BRIZARD WINE AND SPIRITS'),
                 ('XPAR', 'FR0000079600', 'GAMELOFT'),
                 ('XPAR', 'FR0000125346', 'INGENICO GROUP'),                 
                 ('XPAR', 'FR0004026714', 'CLARANOVA'),
                 ('XPAR', 'FR0010096354', 'SOLOCAL GROUP'),
                 ('XPAR', 'FR0010613471', 'SUEZ ENVIRONNEMENT'),
                 ('XPAR', 'NL0000235190', 'AIRBUS')                 
                  ]


cleaning_holders = [('XPAR', 'WELLINGTON MANAGEMENT COMPANY', 'WELLINGTON MANAGEMENT COMPANY'),
                    ('XPAR', 'BLACKROCK INSTITUTIONAL TRUST COMPANY NATIONAL ASSOCIATION', 'BLACKROCK INSTITUTIONAL TRUST'),
                    ('XPAR', 'BLACKROCK INVESTMENT MANAGEMENT UK LIMITED', 'BLACKROCK INVESTMENT MANAGEMENTUK'),
                    ('XPAR', 'ARROWGRASS CAPITAL PARTNERS L.L.P.', 'ARROWGRASS CAPITAL PARTNERSL'),
                    ('XLIS', 'J.P. MORGAN ASSET MANAGEMENT (UK) LIMITED', 'JPMORGAN ASSET MANAGEMENT (UK)'),
                    ('XAMS', 'SCOPIA CAPITAL MANAGEMENT', 'SCOPIA CAPITAL MANAGEMENT'),
                    ('XPAR', 'CONNOR, CLARK & LUNN INVESTMENT MANAGEMENT LTD', 'CONNOR, CLARK & LUNN INVESTMENTM'),
                    ('XPAR', 'INSIGHT INVESTMENT MANAGEMENT (GLOBAL) LIMITED', 'INSIGHT INVESTMENT MANAGEMENT(GLOBAL)')
                    ]

"""
with FullData as (
select distinct ISSUER, ISIN
from SourceData
), Doubles as 
(select ISIN from FullData
group by isin having count(*) > 1
)
select ISSUER, ISIN
from FullData
where ISIN in (select ISIN from Doubles)
order by isin;
"""

# measure duration of run
startTime = time.time()

def CleaningIssuerNames(tbl, cleaning_tuple):

    mic = cleaning_tuple[0]
    isin = cleaning_tuple[1]
    new_name = cleaning_tuple[2]

    # connections
    con = lite.connect("{}.db".format(mic))
    cur = con.cursor()
    
    # correct names
    cur.execute('update "{0}" set ISSUER="{1}" where ISIN="{2}";'.format(tbl, new_name, isin))

    # save & close connections
    con.commit()
                  
    cur.close()
    con.close()


def CleaningHolderNames(tbl, cleaning_tuple):

    mic = cleaning_tuple[0]
    holder = cleaning_tuple[1]
    like_holder = cleaning_tuple[2]

    # connections
    con = lite.connect("{}.db".format(mic))
    cur = con.cursor()
    
    # correct names
    cur.execute('update "{0}" set HOLDER="{1}" where HOLDER like "%{2}%";'.format(tbl, holder, like_holder))

    # save & close connections
    con.commit()
                  
    cur.close()
    con.close()
    

# only run when this is main script
if __name__ == "__main__":

    print("Cleaning the ISSUER names and HOLDER names..")
    
    # run functions
    for tpl in cleaning_issuers:
        CleaningIssuerNames('SourceData', tpl)
        CleaningIssuerNames('RawSourceData', tpl)
        CleaningIssuerNames('Corrections', tpl)

    for tpl in cleaning_holders:
        CleaningHolderNames('SourceData', tpl)
        CleaningHolderNames('RawSourceData', tpl)
        CleaningHolderNames('Corrections', tpl)

    print("\nDone.\n")
    # print duration of run
    endTime = time.time()
    print("Script ran for {} seconds.".format(round(endTime-startTime,2)))

