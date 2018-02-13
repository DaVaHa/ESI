'''
This script creates the web app in Flask.
'''

from flask import Flask, render_template, request, url_for
import sqlite3 as lite
from functions import TimeStamp, QueryDB, InsertDB

app = Flask(__name__)


@app.route('/', methods=['GET','POST'])
@app.route('/home/', methods=['GET','POST'])
def HomePage():
    
    if request.method == 'POST' and request.form['name'] != "":
        
        string = request.form['name'].replace('"','').replace(';','')
        name = ''.join([i for i in string.replace('_',' ') if i.isalnum()])
        
        query =  """SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER
                    FROM Issuers
                    WHERE UPPER(PRETTY_NAME) LIKE "%{}%" AND DELETED = 0
                    ORDER BY LATEST_INTEREST DESC;""".format(string.upper())

    else: # shows current highest SI
        query =  """SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER
                    FROM Issuers
                    WHERE DELETED = 0
                    ORDER BY LATEST_INTEREST DESC
                    LIMIT 3;"""
        
    # same for if & else
    query_data = QueryDB(query, 'SummaryDB.db')
    graphs = [(g[0],g[1],g[2],g[3]) for g in query_data]  #incl issuer for notifications (see below)

    notifs_dict = {}
    for g in graphs[:5]:
        qry = '''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE
                 FROM Latest_Notifications
                 WHERE ISSUER = "{}"
                 ORDER BY POSITION_DATE DESC;'''.format(g[3])
        qry_data = QueryDB(qry, 'SummaryDB.db')
        data = [(d[0],d[1],d[2],d[3],d[4]) for d in qry_data]
        notifs_dict[g[3]] = data

    # return template
    return render_template('homepage.html', graphs=graphs[:5], notifs_dict=notifs_dict)


@app.route('/notifications/', methods=['GET','POST'])
def Notifications():
    
    mics = [{'name':['XBRU','BE - Euronext Brussels']},
            {'name':['XPAR','FR - Euronext Paris']},
            {'name':['XAMS','NL - Euronext Amsterdam']},
            {'name':['XLIS','PT - Euronext Lisbon']}
            ]
    
    if request.method == 'POST':
        
        mic = request.form['myMic']
        mic_db = "{}.db".format(mic)
        
        string = request.form['name']
        name = ''.join([i for i in string if i.isalnum()])
        
        query = '''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC
                   FROM SourceData
                   WHERE (ISSUER LIKE "%{0}%" OR HOLDER LIKE "%{0}%" OR ISIN LIKE "%{0}%") AND COMMENT not like "%_%"
                   ORDER BY POSITION_DATE DESC
                   LIMIT 50;'''.format(name.upper())
        query_data = QueryDB(query, mic_db)
    
    else:
        query = '''SELECT * FROM SourceData
                   WHERE COMMENT not like "%_%"
                   ORDER BY POSITION_DATE DESC
                   LIMIT 100;'''
        query_data = QueryDB(query, 'XBRU.db')
        mic = 'XBRU'
    
    return render_template('notifications.html', data=query_data, mics=mics, selection=mic)


@app.route('/interactive/', methods=['GET','POST'])
def Interactive():
    if request.method == 'POST' and request.form['name'] != "":
        
        string = request.form['name'].replace('"','')
        name = ''.join([i for i in string.replace('_',' ') if i.isalnum()])
        
        query =  """SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER
                    FROM Issuers
                    WHERE UPPER(PRETTY_NAME) LIKE "{}%" AND DELETED = 0
                    ORDER BY PRETTY_NAME
                    LIMIT 1;""".format(string.upper())

    else:  # current highest SI
        query = """ SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER
                    FROM Issuers
                    WHERE DELETED = 0
                    ORDER BY LATEST_INTEREST DESC
                    LIMIT 1;"""
        
    # same for if & else
    query_data = QueryDB(query, 'SummaryDB.db')
    graphs = [(g[0],g[1],g[2].replace('.png', '.html'),g[3]) for g in query_data]  #incl issuer for notifications (see below)

    return render_template('interactive.html', graphs=graphs[:1])


@app.route('/stats/')
def Stats():

    date = TimeStamp('%d/%m/%Y')
    return render_template('statslinks.html', date=date)


@app.route('/stats/all-time-highest/')
def StatsAllTimeHighest():

    query = ''' SELECT PRETTY_NAME, MAX_SI, GRAPH_NAME
                FROM Issuers
                WHERE DELETED = 0
                ORDER BY MAX_SI DESC
                LIMIT 10;'''
    
    query_data = QueryDB(query, 'SummaryDB.db')
    graphs = [(g[0],g[1],g[2]) for g in query_data]

    return render_template('statsalltime.html', graphs=graphs)


@app.route('/stats/current-highest/', methods=['GET','POST'])
def StatsCurrentHighest():

    mics = [{'name':['X','ALL']},
            {'name':['XBRU','BE - Euronext Brussels']},
            {'name':['XPAR','FR - Euronext Paris']},
            {'name':['XAMS','NL - Euronext Amsterdam']},
            {'name':['XLIS','PT - Euronext Lisbon']}
            ]

    if request.method == 'POST':

        mic = request.form['myMic']

        query = ''' SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER
                    FROM Issuers
                    WHERE DELETED = 0 AND MIC like '%{}%'
                    ORDER BY LATEST_INTEREST DESC
                    LIMIT 10; '''.format(mic)

    # select ALL
    else:
        query = ''' SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER
                    FROM Issuers
                    WHERE DELETED = 0
                    ORDER BY LATEST_INTEREST DESC
                    LIMIT 10; '''
        mic = 'X'

    # same for if & else  
    query_data = QueryDB(query, 'SummaryDB.db')
    graphs = [(g[0],g[1],g[2],g[3]) for g in query_data]  #incl issuer for notifications (see below)

    notifs_dict = {}
    for g in graphs:
        qry = '''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE
                 FROM Latest_Notifications
                 WHERE ISSUER = "{}"
                 ORDER BY POSITION_DATE DESC;'''.format(g[3])
        qry_data = QueryDB(qry, 'SummaryDB.db')
        data = [(d[0],d[1],d[2],d[3],d[4]) for d in qry_data]
        notifs_dict[g[3]] = data
        
    # return template
    return render_template('statscurrent.html', graphs=graphs, notifs_dict=notifs_dict, mics=mics, selection=mic)


@app.route('/stats/one-month-rising/')
def StatsRisingOneMonth():

    query = ''' SELECT PRETTY_NAME, LATEST_INTEREST - CHANGE_1M "Rising1M", GRAPH_NAME
                FROM Issuers
                WHERE DELETED = 0
                ORDER BY "Rising1M" DESC
                LIMIT 10;'''
    
    query_data = QueryDB(query, 'SummaryDB.db')
    graphs = [(g[0],"%.2f" % g[1],g[2]) for g in query_data]

    return render_template('statschanges.html', graphs=graphs, sign='+', header="Strongest Rising Short Interest (One Month)")


@app.route('/stats/three-months-rising/')
def StatsRisingThreeMonths():

    query = ''' SELECT PRETTY_NAME, LATEST_INTEREST - CHANGE_3M "Rising3M", GRAPH_NAME
                FROM Issuers
                WHERE DELETED = 0
                ORDER BY "Rising3M" DESC
                LIMIT 10;'''
    
    query_data = QueryDB(query, 'SummaryDB.db')
    graphs = [(g[0],"%.2f" % g[1],g[2]) for g in query_data]
    
    return render_template('statschanges.html', graphs=graphs, sign='+', header="Strongest Rising Short Interest (Three Months)")


@app.route('/stats/one-month-declining/')
def StatsDecliningOneMonth():
    
    query = ''' SELECT PRETTY_NAME, LATEST_INTEREST - CHANGE_1M "Declining1M", GRAPH_NAME
                FROM Issuers
                WHERE DELETED = 0
                ORDER BY "Declining1M" ASC
                LIMIT 10;'''
    
    query_data = QueryDB(query, 'SummaryDB.db')
    graphs = [(g[0],"%.2f" % g[1],g[2]) for g in query_data]
    
    return render_template('statschanges.html', graphs=graphs, sign='', header="Strongest Declining Short Interest (One Month)")


@app.route('/stats/three-months-declining/')
def StatsDecliningThreeMonths():
    
    query = ''' SELECT PRETTY_NAME, LATEST_INTEREST - CHANGE_3M "Declining3M", GRAPH_NAME
                FROM Issuers
                WHERE DELETED = 0
                ORDER BY "Declining3M" ASC
                LIMIT 10;'''
    
    query_data = QueryDB(query, 'SummaryDB.db')
    graphs = [(g[0],"%.2f" % g[1],g[2]) for g in query_data]

    return render_template('statschanges.html', graphs=graphs, sign='', header="Strongest Declining Short Interest (Three Months)")


@app.route('/about/')
def About():
    
    return render_template('about.html')


@app.route('/contact/', methods=['GET','POST'])
def Contact():
    if request.method == 'POST':

        person = request.form['person'].replace('"','').replace(';','')
        email = request.form['email'].replace('"','').replace(';','')
        subject = request.form['subject'].replace('"','').replace(';','')
        comment = request.form['comment'].replace('"','').replace(';','')

        query = """INSERT INTO Comments (PERSON,EMAIL,SUBJECT,COMMENT,TIMESTAMP)
                       VALUES ("{0}","{1}","{2}","{3}","{4}");""".format(person,email,subject,comment,TimeStamp("%Y%m%d %H:%M:%S %p"))
        InsertDB(query, 'Contact.db')
        
        message="Thanks for the feedback!"
        
        return render_template('contact.html', message=message)
    else:
        return render_template('contact.html', message='')
    

## test
if __name__ == "__main__":
    app.debug = True
    app.run(host = '127.0.0.1', port = 6969)

