'''
This script creates the web app in Flask.
'''

from flask import Flask, render_template, request, url_for, redirect, flash
import sqlite3 as lite
from functions import TimeStamp

app = Flask(__name__)


@app.route('/', methods=['GET','POST'])
@app.route('/home/', methods=['GET','POST'])
def HomePage():
    if request.method == 'POST' and request.form['name'] != "":
        
        string = request.form['name'].replace('"','').replace(';','')
        name = ''.join([i for i in string.replace('_',' ') if i.isalnum()])
        
        con = lite.connect('SummaryDB.db')
        cur = con.cursor()
        
        cur.execute("""SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER FROM Issuers
                    WHERE UPPER(PRETTY_NAME) LIKE "%{}%" AND DELETED = 0 ORDER BY LATEST_INTEREST DESC;""".format(string.upper()))
        graphs = [(g[0],g[1],g[2],g[3]) for g in cur.fetchall()]  #incl issuer for notifications (see below)

        notifs_dict = {}
        for g in graphs[:5]:
            cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE
                        FROM Latest_Notifications WHERE ISSUER = "{}" ORDER BY POSITION_DATE DESC;'''.format(g[3]))
            data = [(d[0],d[1],d[2],d[3],d[4]) for d in cur.fetchall()]
            notifs_dict[g[3]] = data
        
        cur.close()
        con.close()
        
        return render_template('homepage.html', graphs=graphs[:5], notifs_dict=notifs_dict)

    else: # shows current highest SI
        con = lite.connect('SummaryDB.db')
        cur = con.cursor()
        cur.execute("""SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER FROM Issuers
                    WHERE DELETED = 0 ORDER BY LATEST_INTEREST DESC LIMIT 3;""")
        graphs = [(g[0],g[1],g[2],g[3]) for g in cur.fetchall()]  #incl issuer for notifications (see below)

        notifs_dict = {}
        for g in graphs[:5]:
            cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE
                        FROM Latest_Notifications WHERE ISSUER = "{}" ORDER BY POSITION_DATE DESC;'''.format(g[3]))
            data = [(d[0],d[1],d[2],d[3],d[4]) for d in cur.fetchall()]
            notifs_dict[g[3]] = data
        
        cur.close()
        con.close()
        return render_template('homepage.html', graphs=graphs[:3], notifs_dict=notifs_dict)


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
        con = lite.connect(mic_db)
        cur = con.cursor()
        string = request.form['name']
        name = ''.join([i for i in string if i.isalnum()])
        cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE, MIC
                FROM SourceData
                WHERE ISSUER LIKE "%{0}%"
                OR HOLDER LIKE "%{0}%"
                OR ISIN LIKE "%{0}%"
                ORDER BY POSITION_DATE DESC
                LIMIT 50;'''.format(name.upper()))
        data = cur.fetchall()
    
        cur.close()
        con.close()
        return render_template('notifications.html', data=data, mics=mics, mic_db=mic_db)
    
    else:
        con = lite.connect('XBRU.db')
        cur = con.cursor()
        cur.execute('SELECT * FROM SourceData ORDER BY POSITION_DATE DESC LIMIT 100;')
        data = cur.fetchall()
        cur.close()
        con.close()
        return render_template('notifications.html', data=data, mics=mics)


@app.route('/interactive/', methods=['GET','POST'])
def Interactive():
    if request.method == 'POST' and request.form['name'] != "":
        
        string = request.form['name'].replace('"','')
        name = ''.join([i for i in string.replace('_',' ') if i.isalnum()])
        
        con = lite.connect('SummaryDB.db')
        cur = con.cursor()
                
        cur.execute("""SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER FROM Issuers
                    WHERE UPPER(PRETTY_NAME) LIKE "{}%" AND DELETED = 0 ORDER BY PRETTY_NAME LIMIT 1;""".format(string.upper()))
        graphs = [(g[0],g[1],g[2].replace('.png', '.html'),g[3]) for g in cur.fetchall()]  #incl issuer for notifications (see below)
            
        cur.close()
        con.close()
        return render_template('interactive.html', graphs=graphs[:1])

    else:  # current highest SI
        con = lite.connect('SummaryDB.db')
        cur = con.cursor()
        cur.execute("""SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER FROM Issuers
                    WHERE DELETED = 0 ORDER BY LATEST_INTEREST DESC LIMIT 1;""")
        graphs = [(g[0],g[1],g[2].replace('.png', '.html'),g[3]) for g in cur.fetchall()]  #incl issuer for notifications (see below)
        cur.close()
        con.close()
        return render_template('interactive.html', graphs=graphs[:1])


@app.route('/stats/')
def Stats():
    con = lite.connect('SummaryDB.db')
    cur = con.cursor()
    cur.execute('''SELECT PRETTY_NAME, MAX_SI, GRAPH_NAME
                   FROM Issuers
                   WHERE DELETED = 0
                   ORDER BY MAX_SI DESC
                   LIMIT 10;''')
    graphs = [(g[0],g[1],g[2]) for g in cur.fetchall()]
    cur.close()
    con.close()
    date = TimeStamp('%d/%m/%Y')
    return render_template('statslinks.html', graphs=graphs, date=date)


@app.route('/stats/all-time-highest/')
def StatsAllTimeHighest():
    con = lite.connect('SummaryDB.db')
    cur = con.cursor()
    cur.execute('''SELECT PRETTY_NAME, MAX_SI, GRAPH_NAME
                   FROM Issuers
                   WHERE DELETED = 0
                   ORDER BY MAX_SI DESC
                   LIMIT 10;''')
    graphs = [(g[0],g[1],g[2]) for g in cur.fetchall()]
    cur.close()
    con.close()
    return render_template('statsalltime.html', graphs=graphs)


@app.route('/stats/current-highest/')
def StatsCurrentHighest():
    con = lite.connect('SummaryDB.db')
    cur = con.cursor()
    cur.execute('''SELECT PRETTY_NAME, LATEST_INTEREST, GRAPH_NAME, ISSUER
                   FROM Issuers
                   WHERE DELETED = 0
                   ORDER BY LATEST_INTEREST DESC
                   LIMIT 10;''')
    graphs = [(g[0],g[1],g[2],g[3]) for g in cur.fetchall()]  #incl issuer for notifications (see below)

    notifs_dict = {}
    for g in graphs:
        cur.execute('''SELECT HOLDER, ISSUER, ISIN, INTEREST, POSITION_DATE
                        FROM Latest_Notifications WHERE ISSUER = "{}" ORDER BY POSITION_DATE DESC;'''.format(g[3]))
        data = [(d[0],d[1],d[2],d[3],d[4]) for d in cur.fetchall()]
        notifs_dict[g[3]] = data

    cur.close()
    con.close()
    return render_template('statscurrent.html', graphs=graphs, notifs_dict=notifs_dict)


@app.route('/stats/one-month-rising/')
def StatsRisingOneMonth():
    con = lite.connect('SummaryDB.db')
    cur = con.cursor()
    cur.execute('''SELECT PRETTY_NAME, LATEST_INTEREST - CHANGE_1M "Rising1M", GRAPH_NAME
                   FROM Issuers
                   WHERE DELETED = 0
                   ORDER BY "Rising1M" DESC
                   LIMIT 10;''')
    graphs = [(g[0],"%.2f" % g[1],g[2]) for g in cur.fetchall()]
    cur.close()
    con.close()
    return render_template('statschanges.html', graphs=graphs, sign='+', header="Strongest Rising Short Interest (One Month)")



@app.route('/stats/three-months-rising/')
def StatsRisingThreeMonths():
    con = lite.connect('SummaryDB.db')
    cur = con.cursor()
    cur.execute('''SELECT PRETTY_NAME, LATEST_INTEREST - CHANGE_3M "Rising3M", GRAPH_NAME
                   FROM Issuers
                   WHERE DELETED = 0
                   ORDER BY "Rising3M" DESC
                   LIMIT 10;''')
    graphs = [(g[0],"%.2f" % g[1],g[2]) for g in cur.fetchall()]
    cur.close()
    con.close()
    return render_template('statschanges.html', graphs=graphs, sign='+', header="Strongest Rising Short Interest (Three Months)")


@app.route('/stats/one-month-declining/')
def StatsDecliningOneMonth():
    con = lite.connect('SummaryDB.db')
    cur = con.cursor()
    cur.execute('''SELECT PRETTY_NAME, LATEST_INTEREST - CHANGE_1M "Declining1M", GRAPH_NAME
                   FROM Issuers
                   WHERE DELETED = 0
                   ORDER BY "Declining1M" ASC
                   LIMIT 10;''')
    graphs = [(g[0],"%.2f" % g[1],g[2]) for g in cur.fetchall()]
    cur.close()
    con.close()
    return render_template('statschanges.html', graphs=graphs, sign='', header="Strongest Declining Short Interest (One Month)")


@app.route('/stats/three-months-declining/')
def StatsDecliningThreeMonths():
    con = lite.connect('SummaryDB.db')
    cur = con.cursor()
    cur.execute('''SELECT PRETTY_NAME, LATEST_INTEREST - CHANGE_3M "Declining3M", GRAPH_NAME
                   FROM Issuers
                   WHERE DELETED = 0
                   ORDER BY "Declining3M" ASC
                   LIMIT 10;''')
    graphs = [(g[0],"%.2f" % g[1],g[2]) for g in cur.fetchall()]
    cur.close()
    con.close()
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
        
        con = lite.connect('Contact.db')
        cur = con.cursor()
        cur.execute("""INSERT INTO Comments (PERSON,EMAIL,SUBJECT,COMMENT,TIMESTAMP)
                       VALUES ("{0}","{1}","{2}","{3}","{4}");""".format(person,email,subject,comment,TimeStamp("%Y%m%d %H:%M:%S %p")))
        con.commit()
        cur.close()
        con.close()
        message="Thanks for the feedback!"
        return render_template('contact.html', message=message)
    else:
        return render_template('contact.html', message='')
    


if __name__ == "__main__":
    app.debug = True
    app.run(host = '127.0.0.1', port = 6969)

