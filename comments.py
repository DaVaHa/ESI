'''
This script will print all the comments.
'''
import sqlite3 as lite

con = lite.connect('Contact.db')
cur = con.cursor()

cur.execute('''select * from Comments;''')
comments = cur.fetchall()

for feedback in comments:
    
    person, email, subject, comment, timestamp = feedback

    #print out comment
    print("\n###############################")
    print("PERSON: ", person)
    print("TIMESTAMP: ", timestamp)
    print("EMAIL: ", email)
    print("SUBJECT: ", subject)
    print("COMMENT: ", comment)




cur.close()
con.close()
