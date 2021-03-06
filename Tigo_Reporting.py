__author__ = 'brucepannaman'

import MySQLdb
import datetime
import calendar
import configparser
import csv
from subprocess import call
import psycopg2
import os

start_day = 1
end_day = calendar.monthrange(datetime.date.today().year, datetime.date.today().month)[1]
month = datetime.date.today().month
year = datetime.date.today().year

start_date = datetime.date(year, month, start_day)
end_date = datetime.date(year, month, end_day)


config = configparser.ConfigParser()
ini = config.read('conf2.ini')

AWS_ACCESS_KEY_ID = config.get('AWS Credentials', 'key')
AWS_SECRET_ACCESS_KEY = config.get('AWS Credentials', 'secret')
HOST = config.get('Stat Slave Creds', 'host')
PORT = config.get('Stat Slave Creds', 'port')
USER = config.get('Stat Slave Creds', 'user')
PASSWORD = config.get('Stat Slave Creds', 'password')
RED_HOST = config.get('Redshift Creds', 'host')
RED_PORT = config.get('Redshift Creds', 'port')
RED_USER = config.get('Redshift Creds', 'user')
RED_PASSWORD = config.get('Redshift Creds', 'password')


# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % conn_string
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()
print "Deleting current stats for %s" % start_date
cursor.execute("delete tigo_key_stats where month = '%s';" % start_date)

conn.commit()
conn.close()

# Set up csv file to write data to
with open('Tigo_report_%s.csv' % start_date, 'wb') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"')


    # Open database connection
    db = MySQLdb.connect(host =HOST, user =USER, passwd= PASSWORD, db ="busuudata")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()


    print "Calculating for month starting " + str(start_date) + " and ending on " + str(end_date) + "\n"

    # TOTAL CUMILATIVE NUMBER OF TIGO USERS
    cursor.execute("SELECT substring(mpa.msisdn,1,3),count(distinct mpa.msisdn) as cnt FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id WHERE mpas.start < '%s' AND mpa.partner = 'tigo' AND mpas.current = 1 AND mpas.provider_plan != 2 GROUP BY 1;" % end_date)
    data = cursor.fetchall()
    for row in data:
        list = []
        list.append(str(start_date))
        if "None" in str(data):
            data = 0
        else:
            data = data
        print row
        print 'Number of cumulative registered Tigo users at %s was ' % end_date + str(row).replace("L,", "").replace("'", '')
        for piece in row:
            list.append((str(piece).replace("L,", "").replace("'", "")))
        list.append("Cumilative Users")
        writer.writerow(list)

    # TOTAL NUMBER OF NEW TIGO USERS WITH CONFIRMED EMAIL
    cursor.execute("SELECT substring(mpa.msisdn, 1,3), count(DISTINCT mpa.msisdn) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u ON u.uid = mpa.uid WHERE mpa.uid is not null AND mpas.provider_plan != 2 AND mpas.unsubscribed IS NULL  AND mpas.start <= '%s' AND mpa.partner = 'tigo' AND mpas.current = 1 group by 1;" % end_date)
    data = cursor.fetchall()
    for row in data:
        list = []
        list.append(str(start_date))
        if "None" in str(data):
            data = 0
        else:
            data = data
        print row
        print 'Number of registered Tigo users with confirmed email at %s was ' % end_date + str(row).replace("L,", "").replace("'", '')
        for piece in row:
            list.append((str(piece).replace("L,", "").replace("'", "")))
        list.append("Confirmed Email")
        writer.writerow(list)

    # TOTAL NUMBER OF NEW TIGO USERS
    cursor.execute("SELECT substring(mpa.msisdn, 1,3), count(DISTINCT mpa.uid) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id WHERE mpas.start >= '%s' AND mpas.start < '%s' AND mpa.partner = 'tigo' AND mpas.unsubscribed IS NULL AND mpas.provider_plan != 2 AND mpas.current = 1 group by 1;" % (start_date, end_date))
    data = cursor.fetchall()
    for row in data:
        list = []
        list.append(str(start_date))
        if "None" in str(data):
            data = 0
        else:
            data = data
        print row
        print 'Number of newly registered Tigo users at %s was ' % end_date + str(row).replace("L,", "").replace("'", '')
        for piece in row:
            list.append((str(piece).replace("L,", "").replace("'", "")))
        list.append("New Subscribers")
        writer.writerow(list)

    # TOTAL NUMBER OF TIGO USERS REQUESTED TO CANCEL
    cursor.execute("SELECT substring(mpa.msisdn,1,3),count(distinct mpa.msisdn)  FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id WHERE mpas.unsubscribed >= '%s' AND mpas.unsubscribed < '%s' AND mpa.partner = 'tigo' AND mpas.current = 1 AND mpas.provider_plan != 2 GROUP BY 1" % (start_date, end_date))
    data = cursor.fetchall()
    for row in data:
        list = []
        list.append(str(start_date))
        if "None" in str(data):
            data = 0
        else:
            data = data
        print row
        print 'Number of registered Tigo users who requested to cancel at %s was ' % end_date + str(row).replace("L,", "").replace("'", '')
        for piece in row:
            list.append((str(piece).replace("L,", "").replace("'", "")))
        list.append("Cancelled Users")
        writer.writerow(list)

    # TOTAL NUMBER OF TIGO USERS THAT DO NOT HAVE A VALIDATED EMAIL
    cursor.execute("SELECT substring(mpa.msisdn,1,3),count(distinct mpa.msisdn) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id WHERE mpas.start < '%s' AND mpas.unsubscribed IS NULL AND mpa.uid IS NULL AND mpa.partner = 'tigo' AND mpas.current = 1 AND mpas.provider_plan != 2 GROUP BY 1;" % end_date)
    data = cursor.fetchall()
    for row in data:
        list = []
        list.append(str(start_date))
        if "None" in str(data):
            data = 0
        else:
            data = data
        print row
        print 'Number of registered Tigo users that do not have a validated email at %s was ' % end_date + str(row).replace("L,", "").replace("'", '')
        for piece in row:
            list.append((str(piece).replace("L,", "").replace("'", "")))
        list.append("Unconfirmed Email Address")
        writer.writerow(list)

    # TOTAL NUMBER OF Tigo USERS THAT HAVE OPENED THE APP
    cursor.execute("SELECT substring(mpa.msisdn,1,3),count(distinct mpa.msisdn) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u on u.uid = mpa.uid WHERE mpas.start < '%s' AND mpas.unsubscribed IS NULL AND mpa.partner = 'tigo' AND date(from_unixtime(u.access)) >= '%s' AND date(from_unixtime(u.access)) < '%s' AND mpas.current = 1 AND mpas.provider_plan != 2 GROUP BY 1;" % (end_date, start_date, end_date))
    data = cursor.fetchall()
    for row in data:
        list = []
        list.append(str(start_date))
        if "None" in str(data):
            data = 0
        else:
            data = data
        print row
        print 'Number of registered Tigo users who have opened the app at %s was ' % end_date + str(row).replace("L,", "").replace("'", '')
        for piece in row:
            list.append((str(piece).replace("L,", "").replace("'", "")))
        list.append("Active Users")
        writer.writerow(list)

    # TOTAL NUMBER OF TIGO USERS THAT HAVE NOT OPENED THE APP
    cursor.execute("SELECT substring(mpa.msisdn,1,3),count(distinct mpa.msisdn) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u on u.uid = mpa.uid WHERE mpas.start <= '%s' AND mpas.unsubscribed IS NULL AND mpa.partner = 'tigo' AND from_unixtime(u.access) < '%s' AND mpas.current = 1 AND mpas.provider_plan != 2 GROUP BY 1;" % (end_date, start_date))
    data = cursor.fetchall()
    for row in data:
        list = []
        list.append(str(start_date))
        if "None" in str(data):
            data = 0
        else:
            data = data
        print row
        print 'Number of registered Tigo users who have not opened app at %s was ' % end_date + str(row).replace("L,", "").replace("'", '')
        for piece in row:
            list.append((str(piece).replace("L,", "").replace("'", "")))
        list.append("Inactive/ Not Opened")
        writer.writerow(list)

    # USERS IN SYSTEM NOT CANCELLED - # THAT THEY NEED TO PAY US FOR
    cursor.execute("SELECT substring(mpa.msisdn,1,3),count(distinct mpa.msisdn) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id WHERE mpas.start < '%s' AND mpas.end >= '%s' AND mpas.unsubscribed IS NULL AND mpa.partner = 'tigo' AND mpas.current = 1 AND mpas.provider_plan != 2 group by 1;" % (end_date, start_date))
    data = cursor.fetchall()
    for row in data:
        list = []
        list.append(str(start_date))
        if "None" in str(data):
            data = 0
        else:
            data = data
        print row
        print 'Number of registered Tigo users who have not cancelled and owe us at %s was ' % end_date + str(row).replace("L,","").replace("'", '')
        for piece in row:
            list.append((str(piece).replace("L,", "").replace("'", "")))
        list.append("Chargeable Users")
        writer.writerow(list)

db.close()

# Put finished report into S3
print 'Uploading to s3'
call(["s3cmd", "put", 'Tigo_report_%s.csv' % start_date,  "s3://bibusuu/Tigo_Reporting/Tigo_report_%s.csv" % start_date])


# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results
print "Adding new Data to Tigo_key_stats"
cursor.execute("COPY tigo_key_stats  FROM 's3://bibusuu/Tigo_Reporting/Tigo_report_%s.csv' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' CSV;" % (start_date, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))


os.remove('Tigo_report_%s.csv' % start_date)

conn.commit()
conn.close()
