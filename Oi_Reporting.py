__author__ = 'brucepannaman'

import MySQLdb
import datetime
import calendar
import configparser
import csv
import os
from subprocess import call
import psycopg2

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


start_day = 1
end_day = 31
month = 8
year = datetime.date.today().year

start_date = datetime.date(year, month, start_day)
end_date = datetime.date(year, month, end_day)

# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

print "Deleting current stats for %s" % start_date
cursor.execute("delete oi_key_stats where month = '%s';" % start_date)

conn.commit()
conn.close()


# Set up csv file to write data to
with open('OI_report_%s.csv' % start_date, 'wb') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"')


    # Open database connection
    db = MySQLdb.connect(host =HOST, user =USER, passwd= PASSWORD, db ="busuudata")

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    print "Calculating for month starting " + str(start_date) + " and ending on " + str(end_date) + "\n"

    list = []
    list.append(str(start_date))


    # TOTAL NUMBER OF OI USERS
    cursor.execute("SELECT count(DISTINCT mpa.uid) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u ON u.uid = mpa.uid WHERE mpa.uid is not null AND mpas.start <= '%s' AND mpa.partner = 'oi' AND mpa.msisdn like '55%s' AND mpas.current =1;" % (end_date, "%"))
    data = cursor.fetchone()
    print 'Number of registered Oi users at %s was ' % end_date + (str(data).replace("L,)","")).replace("(","")
    list.append(int((str(data).replace("L,)","")).replace("(","")))

    # TOTAL NUMBER OF NEW USERS WITH CONFIRMED EMAIL
    cursor.execute("SELECT count(DISTINCT mpa.msisdn) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u ON u.uid = mpa.uid WHERE mpa.uid is not null AND mpas.unsubscribed IS NULL AND mpas.start <= '%s' AND mpas.end >= '%s' AND mpa.partner = 'oi' AND u.mail NOT LIKE '%soibusuu%s' AND mpa.msisdn like '55%s' AND mpas.current = 1;" % (end_date, start_date, "%", "%", "%"))
    data = cursor.fetchone()
    print 'Number of new oi users with confirmed email addresses between %s and %s was ' % (start_date, end_date) + (str(data).replace("L,)","")).replace("(","")
    list.append(int((str(data).replace("L,)","")).replace("(","")))

    # TOTAL NUMBER OF NEW OI USERS
    cursor.execute("SELECT count(DISTINCT mpa.uid) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u ON u.uid = mpa.uid WHERE mpa.uid is not null AND mpas.start >= '%s' AND mpas.start < '%s' AND mpa.partner = 'oi' and mpa.msisdn like '55%s' AND mpas.current = 1;" % (start_date, end_date, "%"))
    data = cursor.fetchone()
    print 'Number of new oi users between %s and %s was ' % (start_date, end_date) + (str(data).replace("L,)","")).replace("(","")
    list.append(int((str(data).replace("L,)","")).replace("(","")))

    # TOTAL NUMBER OF OI USERS REQUESTED TO CANCEL
    cursor.execute("SELECT COUNT(DISTINCT mpa.msisdn) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u ON u.uid = mpa.uid WHERE mpa.uid is not null AND mpas.unsubscribed >= '%s' AND mpas.unsubscribed < '%s' AND mpa.partner = 'oi' AND mpa.msisdn like '55%s' AND mpas.current = 1;" % (start_date, end_date,"%"))
    data = cursor.fetchone()
    print 'Number of oi users that requested to cancelled between %s and %s was ' % (start_date, end_date) + (str(data).replace("L,)","")).replace("(","")
    list.append(int((str(data).replace("L,)","")).replace("(","")))

    # TOTAL NUMBER OF OI USERS THAT OWE US MONEY
    cursor.execute("SELECT count(distinct `msisdn`) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u ON u.uid = mpa.uid WHERE mpa.uid is not null AND mpas.start < '%s' AND mpas.end >= '%s' AND mpas.unsubscribed IS NULL AND mpa.partner = 'oi' AND mpa.msisdn like '55%s' AND mpas.current = 1;" % (end_date, start_date, "%"))
    data = cursor.fetchone()
    print 'Number of oi users that owe us money between %s and %s was ' % (start_date, end_date) + (str(data).replace("L,)","")).replace("(","")
    list.append(int((str(data).replace("L,)","")).replace("(","")))

    # TOTAL NUMBER OF OI USERS THAT HAVE A FAKE EMAIL ADDRESS
    cursor.execute("SELECT count(DISTINCT mpa.msisdn) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u ON u.uid = mpa.uid WHERE mpa.uid is not null AND mpas.unsubscribed IS NULL AND mpas.start <= '%s' AND mpas.end >= '%s'and mpa.partner = 'oi' AND u.mail LIKE '%soibusuu%s' AND mpa.msisdn like '55%s' AND mpas.current = 1;" % (end_date, start_date, "%","%","%"))
    data = cursor.fetchone()
    print 'Number of oi users that have a fake email address between %s and %s was ' % (start_date, end_date) + (str(data).replace("L,)","")).replace("(","")
    list.append(int((str(data).replace("L,)","")).replace("(","")))

    # TOTAL NUMBER OF OI USERS THAT HAVE OPENED THE APP
    cursor.execute("SELECT count(DISTINCT mpa.uid) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u ON u.uid = mpa.uid WHERE mpa.uid is not null AND mpas.unsubscribed IS NULL AND date(from_unixtime(u.access)) >= '%s' AND date(from_unixtime(u.access)) < '%s' AND u.mail NOT LIKE '%soibusuu%s' AND mpa.partner = 'oi' AND mpas.current = 1;" % (start_date, end_date, "%","%"))
    data = cursor.fetchone()
    print 'The total number of oi users that have opened the app at some point between %s and %s ' % (start_date, end_date) + (str(data).replace("L,)","")).replace("(","")
    list.append(int((str(data).replace("L,)","")).replace("(","")))

    # TOTAL NUMBER OF OI USERS THAT HAVE NOT OPENED THE APP
    cursor.execute("SELECT count(DISTINCT mpa.uid) FROM bs_mobile_partner_accounts mpa LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id LEFT JOIN users u ON u.uid = mpa.uid WHERE mpa.uid is not null AND mpas.unsubscribed IS NULL AND from_unixtime(access) < '%s' AND u.mail NOT LIKE '%soibusuu%s' AND mpa.partner = 'oi' AND mpas.current = 1;" % (end_date, "%","%"))
    data = cursor.fetchone()
    print 'The total number of oi users that have  NOT opened the app yet ' + (str(data).replace("L,)","")).replace("(","") + "\n"
    list.append(int((str(data).replace("L,)","")).replace("(","")))

    print list

    writer.writerow(list)

db.close()

# Put finished report into S3
print 'Uploading to s3'
call(["s3cmd", "put", 'OI_report_%s.csv' % start_date,  "s3://bibusuu/OI_Reporting/OI_report_%s.csv" % start_date])

os.remove('OI_report_%s.csv' % start_date)

# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results
print "Updating data in OI_Key_Stats"
cursor.execute("COPY oi_key_stats  FROM 's3://bibusuu/OI_Reporting/OI_report_%s.csv'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' CSV;" % (start_date, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))

conn.commit()
conn.close()
