from bs4 import BeautifulSoup
from socket import error as SocketError
import urllib
import pandas
import re
import requests
import time
import random
import MySQLdb
import sys

startTime = time.time()
    
db = MySQLdb.connect(
    host = '127.0.0.1',
    user = 'root', #like root
    passwd = '310116', #password
    db = 'diktiScraperdb', #name of database that you used
)
cur = db.cursor() #connect to db as instruction

web = 'http://localhost/banpt/banpt.html'
r = requests.get(web)
scrape0 = BeautifulSoup(r.text, "html.parser")
#print scrape
dataAkreditasi = {
    'wilayah' : [],
    'strata' : [],
    'perguruanTinggi' : [],
    'kotaKab' : [],
    'prodi' : [],
    'noSK' : [],
    'tahunSK' : [],
    'peringkat' : [],
    'tanggalKadaluarsa' : [],
    'status' : [],
}
counter = 0
scrape = scrape0.find('table', {"class": "instrumen"})
for record in scrape.findAll('td'):
    #print str(counter) +' - ' + record.text
    #line = counter/10
    inRecord = record.text
    tempList0 = inRecord.split()
    tempWord0 = ''
    for item in tempList0:
    	tempWord0 = tempWord0 + item + " "
   	inRecord = tempWord0.encode('utf-8')
    if inRecord <> " " :
	    if counter%10 == 1:
	        print inRecord
	        dataAkreditasi['wilayah'].append(inRecord)
	    elif counter%10 == 2:
	        if inRecord == 'D-IV ':
	            inRecord = 'D4'
	        elif inRecord == 'D-III ':
	            inRecord = 'D3'
	            print inRecord
	        elif inRecord == 'D-II ':
	            inRecord = 'D2'
	        elif inRecord == 'D-I ':
	            inRecord = 'D1'
	        print inRecord
	        dataAkreditasi['strata'].append(inRecord)
	    elif counter%10 == 3:
	        tempList = []
	        tempList = inRecord.split(',')
	        print tempList[0]
	        dataAkreditasi['perguruanTinggi'].append(tempList[0])
	        tempWord = tempList[1]
	        tempList = []
	        tempList = tempWord.split()
	        print tempList[0]
	        dataAkreditasi['kotaKab'].append(tempList[0])
	    elif counter%10 == 4:
	        print inRecord
	        dataAkreditasi['prodi'].append(inRecord)
	    elif counter%10 == 5:
	        print inRecord
	        dataAkreditasi['noSK'].append(inRecord)
	    elif counter%10 == 6:
	        print inRecord
	        dataAkreditasi['tahunSK'].append(inRecord)
	    elif counter%10 == 7:
	        print inRecord
	        dataAkreditasi['peringkat'].append(inRecord)
	    elif counter%10 == 8:
	        print inRecord
	        dataAkreditasi['tanggalKadaluarsa'].append(inRecord)
	    elif counter%10 == 9:
	        print inRecord
	        dataAkreditasi['status'].append(inRecord)
	    counter = counter+1
	    print '==='
dfAkreditasi = pandas.DataFrame(dataAkreditasi, columns=['wilayah', 'strata', 'perguruanTinggi', 'kotaKab', 'prodi', 'noSK', 'tahunSK', 'peringkat', 'tanggalKadaluarsa', 'status'])
dfAkreditasi.to_csv('dfAkreditasi.csv')
print 'Done'
