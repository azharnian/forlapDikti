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

start_time = time.time()
db = MySQLdb.connect(
    host = '127.0.0.1',
    user = 'root',
    passwd = '310116',
    db = 'tes'
)

def openCheckPoint():
    cP = []
    with open('checkPoint.txt') as openFile:
        for item in openFile:
            cP.append(int(item))
            print int(item)
    #reading checkpoin
    return cP
    
def saveCP(i, iS, iF, k, kS, kF):
    openFile = open('checkPoint.txt', 'w')
    openFile.write(str(i)+"\n")
    openFile.write(str(iS)+"\n")
    openFile.write(str(iF)+"\n")
    openFile.write(str(k)+"\n")
    openFile.write(str(kS)+"\n")
    openFile.write(str(kF)+"\n") 
    result = 'SAVED'
    return result
#========

def getInfoProdi(dfProdi):
    cP = openCheckPoint()
    i = cP[0]
    iS = cP[1]
    iF = cP[2]
    k = cP[3]
    kS = cP[4]
    kF = cP[5]
    for link in dfProdi['link']:
        #saving
        kS = 1
        kF = 0
        saveCP(i, iS, iF, k, kS, kF)
        #saving
        print ">"+link
        kode = str(dfProdi['kode'][k])
        prodi = str(dfProdi['prodi'][k])
        status = str(dfProdi['status'][k])
        jenjang = str(dfProdi['jenjang'][k])
        dosenTetap = str(dfProdi['dosenTetap'][k])
        mahasiswa = str(dfProdi['mahasiswa'][k])
        urlProdi = str(link)
        scrapeProdi = BeautifulSoup(urllib.urlopen(urlProdi), "html.parser")
        listInfoProd = []
        listDosen = []
        listTemp = []
        for record in scrapeProdi.findAll('table', {"class": "table1"}):
            l = 1
            for record2 in record.findAll('td'):
                if l % 3 == 0 and l <= 24:
                    #print record2.text
                    listTemp.append(record2.text)
                l = l+1
            l = 1
            for item in listTemp:
                if l == 1 or l == 3 or l == 5 or l == 6:
                    #print item
                    listInfoProd.append(item)
                l=l+1
        listTemp = []
        for record in scrapeProdi.findAll('div', {"id": "dosen"}):
            #print record
            for record2 in record.findAll('td'):
                listTemp.append(record2.text)
                #print listTemp
        for item in listTemp[3::5]:
            #print item
            listDosen.append(item)
        S2 = 0
        S3 = 0
        for item in listDosen:
            if item == 'S2':
                S2 = S2 +1
            elif item == 'S3':
                S3 = S3 +1
        dosenS2 = str(S2)
        dosenS3 = str(S3)
        query9 = 'INSERT INTO infoProdi (idUniv, kode, status, prodi, jenjang, dosenTetap, dosenS2, dosenS3, mahasiswa, link) VALUES ("'+str(i+1)+'","'+kode+'","'+status+'","'+prodi+'","'+jenjang+'","'+dosenTetap+'","'+dosenS2+'","'+dosenS3+'","'+mahasiswa+'","'+link+'");'
        cur.execute(query9)
        db.commit()
        print '>'+str(i+1)+'|'+kode+'|'+status+'|'+prodi+'|'+jenjang+'|dosen='+dosenTetap+'|s2='+dosenS2+'|s3='+dosenS3+'|Mhs='+mahasiswa+'|'
        k = k+1
        #saving
        kS = 1
        kF = 1
        saveCP(i, iS, iF, k, kS, kF)
        #saving
    #saving
    k = 0
    kS = 0
    kF = 0
    saveCP(i, iS, iF, k, kS, kF)
    

#====

def getInfoUniv():
    cP = openCheckPoint()
    i = cP[0]
    iS = cP[1]
    iF = cP[2]
    k = cP[3]
    kS = cP[4]
    kF = cP[5]
    for univ in df['univ'][i:]:
        #saving
        iS = 1
        iF = 0
        k = 0
        kS = 0
        kF = 0
        saveCP(i, iS, iF, k, kS, kF)
        #saving
        print univ
        univRe = re.sub("[ ]", '+', univ)
        urlUniv = ""
        research_later = '"'+univRe+'"+site:http://forlap.dikti.go.id/perguruantinggi/detail/'
        goog_search = "https://www.google.co.uk/search?sclient=psy-ab&client=ubuntu&hs=k5b&channel=fs&biw=1366&bih=648&noj=1&q=" + research_later
        r = requests.get(goog_search)
        wt = random.uniform(10, 20)
        time.sleep(wt)
        scrapeGoogle = BeautifulSoup(r.text, "html.parser")
        j = 1
        for item in scrapeGoogle.findAll('a', href=True):
            if j == 25 : 
                #print i
                #print item['href'][7:104]
                urlUniv = item['href'][7:104]
            j = j+1
        if urlUniv[0:4] == "http":
            print urlUniv
            query6 = 'UPDATE indexUniv SET link = "'+urlUniv+'" WHERE Id = '+str(i+1)+' ;'
            cur.execute(query6)
            db.commit()
            listId = []
            scrapeUniv = BeautifulSoup(urllib.urlopen(urlUniv), "html.parser")
            for record in scrapeUniv.findAll('table', {"class": "table1"}):
                j = 1
                for record2 in record.findAll('td'):
                    if j % 3 == 0:
                        listId.append(record2.text)
                    j = j+1
            status = str(listId[0])
            status = re.sub("[!@#$/']", '', status)
            universitas = str(listId[1])
            universitas = re.sub("[!@#$/']", '', universitas)
            berdiri = str(listId[2])
            berdiri = re.sub("[!@#$/']", '', berdiri)
            noSK = str(listId[3])
            noSK = re.sub("[!@#$/']", '', noSK)
            tanggalSK = str(listId[4])
            tanggalSK = re.sub("[!@#$/']", '', tanggalSK)
            alamat = str(listId[5])
            alamat = re.sub("[!@#$/']", '', alamat)
            kotakab = str(listId[6])
            kotakab = re.sub("[!@#$/']", '', kotakab)
            kodePos = str(listId[7])
            kodePos = re.sub("[!@#$/']", '', kodePos)
            telepon = str(listId[8])
            telepon = re.sub("[!@#$/']", '', telepon)
            fax = str(listId[9])
            fax = re.sub("[!@#$/']", '', fax)
            email = str(listId[10])
            email = re.sub("[!#$/']", '', email)
            website = str(listId[11])
            website = re.sub("[!@#$/']", '', website)
            if universitas + " " == univ:
                #memasukkan ke tabel infoUniv
                print "VALID"
                valid = "Y"
                query7 = 'UPDATE indexUniv SET valid = "'+valid+'" WHERE Id = '+str(i+1)+' ;'
                cur.execute(query7)
                db.commit()
                query8 = 'INSERT INTO infoUniv (idUniv, status, universitas, berdiri, noSK, tanggalSK, alamat, kotakab, kodePos, telepon, fax, email, website) VALUES ("'+str(i+1)+'","'+status+'","'+universitas+'","'+berdiri+'","'+noSK+'","'+tanggalSK+'","'+alamat+'","'+kotakab+'","'+kodePos+'","'+telepon+'","'+fax+'","'+email+'","'+website+'");'
                cur.execute(query8)
                db.commit()
                print '>'+str(i+1)+'|'+status+'|'+universitas+'|'+berdiri+'|'+noSK+'|'+tanggalSK+'|'+alamat+'|'+kotakab+'|'+kodePos+'|'+telepon+'|'+fax+'|'+email+'|'+website
                print '================================================='

                #memasukkan ke tabel infoProdi
                listProdi = []
                listLinkProdi = []
                dataProdi = {
                    'kode' : [],
                    'prodi' : [],
                    'status' : [],
                    'jenjang' : [],
                    'dosenTetap' : [],
                    'mahasiswa' : [],
                    'link' : [],
                }
                for record in scrapeUniv.findAll('table', {"class": "table table-bordered"}):
                    j = 1
                    for record2 in record.findAll('td'):
                        #print record2.text
                        if j % 8 <> 1 and j % 8 <> 0:
                            #print record2.text
                            tempText = ""
                            tempData = record2.text.split()
                            for item in tempData:
                                tempText = tempText + item + " "
                            listProdi.append(tempText)
                        j = j+1
                    for record2 in record.findAll('a', href = True):
                        #print record2['href']
                        listLinkProdi.append(record2['href'])
                j = 1
                for item in listProdi :
                    #print j, ">" ,item
                    if j % 6 == 1:
                        dataProdi['kode'].append(str(item))
                    elif j % 6 == 2 :
                        dataProdi['prodi'].append(str(item))
                    elif j % 6 == 3 :
                        dataProdi['status'].append(str(item))
                    elif j % 6 == 4 :
                        dataProdi['jenjang'].append(str(item))
                    elif j % 6 == 5 :
                        dataProdi['dosenTetap'].append(str(item))
                    elif j % 6 == 0 :
                        dataProdi['mahasiswa'].append(str(item))
                        ###error
                        dataProdi['link'].append(str(listLinkProdi[(j/6)-1]))
                    j = j+1
                dfProdi = pandas.DataFrame(dataProdi, columns=['kode', 'prodi', 'status', 'jenjang', 'dosenTetap', 'mahasiswa', 'link'])
                #print dfProdi
                dfProdi.to_csv('dfProdi.csv')
                dfProdi = pandas.read_csv('dfProdi.csv')
                #saving
                iS = 1
                iF = 1
                k = 0
                kS = 0
                kF = 0
                saveCP(i, iS, iF, k, kS, kF)
                #saving
                getInfoProdi(dfProdi)
                #saving
                #sys.exit("STOPPED")
            else :
                print "TIDAK VALID"
                valid = "T"
                query7 = 'UPDATE indexUniv SET valid = "'+valid+'" WHERE Id = '+str(i+1)+' ;'
                cur.execute(query7)
                db.commit()
                #saving
                iS = 1
                iF = 1
                k = 0
                kS = 0
                kF = 0
                saveCP(i, iS, iF, k, kS, kF)
                #saving
        else:
            print "Tidak Ada Link"
            link = "TidakAda"
            query6 = 'UPDATE indexUniv SET link =  "'+link+'" WHERE Id ='+str(i+1)+' ;'
            cur.execute(query6)
            db.commit()
            print "TIDAK VALID"
            valid = "T"
            query7 = 'UPDATE indexUniv SET valid = "'+valid+'" WHERE Id = '+str(i+1)+' ;'
            cur.execute(query7)
            db.commit()
            #saving
            iS = 1
            iF = 1
            k = 0
            kS = 0
            kF = 0
            saveCP(i, iS, iF, k, kS, kF)
            #saving
        i = i+1
        #saving
        saveCP(i, iS, iF, k, kS, kF)
        #saving
    result = 'done'
    return result

#====


cur = db.cursor()
query0 = 'USE tes;'
cur.execute(query0)
query1 = "SELECT Id, univ FROM indexUniv;"
df = pandas.read_sql(query1 ,db)
cP = openCheckPoint()
i = cP[0]
iS = cP[1]
iF = cP[2]
k = cP[3]
kS = cP[4]
kF = cP[5]
#reading
if i == 0:
    print "Mencari dari awal"
    query2 = "DROP TABLE IF EXISTS infoUniv;"
    query3 = "CREATE TABLE infoUniv (Id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, idUniv VARCHAR(255), status VARCHAR(255), universitas VARCHAR(255), berdiri VARCHAR(255), noSK VARCHAR(255), tanggalSK VARCHAR(255), alamat VARCHAR(255), kotakab VARCHAR(255), kodePos VARCHAR(255), telepon VARCHAR(255), fax VARCHAR(255), email VARCHAR(255), website VARCHAR(255) );"
    cur.execute(query2)
    cur.execute(query3)
    print "infoUniv CREATED"
    query4 = "DROP TABLE IF EXISTS infoProdi;"
    query5 = "CREATE TABLE infoProdi (Id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, idUniv VARCHAR(255), kode VARCHAR(255), status VARCHAR(255), prodi VARCHAR(255), jenjang VARCHAR(255), dosenTetap VARCHAR(255), dosenS2 VARCHAR(255), dosenS3 VARCHAR(255), mahasiswa VARCHAR(255), akreditasi CHAR(1), link VARCHAR(255) );"
    cur.execute(query4)
    cur.execute(query5)
    print "infoProdi CREATED"
    iS = 0
    iF = 0
    k = 0
    kS = 0
    kF = 0
    saveCP(i, iS, iF, k, kS, kF)
    getInfoUniv()
    
else:
    if (cP[1] == 0 and cP[2] == 0) or (cP[1] == 1 and cP[2] == 0) or (cP[4] == 1 and cP[5] == 1):
        print "Mencari dari urutan ke-%s universitas" %i
        iS = 0
        iF = 0
        k = 0
        kS = 0
        kF = 0
        saveCP(i, iS, iF, k, kS, kF)
        getInfoUniv()   
        #====
    elif cP[1] == 1 and cP[2] == 1:
        #print  'Pencarian identitas Univ sudah selesai'
        if cP[3] == 0:
            print "Universitas sudah selesai , lanjut mencari prodi mulai dari 0"
            kS = 0
            kF = 0
            saveCP(i, iS, iF, k, kS, kF)
            dfProdi = pandas.read_csv('dfProdi.csv')
            getInfoProdi(dfProdi)
            getInfoUniv()
        else:
            if (cP[4] == 0 and cP[5] == 0) or (cP[4] == 1 and cP[5] == 0):
                print "Universitas sudah selesai , lanjut mencari prodi mulai dari %s" %k
                saveCP(i, iS, iF, k, kS, kF)
                dfProdi = pandas.read_csv('dfProdi.csv')
                getInfoProdi(dfProdi)
                getInfoUniv()

print("--- %s seconds ---" % (time.time() - start_time))