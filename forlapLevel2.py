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


class info(object):
    name = ''
    startTime = time.time()
    
    db = MySQLdb.connect(
        host = '127.0.0.1',
        user = 'root', #like root
        passwd = '310116', #password
        db = 'diktiScraperdb', #name of database that you used
    )
    cur = db.cursor() #connect to db as instruction

    def __init__(self, name):
        self.name = name
    
    def read_dataSQLToDf(self, csvFile):
        #print "> read data SQL %s" % csvFile 
        try:
            tempDf = pandas.read_sql(csvFile, self.db)
            if not tempDf.empty:
                return tempDf
        except MySQLdb.IntegrityError:
            sys.exit('Stopped by System')
        finally:
            print '> Already read and export to dataframe'
            #self.cur.close()
            
    def write_dataSQL(self, tempQuery):
        #print "> write data SQL %s" % tempQuery
        try:
            self.cur.execute(tempQuery)
            self.db.commit()
            return True
        except MySQLdb.IntegrityError:
            print 'Error write data in MySQL'
            sys.exit('Stopped by System')
            return False
        finally:
            print '> Already written'
            #self.cur.close()
            
    def exe_SQLToDf(self, tempQuery):
        #print "> execute query SQL %s" % tempQuery
        try :
            tempDf = pandas.read_sql(tempQuery ,self.db)
            #print "> executed"
            return tempDf
        except MySQLdb.IntegrityError:
            print 'Error execute query in MySQL'
            sys.exit('Stopped by system')
            return False
        finally:
            print '> Already executed'
            #self.cur.close()
    
    def exe_SQLFind(self, tempQuery):
        #print "> execute query SQL %s" % tempQuery
        try:
            self.cur.execute(tempQuery)
            result = self.cur.fetchone()[0]
            result = str(result)
            #print "> executed"
            return result
        except MySQLdb.IntegrityError:
            print 'Error execute query in MySQL'
            sys.exit('Stopped by system')
            return False
        finally:
            print '> Already executed'
            #self.cur.close()
            
    def exe_SQL(self, tempQuery):
        #print "> execute query SQL %s" % tempQuery
        try :
            self.cur.execute(tempQuery)
            #print "> executed"
            return True
        except MySQLdb.IntegrityError:
            print 'Error execute query in MySQL'
            sys.exit('Stopped by system')
            return False
        finally:
            print '> Already executed'
            #self.cur.close()
    
    def isForlapExist(self, tempUrl):
        result = ''
        if tempUrl[0:4] == 'http':
            #print tempUrl
            result = tempUrl
            return result
        else:
            return False
            
    def urlForlapInGoogle(self, tempUniv):
        result = ''
        try:
            wt = random.uniform(10, 20)
            tempUniv = re.sub("[ ]", '+', tempUniv)
            research_later = '"'+tempUniv+'"+site:http://forlap.dikti.go.id/perguruantinggi/detail/'
            goog_search = "https://www.google.co.uk/search?sclient=psy-ab&client=ubuntu&hs=k5b&channel=fs&biw=1366&bih=648&noj=1&q=" + research_later
            r = requests.get(goog_search)
            time.sleep(wt)
            scrapeGoogle = BeautifulSoup(r.text, "html.parser")
            tempList = []
            for item in scrapeGoogle.findAll('a', href=True):
                item = str(item['href'])
                if len(item) > 104:
                    tempList.append(item[7:104])

            tempTempList = []
            for item in tempList:
                if item[0:49] == "http://forlap.dikti.go.id/perguruantinggi/detail/":
                    tempTempList.append(item)
            result = tempTempList[0]
            return result
        except Exception as e:
            print e.message
            return result

    def read_checkPoint(self):
        try:
            print "> read data check point"
            fileCheckPoint = 'checkPoint.txt'
            tempList = []
            with open(fileCheckPoint) as tempOpenFile:
                for item in tempOpenFile:
                    tempList.append(int(item))
                    print int(item)
            return tempList
        except IOError:
            print 'Reading Data Failed'
            sys.exit('Stopped by System')
        
    def write_checkPoint(self, tempPointUniv, tempPointStartUniv, tempPointFinishUniv, tempPointProdi, tempPointStartProdi, tempPointFinishProdi, tempCurrentTime):
        try:
            tempOpenFile = open('checkPoint.txt', 'w')
            tempOpenFile.write(str(tempPointUniv)+"\n"+str(tempPointStartUniv)+"\n"+str(tempPointFinishUniv)+"\n"+str(tempPointProdi)+"\n"+str(tempPointStartProdi)+"\n"+str(tempPointFinishProdi)+"\n"+str(tempCurrentTime))
            print "> data check point saved"
            return True
        except IOError:
            print 'Saving Data Failed'
            sys.exit('Stopped by System')
        finally:
            tempOpenFile.close()
    
    def check_dbInfoProdi(self, tempCode, tempIdUniv, tempSqProdi):
        tempCode = str(tempCode); tempIdUniv = str(tempIdUniv); tempSqProdi = str(tempSqProdi)
        tempQuery = 'SELECT EXISTS(SELECT * FROM infoProdi WHERE kode LIKE "%'+tempCode+'%" and idUniv LIKE "%'+tempIdUniv+'%"and sqProdi LIKE "%'+tempSqProdi+'%")'
        if self.exe_SQLFind(tempQuery) == '1':
            return 1
        else:
            return 0
        
    def check_dbInfoUniv(self, tempIdUniv):
        tempIdUniv = str(tempIdUniv)
        tempQuery = 'SELECT EXISTS(SELECT * FROM infoUniv WHERE idUniv LIKE "%'+tempIdUniv+'%")'
        if self.exe_SQLFind(tempQuery) == '1':
            return 1
        else:
            return 0
    
    def check_urlUniv(self, tempLinkUniv):
        tempQuery = 'SELECT EXISTS(SELECT * FROM indexUniv WHERE link LIKE "%'+tempLinkUniv+'%")'
        if self.exe_SQLFind(tempQuery) == '1':
            return 1
        else:
            return 0
    
    def isExistsUrl(self, tempUniv):
        tempQuery = 'SELECT EXISTS(SELECT * FROM indexUniv WHERE univ LIKE "%'+tempUniv+'%" AND (link NOT LIKE "%link not exists%" AND link IS NOT NULL))'
        if self.exe_SQLFind(tempQuery) == '1':
            return 1
        else:
            return 0
        
    def scrapeUniv(self, tempUrl):
        tempList = []
        scrape = BeautifulSoup(urllib.urlopen(tempUrl), "html.parser")
        for record in scrape.findAll('table', {"class": "table1"}):
            count = 1
            for record2 in record.findAll('td'):
                if count % 3 == 0:
                    tempList.append(record2.text)
                count = count+1
        return tempList
    
    def prodi(self, tempCPoint, tempDfProdi):
    	print tempCPoint
    	#sys.exit('System STOPPED')
        tempI = tempCPoint[0]; tempIStart = 1; tempIFinish = 1
        tempJ = tempCPoint[3]; tempJStrat = tempCPoint[4]; tempJFInish = tempCPoint[5]
        tempCurrentTime = tempCPoint[6]
        tempJ = tempCPoint[3]; tempJStart = 0; tempJFinish = 0
        tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
        tempCurrentTime = int(tempCurrentTime)
        print tempCPoint
        #if tempI == 51:
        #	print str(tempI)+" "+str(tempIStart)+" "+str(tempIFinish)
        #	sys.exit('System STOPPED')
        self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
        for link in tempDfProdi['link'][tempJ:]:
            tempJStart = 1; tempJFinish = 0
            tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
            tempCurrentTime = int(tempCurrentTime)
            self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
            if self.check_dbInfoProdi(tempDfProdi['kode'][tempJ], str(tempI+1), str(tempJ+1)) == 0:
                print ">"+link
                kode = str(tempDfProdi['kode'][tempJ])
                prodi = str(tempDfProdi['prodi'][tempJ])
                status = str(tempDfProdi['status'][tempJ])
                jenjang = str(tempDfProdi['jenjang'][tempJ])
                dosenTetap = str(tempDfProdi['dosenTetap'][tempJ])
                mahasiswa = str(tempDfProdi['mahasiswa'][tempJ])
                urlProdi = str(link)
                scrapeProdi = BeautifulSoup(urllib.urlopen(urlProdi), "html.parser")
                listInfoProd = []
                listDosen = []
                listTemp = []
                for record in scrapeProdi.findAll('table', {"class": "table1"}):
                    count = 1
                    for record2 in record.findAll('td'):
                        if count % 3 == 0 and count <= 24:
                            listTemp.append(record2.text)
                        count = count + 1
                    count = 1
                    for item in listTemp:
                        if count == 1 or count == 3 or count == 5 or count == 6:
                            listInfoProd.append(item)
                        count = count + 1
                listTemp = []
                for record in scrapeProdi.findAll('div', {"id": "dosen"}):
                    for record2 in record.findAll('td'):
                        listTemp.append(record2.text)
                for item in listTemp[3::5]:
                    listDosen.append(item)
                S2 = 0; S3 = 0
                for item in listDosen:
                    if item == 'S2':
                        S2 = S2 +1
                    elif item == 'S3':
                        S3 = S3 +1
                dosenS2 = str(S2); dosenS3 = str(S3)
                query = 'INSERT INTO infoProdi (idUniv, sqProdi, kode, status, prodi, jenjang, dosenTetap, dosenS2, dosenS3, mahasiswa, link) VALUES ("'+str(tempI+1)+'","'+str(tempJ+1)+'","'+kode+'","'+status+'","'+prodi+'","'+jenjang+'","'+dosenTetap+'","'+dosenS2+'","'+dosenS3+'","'+mahasiswa+'","'+link+'");'
                self.write_dataSQL(query)
                print '>'+str(tempI+1)+'|'+str(tempJ+1)+'|'+kode+'|'+status+'|'+prodi+'|'+jenjang+'|dosen='+dosenTetap+'|s2='+dosenS2+'|s3='+dosenS3+'|Mhs='+mahasiswa+'|'
                tempJ = tempJ + 1; tempFinishJ = 1
                tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
                tempCurrentTime = int(tempCurrentTime)
                self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
            else:
                tempJ = tempJ+1; tempJStart = 1; tempJFinish = 1
                tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
                tempCurrentTime = int(tempCurrentTime)
                self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
                #tempCPoint = self.read_checkPoint()
                print "Prodi already exists"
                #tempDfProdi = pandas.read_csv('dfProdi.csv')
                #self.prodi(tempCPoint, tempDfProdi)
        tempJ = 0; tempStartJ = 0; tempFinishJ = 0
        tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
        tempCurrentTime = int(tempCurrentTime)
        self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
        return True
        
    def univ(self, tempCPoint, tempDfUniv):
        tempI = tempCPoint[0]; tempIStart = tempCPoint[1]; tempIFinish = tempCPoint[2]
        tempJ = 0; tempJStart = 0; tempJFinish = 0
        tempCurrentTime = tempCPoint[6]
        tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
        tempCurrentTime = int(tempCurrentTime)
        print tempCPoint
        self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
        for university in tempDfUniv['univ'][tempI:]:
            print tempI
            tempCPoint[0] = tempI
            print tempCPoint
            self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
            print university
            urlUniv = ''
            if self.isExistsUrl(university) == 0:
                print "> There was no link before"
                urlUniv = self.isForlapExist(self.urlForlapInGoogle(university))
            else:
                print "> Link for this University is Already Exists"
                urlUniv = tempDfUniv['link'][tempI]
            print "URL : "+ str(urlUniv)
            if (urlUniv <> False) and (urlUniv <> "link not exists") and (urlUniv <> None) :
                tempStartI = 1
                tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
                tempCurrentTime = int(tempCurrentTime)
                self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
                if self.check_urlUniv(urlUniv) == 0:
                    tempQuery = 'UPDATE indexUniv SET link = "'+urlUniv+'" WHERE Id = '+str(tempI+1)+' ;'
                    self.write_dataSQL(tempQuery)
                else:
                    print '> Data Already Exists'
                listIdUniv = []
                scrapeUniv = BeautifulSoup(urllib.urlopen(urlUniv), "html.parser")
                for record in scrapeUniv.findAll('table', {"class": "table1"}):
                    count = 1
                    for record2 in record.findAll('td'):
                        if count % 3 == 0:
                            listIdUniv.append(record2.text)
                        count = count +1
                status = str(listIdUniv[0]); status = re.sub("[!@#$/']", '', status)
                universitas = str(listIdUniv[1]); universitas = re.sub("[!@#$/']", '', universitas)
                berdiri = str(listIdUniv[2]); berdiri = re.sub("[!@#$/']", '', berdiri)
                noSK = str(listIdUniv[3]); noSK = re.sub("[!@#$/']", '', noSK)
                tanggalSK = str(listIdUniv[4]); tanggalSK = re.sub("[!@#$/']", '', tanggalSK)
                alamat = str(listIdUniv[5]); alamat = re.sub("[!@#$/']", '', alamat)
                kotakab = str(listIdUniv[6]); kotakab = re.sub("[!@#$/']", '', kotakab)
                kodePos = str(listIdUniv[7]); kodePos = re.sub("[!@#$/']", '', kodePos)
                telepon = str(listIdUniv[8]); telepon = re.sub("[!@#$/']", '', telepon)
                fax = str(listIdUniv[9]); fax = re.sub("[!@#$/']", '', fax)
                email = str(listIdUniv[10]); email = re.sub("[!#$/']", '', email)
                website = str(listIdUniv[11]); website = re.sub("[!@#$/']", '', website)
                if universitas + ' ' == university:
                    print university + "> valid as webpage"
                    tempQuery = 'UPDATE indexUniv SET valid = "Y" WHERE Id = '+str(tempI+1)+' ;'
                    self.write_dataSQL(tempQuery)
                    if self.check_dbInfoUniv(tempI+1) == 0:
                        tempQuery = 'INSERT INTO infoUniv (idUniv, status, universitas, berdiri, noSK, tanggalSK, alamat, kotakab, kodePos, telepon, fax, email, website) VALUES ("'+str(tempI+1)+'","'+status+'","'+universitas+'","'+berdiri+'","'+noSK+'","'+tanggalSK+'","'+alamat+'","'+kotakab+'","'+kodePos+'","'+telepon+'","'+fax+'","'+email+'","'+website+'");'
                        self.write_dataSQL(tempQuery)
                        print '>'+str(tempI+1)+'|'+status+'|'+universitas+'|'+berdiri+'|'+noSK+'|'+tanggalSK+'|'+alamat+'|'+kotakab+'|'+kodePos+'|'+telepon+'|'+fax+'|'+email+'|'+website
                    else :
                        print '> Data Already Exists'
                    #====#
                    tempListProdi = []
                    tempListLinkProdi = []
                    tempDataProdi = {
                        'kode' : [],
                        'prodi' : [],
                        'status' : [],
                        'jenjang' : [],
                        'dosenTetap' : [],
                        'mahasiswa' : [],
                        'link' : [],
                    }
                    for record in scrapeUniv.findAll('table', {"class": "table table-bordered"}):
                        count = 1
                        for record2 in record.findAll('td'):
                            #print record2.text
                            if count % 8 <> 1 and count % 8 <> 0:
                                #print record2.text
                                tempText = ""
                                tempData = record2.text.split()
                                for item in tempData:
                                    tempText = tempText + item + " "
                                tempListProdi.append(tempText)
                            count = count+1
                        for record2 in record.findAll('a', href = True):
                            #print record2['href']
                            tempListLinkProdi.append(record2['href'])
                    count = 1
                    for item in tempListProdi :
                        if count % 6 == 1:
                            tempDataProdi['kode'].append(str(item))
                        elif count % 6 == 2 :
                            tempDataProdi['prodi'].append(str(item))
                        elif count % 6 == 3 :
                            tempDataProdi['status'].append(str(item))
                        elif count % 6 == 4 :
                            tempDataProdi['jenjang'].append(str(item))
                        elif count % 6 == 5 :
                            tempDataProdi['dosenTetap'].append(str(item))
                        elif count % 6 == 0 :
                            tempDataProdi['mahasiswa'].append(str(item))
                            tempDataProdi['link'].append(str(tempListLinkProdi[(count/6)-1]))
                        count = count+1
                    tempDfProdi = pandas.DataFrame(tempDataProdi, columns=['kode', 'prodi', 'status', 'jenjang', 'dosenTetap', 'mahasiswa', 'link'])
                    tempDfProdi.to_csv('dfProdi.csv')
                    tempDfProdi = pandas.read_csv('dfProdi.csv')
                    tempIFinish = 1
                    tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
                    tempCurrentTime = int(tempCurrentTime)
                    print "> Finding Information of University Done"
                    #tempI = tempI + 1
                    self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
                    print tempCPoint
                    #sys.exit('System STOPPED')
                    self.prodi(tempCPoint, tempDfProdi)
                else:
                    print university + "> has no valid data"
                    tempQuery = 'UPDATE indexUniv SET valid = "N" WHERE Id = '+str(tempI+1)+' ;'
                    self.write_dataSQL(tempQuery)
                    tempIStart = 1; tempIFinish = 1
                    tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
                    tempCurrentTime = int(tempCurrentTime)
                    print "> Finding Information of University Done"
                    #tempI = tempI + 1
                    self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
            else :
                print university + "> has no link in google"
                tempQuery = 'UPDATE indexUniv SET link =  "link not exists" WHERE Id ='+str(tempI+1)+' ;'
                self.write_dataSQL(tempQuery)
                print university + "> has no valid data"
                tempQuery = 'UPDATE indexUniv SET valid = "N" WHERE Id = '+str(tempI+1)+' ;'
                self.write_dataSQL(tempQuery)
                tempIStart = 1; tempIFinish = 1
                tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
                tempCurrentTime = int(tempCurrentTime)
                print "> Finding Information of University Done"
                #tempI = tempI + 1
                self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
            tempI = tempI + 1
            tempIStart = 1; tempIFinish = 1
            tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
            tempCurrentTime = int(tempCurrentTime)
            self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
        tempIStart = 1; tempIFinish = 1
        tempCurrentTime = tempCurrentTime + (time.time() - self.startTime)
        tempCurrentTime = int(tempCurrentTime)
        self.write_checkPoint(tempI, tempIStart, tempIFinish, tempJ, tempJStart, tempJFinish, tempCurrentTime) #saving
        return True

getInfo = info('getInfo') # make an object for class info
getInfo.exe_SQL('USE diktiScraperdb')
dfUniv = getInfo.exe_SQLToDf('SELECT Id, univ, link FROM indexUniv')
cPoint = getInfo.read_checkPoint() #reading saving
pointUniv = cPoint[0]; pointStartUniv = cPoint[1]; pointFinishUniv = cPoint[2]; pointProdi = cPoint[3]; pointStartProdi = cPoint[4]; pointFinishProdi = cPoint[5]
currentTime = cPoint[6]
if pointUniv == 0:
    print "> Finding From Beginning"
    query = "DROP TABLE IF EXISTS infoUniv;"; getInfo.exe_SQL(query)
    query = "CREATE TABLE infoUniv (Id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, idUniv VARCHAR(255), status VARCHAR(255), universitas VARCHAR(255), berdiri VARCHAR(255), noSK VARCHAR(255), tanggalSK VARCHAR(255), alamat VARCHAR(255), kotakab VARCHAR(255), kodePos VARCHAR(255), telepon VARCHAR(255), fax VARCHAR(255), email VARCHAR(255), website VARCHAR(255) );"; getInfo.exe_SQL(query); print "> tabel infoUniv created"
    query = "DROP TABLE IF EXISTS infoProdi;"; getInfo.exe_SQL(query)
    query = "CREATE TABLE infoProdi (Id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, idUniv VARCHAR(255), sqProdi VARCHAR(255), kode VARCHAR(255), status VARCHAR(255), prodi VARCHAR(255), jenjang VARCHAR(255), dosenTetap VARCHAR(255), dosenS2 VARCHAR(255), dosenS3 VARCHAR(255), mahasiswa VARCHAR(255), akreditasi CHAR(1), link VARCHAR(255) );"; getInfo.exe_SQL(query); print "> tabel infoProdi CREATED"
    getInfo.univ(cPoint, dfUniv)
else:
    print "> Finding Information of University start from %s" % str(pointUniv)
    query = "CREATE TABLE IF NOT EXISTS infoUniv (Id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, idUniv VARCHAR(255), status VARCHAR(255), universitas VARCHAR(255), berdiri VARCHAR(255), noSK VARCHAR(255), tanggalSK VARCHAR(255), alamat VARCHAR(255), kotakab VARCHAR(255), kodePos VARCHAR(255), telepon VARCHAR(255), fax VARCHAR(255), email VARCHAR(255), website VARCHAR(255) );"; getInfo.exe_SQL(query); print "> tabel infoUniv created"
    query = "CREATE TABLE IF NOT EXISTS infoProdi (Id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, idUniv VARCHAR(255), sqProdi VARCHAR(255), kode VARCHAR(255), status VARCHAR(255), prodi VARCHAR(255), jenjang VARCHAR(255), dosenTetap VARCHAR(255), dosenS2 VARCHAR(255), dosenS3 VARCHAR(255), mahasiswa VARCHAR(255), akreditasi CHAR(1), link VARCHAR(255) );"; getInfo.exe_SQL(query); print "> tabel infoProdi CREATED"
    if pointStartUniv == 0 or (pointStartUniv == 1 and pointFinishUniv == 0):
        getInfo.univ(cPoint, dfUniv)
    else:
        print "> Finding Information of University Done"
        dfProdi = pandas.read_csv('dfProdi.csv')
        if pointProdi == 0:
            print "> Finding Information of Prodi from Beginning"
        else:
            print "> Finding Information of Prodi Start From %s" % str(pointProdi)
        getInfo.prodi(cPoint, dfProdi)
        cPoint = getInfo.read_checkPoint() #reading saving
        cPoint[0] = cPoint[0]+1; cPoint[1] = 0; cPoint[2] = 0; cPoint[3] = 0; cPoint[4] = 0; cPoint[5] = 0;
        print cPoint
        getInfo.write_checkPoint(cPoint[0], cPoint[1], cPoint[2], cPoint[3], cPoint[4], cPoint[5], cPoint[6]) #saving
        getInfo.univ(cPoint, dfUniv)