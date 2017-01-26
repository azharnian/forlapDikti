from bs4 import BeautifulSoup
from socket import error as SocketError
import urllib
import pandas
import MySQLdb
import re
import time, random

start_time = time.time()
db = MySQLdb.connect(
    host = '127.0.0.1',
    user = 'root',
    passwd = '310116',
    db = 'diktiScraperdb'
)
class scrape(object): #membuat objek scrape

    def __init__(self, nameOfMachine): #inisiasi objek
        self.nameOfMachine = nameOfMachine #pemberian atribut objek pada inisiasi

    def getLink(self, addr): #membuat fungsi getLink
        dataLink = {  #membuat dictionary
        'addt' : [], #teks untuk penambahan
        'link' : [] #link yang ada di halaman
        }
        def scrapePage(addr):
            scrapeData = BeautifulSoup(urllib.urlopen(addr), "html.parser")
            return scrapeData
        data = scrapePage(addr)
        try :
            for recordRow in data.findAll('tr'):
                addtText = ""
                for recordLink in recordRow.findAll('a', href=True):
                    tempData = recordLink.text.split()
                    for listOfTempData in tempData:
                        addtText = addtText + listOfTempData + " "
                    dataLink['addt'].append(str(addtText))
                    dataLink['link'].append(str(recordLink['href']))
            print "Get DataLink Scrape HTML Object"        
            return dataLink
        except AttributeError :
            print "Not Found"
            return dataLink
    def getTextOnly(self,addr):
        dataText = {
            'code' : [],
            'univ' : []
        }
        listCode = []
        listUniv = []
        def scrapePage(addr):
            wt = random.uniform(2, 5)
            time.sleep(wt)
            scrapeData = BeautifulSoup(urllib.urlopen(addr), "html.parser")
            return scrapeData
        data = scrapePage(addr)
        try:
            for recordRow in data.findAll('tr'):
                for recordText in recordRow.findAll('td'):
                    listCode.append(recordText.text)
                    listUniv.append(recordText.text)
            for item in listCode[1::19]:
                addtText = ""
                tempData = item.split()
                for listOfTempData in tempData:
                    addtText = addtText + listOfTempData + " "
                dataText['code'].append(str(addtText))
                #print addtText
            for item in listUniv[2::19]:
                addtText = ""
                tempData = item.split()
                for listOfTempData in tempData:
                    addtText = addtText + listOfTempData + " "
                dataText['univ'].append(str(addtText))
                #print addtText 
            print "Get Text ScrapeHTML Object"
            #print dataText
            return dataText
        except SocketError as e:
            if e.errno != errno.ECONNRESET:
                raise # Not error we are looking for
            pass # Handle error here.

urlServer = "/home/alien/diktiScraper" #server program host
urlTarget = "http://forlap.dikti.go.id/perguruantinggi/homerekap" #home rekap
scraperLayer0 = scrape('layer0_df')
dataGet = scraperLayer0.getLink(urlTarget)
layer0_df = pandas.DataFrame(dataGet, columns=['addt', 'link'])
layer0_df.index.name = 'institutionId'
cur = db.cursor()
query0 = 'USE diktiScraperdb;'
cur.execute(query0)
indexUniv = "indexUniv"
query7 = "DROP TABLE IF EXISTS  "+indexUniv+";"
query8 = "CREATE TABLE "+indexUniv+" (Id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,code VARCHAR(255),inst VARCHAR(255), univ VARCHAR(255), link VARCHAR(255), valid CHAR(1));"
cur.execute(query7)
cur.execute(query8)
l = 0
i = 0
for link in layer0_df['link']:
    urlTarget0 = link
    print str(i) + " " + layer0_df['addt'][i]
    if i == 0 or i > 2 :
        scraper1 = scrape('df1')
        dataGet1 = scraper1.getTextOnly(urlTarget0)
        df1 = pandas.DataFrame(dataGet1, columns=['code', 'univ'])
        df1.index.name = 'institutionId'
        j = 0
        for univ in df1['univ']:
            a = df1['code'][j]
            a = re.sub("[!@#$/']", '', a)
            b = layer0_df['addt'][i] #inst
            b = re.sub("[!@#$/']", '', b)
            c = univ #univ
            c = re.sub("[!@#$/']", '', c)
            print " > " + a + " | " + b + " | " + c
            query11 = "INSERT INTO indexUniv (code, inst, univ) VALUES ('"+a+"','"+b+"','"+c+"');"
            cur.execute(query11)
            db.commit()
            j = j + 1
    elif i == 2:
        #
        scraper1 = scrape('df1')
        dataGet1 = scraper1.getLink(urlTarget0)
        df1 = pandas.DataFrame(dataGet1, columns=['addt', 'link'])
        df1.index.name = 'institutionId'
        j = 0
        for link1 in  df1['link']:
            urlTarget1 = link1
            scraper2 = scrape('df2')
            dataGet2 = scraper2.getLink(urlTarget1)
            df2 = pandas.DataFrame(dataGet2, columns=['addt', 'link'])
            df2.index.name = 'institutionId'
            k = 0
            for link2 in df2['link']:
                urlTarget2 = link2
                scraper3 = scrape('df3')
                dataGet3 = scraper3.getTextOnly(urlTarget2)
                df3 = pandas.DataFrame(dataGet3, columns=['code', 'univ'])
                df3.index.name = 'institutionId'
                l = 0
                for univ in df3['univ']:
                    a = df3['code'][l] #code
                    a = re.sub("[!@#$/']", '', a)
                    b = layer0_df['addt'][i] #inst
                    b = re.sub("[!@#$/']", '', b)
                    c = df1['addt'][j] #inst
                    c = re.sub("[!@#$/']", '', c)
                    d = df2['addt'][k] #inst
                    d = re.sub("[!@#$/']", '', d)
                    d = b + " " + c + " " + d
                    e = univ
                    e = re.sub("[!@#$/']", '', e)
                    print " > " + a + " | " + d + " | " + e
                    query11 = "INSERT INTO indexUniv (code, inst, univ) VALUES ('"+a+"','"+d+"','"+e+"');"
                    cur.execute(query11)
                    db.commit()
                    l = l+1
                k = k+1
            j = j+1
    elif i == 1 :
        scraper1 = scrape('df1')
        dataGet1 = scraper1.getLink(urlTarget0)
        df1 = pandas.DataFrame(dataGet1, columns=['addt', 'link'])
        df1.index.name = 'institutionId'
        j = 0
        for link1 in  df1['link']:
            urlTarget1 = link1
            scraper2 = scrape('df2')
            dataGet2 = scraper2.getTextOnly(urlTarget1)
            df2 = pandas.DataFrame(dataGet2, columns=['code', 'univ'])
            df2.index.name = 'institutionId'
            k = 0
            for univ in df2['univ']:
                a = df2['code'][k] #code
                a = re.sub("[!@#$/']", '', a)
                b = layer0_df['addt'][i] #inst
                b = re.sub("[!@#$/']", '', b)
                c = df1['addt'][j] #inst
                c = re.sub("[!@#$/']", '', c)
                c = b + " " + c
                d = univ #univ
                d = re.sub("[!@#$/']", '', d)
                print " > " + a + " | " + c + " | " + d
                query11 = "INSERT INTO indexUniv (code, inst, univ) VALUES ('"+a+"','"+c+"','"+d+"');"
                cur.execute(query11)
                db.commit()
                k = k+1
            j = j+1
    i = i+1
print("--- %s seconds ---" % (time.time() - start_time))