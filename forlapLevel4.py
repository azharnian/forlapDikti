import pandas
import numpy
import re
import requests
import time
import random
import MySQLdb
import sys
import os

startTime = time.time()

db = MySQLdb.connect(
        host = '127.0.0.1',
        user = 'root', #like root
        passwd = '310116', #password
        db = 'diktiScraperdb', #name of database that you used
    )
cur = db.cursor()

if not os.path.exists("dataCSV"):
	os.makedirs("dataCSV")

#sys.exit()

query = "DROP TABLE IF EXISTS bigTable;"; cur.execute(query); print "> db checked"
query = "CREATE TABLE bigTable (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, institusi VARCHAR(255), kodeUniversitas INT, universitas VARCHAR(255), kodeProdi INT, prodiForlap VARCHAR(255), prodiBANPT VARCHAR(255), akreditasi VARCHAR(255), noSKBANPT VARCHAR(255), tanggalKadaluarsaSKBANPT VARCHAR(255), statusAkreditasi VARCHAR(255), statusUniversitas VARCHAR(255), jenjang VARCHAR(255), dosenTetap INT, dosenS2 INT, dosenS3 INT, jumlahMahasiswa INT, berdiriUniversitas VARCHAR(255), noSKUniversitas VARCHAR(255), tanggalSKUniversitas VARCHAR(255), alamat VARCHAR(255), kota VARCHAR(255), kodePos VARCHAR(255), telepon VARCHAR(255), fax VARCHAR(255), email VARCHAR(255), website VARCHAR(255), linkProdi VARCHAR(255), linkUniversitas VARCHAR(255), latitude DECIMAL(10,7), longitude DECIMAL(10,7) );"; cur.execute(query); print "> table bigTable created"

dfProdi = pandas.read_sql('SELECT * FROM infoProdi',db)
dfUniv = pandas.read_sql('SELECT * FROM indexUniv',db)
dfInfoUniv = pandas.read_sql('SELECT * FROM infoUnivLatLong',db)
dfAkreditasi = pandas.read_sql('SELECT * FROM infoAkreditasiProdi',db)
#print dfProdi
count = 0
for prodi in dfProdi['prodi'][count:]:
	sign = 0
	#print dfProdi[dfProdi['prodi'][prodi]]
	#print prodi
	#print dfProdi['idUniv'][count+1].index.values
	idUniv =  int(dfProdi['idUniv'][count])
	#print idUniv
	indexDfUniv = int(dfUniv[dfUniv['Id'] == idUniv].index.values)
	institusi = dfUniv['inst'][indexDfUniv][:-1]
	universitas = dfUniv['univ'][indexDfUniv][:-1]; universitas = re.sub('[!@#$/"]', '', universitas)
	kodeUniversitas = dfUniv['code'][indexDfUniv]
	if dfUniv['link'][indexDfUniv] == None:
		linkUniversitas = '-'
	else:
		linkUniversitas = dfUniv['link'][indexDfUniv]
	if dfProdi['kode'][count] == '':
		kodeProdi = 0
	else:
		kodeProdi = dfProdi['kode'][count]
	#prodi = 'Budidaya Hutan'
	prodiForlap = dfProdi['prodi'][count][:-1]
	statusUniversitas = dfProdi['status'][count][:-1]
	jenjang = dfProdi['jenjang'][count]
	#print ">"+dfProdi['dosenTetap'][count]
	if dfProdi['dosenTetap'][count] == " ":
		dosenTetap = 0
	else:
		dosenTetap = int(dfProdi['dosenTetap'][count])
	if dfProdi['dosenS2'][count] == " ":
		dosenS2 = 0
	else:
		dosenS2 = int(dfProdi['dosenS2'][count])
	if dfProdi['dosenS3'][count] == " ":
		dosenS3 = 0
	else:
		dosenS3 = int(dfProdi['dosenS3'][count])
	if dfProdi['mahasiswa'][count] == " ":
		jumlahMahasiswa = 0
	else:
		jumlahMahasiswa = int(float(dfProdi['mahasiswa'][count]))
	linkProdi = dfProdi['link'][count]
	indexDfInfoUniv = dfInfoUniv[dfInfoUniv['idUniv'] == idUniv]
	if indexDfInfoUniv.empty:
		berdiriUniversitas = '-'
		noSKUniversitas = '-'
		tanggalSKUniversitas = '-'
		alamat = '-'
		kota = '-'
		kodePos = '-'
		telepon = '-'
		fax = '-'
		email = '-'
		website = '-'
		latitude = -6.4949311
		longitude = 106.8488850
	else:
		indexDfInfoUniv = int(dfInfoUniv[dfInfoUniv['idUniv'] == idUniv].index.values)
		berdiriUniversitas = dfInfoUniv['berdiri'][indexDfInfoUniv]
		noSKUniversitas = dfInfoUniv['noSK'][indexDfInfoUniv]
		tanggalSKUniversitas = dfInfoUniv['tanggalSK'][indexDfInfoUniv]
		alamat = dfInfoUniv['alamat'][indexDfInfoUniv]
		kota = dfInfoUniv['kotakab'][indexDfInfoUniv]
		kodePos = dfInfoUniv['kodePos'][indexDfInfoUniv]
		telepon = dfInfoUniv['telepon'][indexDfInfoUniv]
		fax = dfInfoUniv['fax'][indexDfInfoUniv]
		email = dfInfoUniv['email'][indexDfInfoUniv]
		website = dfInfoUniv['website'][indexDfInfoUniv]
		latitude = float(dfInfoUniv['Lat'][indexDfInfoUniv])
		longitude = float(dfInfoUniv['Long'][indexDfInfoUniv])
	#indexDfInfoUniv = int(dfInfoUniv[dfInfoUniv['idUniv'] == idUniv].index.values)
	indexDfAkreditasi = dfAkreditasi[(dfAkreditasi['perguruanTinggi'] == universitas)&(dfAkreditasi['prodi'].str.contains(prodi))&(dfAkreditasi['strata'] == jenjang)]
	jenjang = jenjang[:-1]
	if indexDfAkreditasi.empty:
		sign = 0
		prodiBANPT = '-'
		noSKBANPT = '-'
		tahunSKBANPT = '-'
		akreditasi = '-'
		tanggalKadaluarsa = '-'
		statusAkreditasi = '-'
	else:
		sign = 1
		#print indexDfAkreditasi
		#sys.exit()
		#indexDfAkreditasi = int(indexDfAkreditasi.index.values)
		#print indexDfAkreditasi
		if len(indexDfAkreditasi) > 1:
			indexDfAkreditasi = int(indexDfAkreditasi.index.values[-1])
		elif len(indexDfAkreditasi) == 1:
			indexDfAkreditasi = int(indexDfAkreditasi.index.values[0])
		else:
			print len(indexDfAkreditasi)
			sys.exit()
		prodiBANPT = dfAkreditasi['prodi'][indexDfAkreditasi][:-1]
		noSKBANPT = dfAkreditasi['noSK'][indexDfAkreditasi]
		tahunSKBANPT = dfAkreditasi['tahunSK'][indexDfAkreditasi]
		akreditasi = dfAkreditasi['peringkat'][indexDfAkreditasi][:-1]
		tanggalKadaluarsa = dfAkreditasi['tanggalKadaluarsa'][indexDfAkreditasi]
		statusAkreditasi = dfAkreditasi['status'][indexDfAkreditasi]

	pathCSV = 'dataCSV/'+institusi+'.txt'
	if not os.path.isfile(pathCSV):
		file(pathCSV, 'w').close()
		#sys.exit()
	print count
	print idUniv; 
	print institusi
	print kodeUniversitas
	print universitas
	print kodeProdi
	print prodiForlap
	print prodiBANPT
	print akreditasi
	print noSKBANPT
	print tanggalKadaluarsa
	print statusAkreditasi
	print statusUniversitas
	print jenjang
	print dosenTetap
	print dosenS2
	print dosenS3
	print jumlahMahasiswa
	print berdiriUniversitas
	print noSKUniversitas
	print tanggalSKUniversitas
	print alamat
	print kota
	print kodePos
	print telepon
	print fax
	print email
	print website
	print linkUniversitas
	print linkProdi
	print latitude
	print longitude
	print "========================================================="
	#sys.exit()
	query = 'INSERT INTO bigTable (institusi, kodeUniversitas, universitas, kodeProdi, prodiForlap, prodiBANPT, akreditasi, noSKBANPT, tanggalKadaluarsaSKBANPT, statusAkreditasi, statusUniversitas, jenjang, dosenTetap, dosenS2, dosenS3, jumlahMahasiswa, berdiriUniversitas, noSKUniversitas, tanggalSKUniversitas, alamat, kota, kodePos, telepon, fax, email, website, linkUniversitas, linkProdi, latitude, longitude) VALUES ("'+institusi+'", "'+str(kodeUniversitas)+'", "'+universitas+'", "'+str(kodeProdi)+'", "'+prodiForlap+'", "'+prodiBANPT+'", "'+akreditasi+'", "'+noSKBANPT+'", "'+tanggalKadaluarsa+'","'+statusAkreditasi+'", "'+statusUniversitas+'", "'+jenjang+'", "'+str(dosenTetap)+'", "'+str(dosenS2)+'", "'+str(dosenS3)+'", "'+str(jumlahMahasiswa)+'", "'+berdiriUniversitas+'", "'+noSKUniversitas+'", "'+tanggalSKUniversitas+'", "'+alamat+'", "'+kota+'", "'+kodePos+'", "'+telepon+'", "'+fax+'", "'+email+'",  "'+website+'", "'+linkUniversitas+'", "'+linkProdi+'", "'+str(latitude)+'", "'+str(longitude)+'");'
	cur.execute(query)
	db.commit()
	count = count +1
	#sys.exit()