#with open('dataAkreditasiInstitusi.txt') as fin, open('dataAkreditasiInsitusiNew.csv', 'w') as fout:
#    for line in fin:
#        fout.write(line.replace('\t', ';'))

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