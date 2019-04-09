from client import DiffbotClient,DiffbotCrawl
from config import API_TOKEN
import pprint
import time
import pandas as pd
import numpy as np
from pymongo import MongoClient
from pandas.io.json import json_normalize
import requests
import re
import json
import validators
from datetime import datetime
import os
from time import sleep
import urllib2
token = API_TOKEN
api = "analyze"
DIR = '/home/kpadmana/taskhuman_topic_modelling/data'
client1 = MongoClient('18.191.7.17:27017', \
    username='taskhuman',\
    password='T@skHuman!',\
    authSource='TaskHuman',\
    authMechanism='SCRAM-SHA-1')
client2 = MongoClient('138.201.11.139:27017', \
	username='taskhuman',\
	password='T@$kHum@n!')
regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
db1 = client1.TaskHuman
db2 = client2.taskhuman_providerdata
coll1 = db1['userwebsites']
coll2 = db2['providerdata']
coll3 = db1['users']
exclude_userid =[182, 25, 24, 984, 30, 109,1064, 975, 230, 26, 197, 227, 983, 1040, 1042, 1043, 1045, \
                217, 161, 187, 977, 126, 1188, 1202, 1412, 1185,1085, 1524, 1043, 1045, 1188, 1202, 1412, 1908, 987]
cursor1 = coll1.find({}, {'userId': 1, 'website':1})
cursor2 = coll3.find({}, {'userId': 1, 'users.firstName': 1, 'users.lastName': 1})
cursor3 = coll2.find({}, {'providerId': 1})
df1 = json_normalize(list(cursor1))
df1 = df1[~df1['userId'].isin(exclude_userid)]
df1.reset_index(drop=True, inplace=True)
df2 = json_normalize(list(cursor2))
df3 = json_normalize(list(cursor3))['providerId']
L = list(set(df3.tolist()))
df1 = df1[~df1['userId'].isin(L)]
df1.reset_index(drop=True, inplace=True)
for _, row in df1.T.items():
	pid = row['userId']
	pname = df2[df2['userId']==pid]['users.firstName'] + ' ' + df2[df2['userId']==pid]['users.lastName'] 
	psite = row['website']
	psite = psite.rstrip()
	fs = os.listdir(DIR)
	pidlist = [item for item in fs if str(pid) in item]
	if len(pidlist) > 0:
		i = len(pidlist) + 1
		fname = str(pid) + '_' + str(i) + '_'  + 'analyze.json'
	fname = str(pid) + '_' + 'analyze.json'
	if re.match(regex, psite) is None:
		if not ('com' in psite)|('net' in psite)|('fit' in psite)|('about' in psite)|('org' in psite)|('edu' in psite)|('scholar' in psite):
			psite = psite + '.com'
		url = "http://" + psite
	url = psite
	masked_sites = ['linkedin', 'facebook', 'twitter', 'instagram', 'taskhuman', 'youtube']
	mask = any(substr in url for substr in masked_sites)
	if not mask:
		print(url)
		seeds = url
		name = fname.strip('.json')
		diffbot = DiffbotCrawl(token, name, seeds=seeds, api=api)
		status = True
		while status:
			if diffbot.status()['jobs'] is None:
				status = False
			jstate = diffbot.status()['jobs'][0]['jobStatus']
			print(jstate)
			if (jstate[u'status'] == 6) or (jstate[u'status'] == 9) or (jstate[u'status'] == 10):
				status = False
			sleep(5)
		download_file = diffbot.status()['jobs'][0][u'downloadJson']
		req = urllib2.Request(download_file)
		response = urllib2.urlopen(req)
		output = json.loads(response.read())
		pdf = pd.DataFrame(output)
		print(pdf)
		pdf['providerId'] = pid
		pdf['provider_name'] = pname
		records = json.loads(pdf.T.to_json()).values()
		if len(records) == 1:
			coll2.insert_one(list(records)[0])
		else:
			coll2.insert_many(records)
		diffbot.delete()


