import redis
import requests
import json
import pandas as pd
from pandas import DataFrame as df

# Connecting to redis
client = redis.Redis(host = '127.0.0.1', port = '6379')

# API for Maharashtra Covid19 cases 
JSON_URL = 'https://api.covid19india.org/v5/min/timeseries-MH.min.json'
dist_name  = []


req = requests.get(JSON_URL)


distNames = df(req.json()['MH']['districts'])
distNames = distNames.T

# crreate a list of Districts in Maharashtra
for dis in distNames.index:
    dist_name.append(dis)

# Data cleaning and storing in Redis 
for dist  in dist_name:   
    
    if not 'Unknown' in dist:

        covid = df(req.json()['MH']['districts'][dist])
                     
        for conf, date in zip(covid.dates, covid.index):
            for t in conf.keys():
                if 'total' in t:
                    dist_c = dist + "_confirmed"
                    dist_d = dist + "_deceased"
                    dist_r = dist + "_recovered"
                    if 'confirmed' in (conf['total'].keys()):
                        client.hsetnx(dist_c, date, conf['total']['confirmed'])
                    if not 'confirmed' in (conf['total'].keys()):
                        client.hsetnx(dist_c, date, "0")
                        
                    if 'recovered' in (conf['total'].keys()):
                        client.hsetnx(dist_r, date, conf['total']['recovered'])
                    if not 'recovered' in (conf['total'].keys()):
                        client.hsetnx(dist_r, date, "0")
                        
                    if 'deceased' in (conf['total'].keys()):
                        client.hsetnx(dist_d, date, conf['total']['deceased'])
                    if not 'deceased' in (conf['total'].keys()):
                        client.hsetnx(dist_r, date, "0")
                    
                   



    


