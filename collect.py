import pandas as pd
import csv
import io
import requests
import json

from pathlib import Path
import os.path
import io
#import requests
import glob
import hashlib
import random

#import datetime
import time
from dateutil import parser
from datetime import date, timedelta, datetime, timezone

# pip3 install deep-translator
from deep_translator import GoogleTranslator
from difflib import SequenceMatcher

DATA_PATH = Path.cwd()
ts = int(time.time())

githubRepos = {'MarcoL95-news/earthquakeIsComing': {'Extreme':'Earthquake', 'extremeColor':'#357d49'}, 
               'sonjanews/DroughtIsComing': {'Extreme':'Drought', 'extremeColor':'#357d49'},
              }
currentMonths = []
for m in [0,20,40,60]:
  month = datetime.utcfromtimestamp(ts-60*60*24*m).strftime('%Y_%m')
  if month not in currentMonths:
    currentMonths.append(month)

collectedNews = {}

def loadExistingNews(currentMonth):
    global collectedNews
    fileDate = 'news_'+currentMonth+'.csv'
    if(not fileDate in collectedNews):
        if(os.path.isfile(DATA_PATH / 'csv' / fileDate)):
            df = pd.read_csv(DATA_PATH / 'csv' / fileDate, delimiter=',',index_col='index')
            collectedNews[fileDate] = df.to_dict('index')
        else:
            collectedNews[fileDate] = {}
    return collectedNews[fileDate]

def addNewsToCollection(data):
    global collectedNews
    pubDate = parser.parse(data['published'])
    fileDate = 'news_'+pubDate.strftime('%Y_%m')+'.csv'
    if(fileDate in collectedNews):
      if(not data['hash'] in collectedNews[fileDate]):
        #if(not 'archive' in data):
        #   data = archiveUrl(data)
        collectedNews[fileDate][data['hash']] = data
        return True
    return False

def storeCollection():
    global collectedNews
    #cols = ['published','keyword','domain','language','valid','title','description','url','image','archive','content','quote']
    cols = ['published','extreme','topic','domain','language','valid','title','description','url','image','archive','content','en','de']
    for dateFile in collectedNews:
        df = pd.DataFrame.from_dict(collectedNews[dateFile], orient='index', columns=cols)
        df.index = df['url'].apply( lambda x: hashlib.sha256(x.encode()).hexdigest()[:32])   
        df = removeDuplicates(df)
        #df.to_csv(DATA_PATH / dateFile, index=True) 
        if(not os.path.exists(DATA_PATH / 'csv')):
            os.mkdir(DATA_PATH / 'csv')
        df.to_csv(DATA_PATH / 'csv' / dateFile, index_label='index') 
    collectedNews = {}

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def removeDuplicates(df1):
    df1['md5'] = ''
    df1['group'] = ''
    df1['similarity'] = 0.0
    df1 = df1.sort_values(by=['published'], ascending=True)

    for index, column in df1.iterrows():
        quote = str(column['domain']) + ' ' + str(column['title']) + ' ' + str(column['description'])
        md5 = hashlib.md5(quote.encode('utf-8')).hexdigest()
        df1.loc[index,'md5'] = md5
        day = '1970-01-01'
        if(len(str(column['published']))>5):
          pubDate = parser.parse(column['published'])
          day = pubDate.strftime('%Y-%m-%d')
         
        groupTxt = str(column['domain']) +  ' ' + day
        group = hashlib.md5(groupTxt.encode('utf-8')).hexdigest()  
        df1.loc[index,'group'] = group

    df1 = df1[~df1.md5.duplicated(keep='first')]  

    for index1, column1 in df1.iterrows():
        quote1 = str(column1['title']) + ' ' + str(column1['description']) 
        df2 = df1[df1['group']==column1['group']]
        for index2, column2 in df2.iterrows():
            if(column1['md5']>column2['md5']):
                quote2 = str(column2['title']) + ' ' + str(column2['description'])
                similarity = similar(quote1,quote2)
                if(similarity > df1.loc[index1,'similarity']):
                    df1.loc[index1,'similarity'] = similarity

    df3 = df1[df1['similarity']<0.8]
    df3 = df3.drop(columns=['md5', 'group', 'similarity'])
    df3 = df3.sort_values(by=['published'], ascending=True)
    return df3

def archiveUrl(data):
    timetravelDate = '19700101'
    pubDate = None
    try:
        pubDate = parser.parse(data['published'])
    except:
        print('date parse error 1')
    if(not pubDate):
      try:
        pubDate = parser.isoparse(data['published'])
      except:
        print('date parse error 2')   
    if(pubDate):
        timetravelDate = pubDate.strftime('%Y%m%d')
    #timetravelUrl = 'http://timetravel.mementoweb.org/api/json/'+timetravelDate+'/'+data['url']  #NO LONGER WORKING
    timetravelUrl = 'http://archive.org/wayback/available?url='+data['url']+'&timestamp='+timetravelDate
    try:
        print(["try request", timetravelUrl])
        page = requests.get(timetravelUrl, timeout=60)
        if page.status_code == 200:
            content = page.content
            print(content)
            if(content):
                #print(content)
                jsonData = json.loads(content)
                if(jsonData and ('archived_snapshots' in jsonData)):
                  snapshots = jsonData['archived_snapshots']
                  if('closest' in snapshots):
                    closest = snapshots['closest']
                    if('200'==closest['status']):
                      data['archive'] = closest['url']
                      if('1970-01-01T00:00:00' == data['published']):
                        ts = closest['timestamp']
                        tsNew = ts[0:4]+'-'+ts[4:6]+'-'+ts[6:8]+'T'+ts[8:10]+':'+ts[10:12]+':'+ts[12:14]
                        print(['new ts',ts,tsNew])
                        data['published'] = tsNew
                #'closest'
    except:
        print("not archived yet")
    return data 


def translateData(data):
   if('de'==data['language']):
       data['de'] = str(data['title']) + ' ' + str(data['description'])
       data['en'] = GoogleTranslator(source='de', target='en').translate(text=data['de'])
   if('en'==data['language']):
       data['en'] = str(data['title']) + ' ' + str(data['description'])
       data['de'] = GoogleTranslator(source='en', target='de').translate(text=data['en'])
   return(data) 

#allExtremesDf = pd.DataFrame(None)

repos =  list(githubRepos.keys())     
random.shuffle(repos)
for repo in repos:
  #load keywords...
  keysFile = "https://github.com/"+repo+"/blob/main/keywords.csv?raw=true"
  keyRequest = requests.get(keysFile, headers={'Accept': 'text/plain'})
  if(keyRequest.status_code == 200):
    keysDf=pd.read_csv(io.StringIO(keyRequest.content.decode('utf-8')), delimiter=',')
    keysDf = keysDf.drop(columns = ['language', 'limitPages', 'ratioNew', 'keywordColor'])
    for currMonth in currentMonths:
    #load existing ones
      existingName = "news_"+currMonth+".csv" 
      existingDict = loadExistingNews(currMonth)
      newsFile = "https://github.com/"+repo+"/blob/main/csv/news_"+currMonth+".csv?raw=true"
      print(newsFile)
      newsRequest = requests.get(newsFile, headers={'Accept': 'text/plain'})
      if(newsRequest.status_code == 200):
        newsDf=pd.read_csv(io.StringIO(newsRequest.content.decode('utf-8')), delimiter=',', index_col='index')
        newsDf['extreme'] = githubRepos[repo]['Extreme']
        newsDf['extremeColor'] = githubRepos[repo]['extremeColor']
        newsDf['hash'] = newsDf.index 
        newsDf = pd.merge(newsDf, keysDf, how='left', left_on=['keyword'], right_on=['keyword'])
        newsDf = newsDf.dropna(subset=['topic'])
        ## newsDf = newsDf.drop(newsDf[newsDf.valid < 0.5].index)
        if(existingDict and not newsDf.empty):
            newsDf.index = newsDf['hash']  #!!
            for ha in list(existingDict.keys()):
               newsDf = newsDf.drop(index=ha, errors='ignore')
        #if(allExtremesDf.empty):
        #    allExtremesDf = newsDf
        #else:
        #    allExtremesDf = pd.concat([allExtremesDf, newsDf])
        for idx, column in newsDf.iterrows():
          #print(column)
          data = archiveUrl(column) 
          data = translateData(data)
          addNewsToCollection(data)

storeCollection()


















