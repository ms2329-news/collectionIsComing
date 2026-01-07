import pandas as pd

from pathlib import Path
import os.path
import io
import glob

import requests
from PIL import Image
from PIL import features
from io import BytesIO
import PIL

import hashlib
import time
from datetime import date, timedelta, datetime, timezone


DATA_PATH = Path.cwd()
if(not os.path.exists(DATA_PATH / 'csv')):
    os.mkdir(DATA_PATH / 'csv')
if(not os.path.exists(DATA_PATH / 'pictures')):
    os.mkdir(DATA_PATH / 'pictures')


ts = int(time.time())


repo = "JoPhi24-news/leuteFluesterer"
repo = "JoPhi24-news/peopleWhisperer"


currentMonths = []
for m in range(0,730,20):
  month = datetime.utcfromtimestamp(ts-60*60*24*m).strftime('%Y_%m')
  if month not in currentMonths:
    currentMonths.append(month)

for currMonth in currentMonths:
  existingName = "news_"+currMonth+".csv" 
  newsFile = "https://github.com/"+repo+"/blob/main/csv/news_"+currMonth+".csv?raw=true"
  print(newsFile)
  newsRequest = requests.get(newsFile, headers={'Accept': 'text/plain'})
  if(newsRequest.status_code == 200):
    newsDf=pd.read_csv(io.StringIO(newsRequest.content.decode('utf-8')), delimiter=',', index_col='index')

    newsDf = newsDf.sample(n=len(newsDf), axis=0, ignore_index=True).copy()
    newsDf = newsDf.reset_index()
    print(newsDf)
    ##sdhskjhdkjshdk()
    counting = 10 
    for ind, column in newsDf.iterrows():
      if(counting>0):
          #index,extreme,image
          print(ind) 
          print(column['url'])
          print(column['image'])
          print(column)
          if(not os.path.exists(DATA_PATH / 'pictures' / 'unknown')):
            os.mkdir(DATA_PATH / 'pictures' / 'unknown')
          if(column['image'] and 'http' == str(column['image'])[0:4]):
            imgIndex = hashlib.sha256(column['image'].encode()).hexdigest()[:32]
            print(imgIndex)
            ##sdhskjhdkjshdk()
            imgPath = DATA_PATH / 'pictures' / 'unknown' / (imgIndex + '.jpg')
            if(not os.path.exists(imgPath)):
              try:
                r = requests.get(column['image'], timeout=10)
              except requests.exceptions.Timeout:
                print("Timed out")
              except requests.exceptions.ConnectionError:
                 print("ConnectionError")
              #except urllib3.exceptions.MaxRetryError:
              #  print("Retrys")
              else:
               if r.status_code == 200:
                print(column['image']) 
                try:
                  pilImage = Image.open(BytesIO(r.content))
                except PIL.UnidentifiedImageError:
                  print("Format")  
                else:
                  width, height = pilImage.size
                  minSize = min(width,height)
                  newWidth = int(round(128*width/minSize))
                  newHeight = int(round(128*height/minSize))  
                  pilImage.thumbnail((newWidth, newHeight), Image.LANCZOS)
                  pilImage = pilImage.convert('RGB')
                  pilImage.save(imgPath, "JPEG")
                  counting -= 1  


# https://stackoverflow.com/questions/58378394/open-webp-images-in-gce-deep-learning-vm !!
# crePython & creMedia     


