import pandas as pd

from pathlib import Path
import os.path
import io
import glob

import requests
from PIL import Image
from io import BytesIO
import PIL


DATA_PATH = Path.cwd()
if(not os.path.exists(DATA_PATH / 'csv')):
    os.mkdir(DATA_PATH / 'csv')
if(not os.path.exists(DATA_PATH / 'pictures')):
    os.mkdir(DATA_PATH / 'pictures')

def getNewsFiles():
    fileName = './csv/news_????_??.csv'
    files = glob.glob(fileName)
    return files  

def getNewsDFbyList(files):    
    newsDF = pd.DataFrame(None)
    for file in files:
        df = pd.read_csv(file, delimiter=',')
        if(newsDF.empty):
            newsDF = df
        else:
            newsDF = pd.concat([newsDF, df])
    newsDF = newsDF.sort_values(by=['published'], ascending=True)        
    return newsDF 

def getNewsDF():
    files = getNewsFiles()
    newsDF = getNewsDFbyList(files)
    return newsDF  

newsDf = getNewsDF()
newsDf = newsDf[newsDf['image'].notna()]
newsDf = newsDf[newsDf['valid'] < 0.5]
#print(newsDf) 

newsDf = newsDf.sample(n=len(newsDf), axis=0, ignore_index=True).copy()
newsDf = newsDf.reset_index()

from PIL import features
print (features.check_module('webp'))

counting = 500 
for index, column in newsDf.iterrows():
 if(counting>0):
  #index,extreme,image
  print(column)
  if(not os.path.exists(DATA_PATH / 'pictures' / 'invalid')):
    os.mkdir(DATA_PATH / 'pictures' / 'invalid')
  if(column['image'] and 'http' == column['image'][0:4]):
    imgPath = DATA_PATH / 'pictures' / 'invalid' / (column['index'] + '.jpg')
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
