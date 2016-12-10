
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import pickle 

year = datetime.now().year
month = datetime.now().month
day = datetime.now().day

if day < 10:
    date = str(month)+"0"+str(day)+str(year)
else:
    date = str(month)+str(day)+str(year)

with open('get_coordinates.json', 'r') as fp:
    coordinates = json.load(fp)

skip_coordinates = []
all_listing_geo = []
i=1
for url in coordinates[:10]:
    
    try:
        
        r = requests.get(url)
        soup = BeautifulSoup(r.text,"lxml")
        print i
        coordinate = [x for x in soup.find('span',itemprop="geo")]
        doorman = [1 if "Doorman" in x.text else 0 for x in soup.find_all("li",class_="")][0]
        listing_geo = {"coordinates":coordinate,"Doorman":doorman,"list_url":url}
        print listing_geo
        all_listing_geo.append(listing_geo)
        i+=1
        if i==10:
            time.sleep(10)
    except:
        skip_coordinates.append(url)

geo_df = pd.DataFrame(all_listing_geo)
geo_df.to_csv('geo_df_'+date+'.csv')


# with open('geo_coordinates_'+date+'.pkl', 'w') as fp:
#      pickle.dump(all_listing_geo, fp)

with open('geo_coordinates_skipped_'+date+'.json', 'w') as fp:
     json.dump(skip_coordinates, fp)
