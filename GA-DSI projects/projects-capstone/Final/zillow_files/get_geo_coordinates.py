
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
for url in coordinates:
    
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

latitude_list = []
longitude_list = []
coord_list = []
for x in geo_df['coordinates']:
    latitude =  str(x).split('itemprop="latitude"/>,')[0].split("\"")[1]
    longitude =  str(x).split('itemprop="latitude"/>,')[1].split("\"")[1]
    print latitude,longitude
    geo_coord = {'coordinates':x, 'latitude':latitude,'longitude':longitude}
    coord_list.append(geo_coord)

with open('geo_coordinates_skipped_'+date+'.json', 'w') as fp:
     json.dump(skip_coordinates, fp)


geo_coord_df = pd.DataFrame(coord_list)
geo_coord_df.to_csv('geo_coord_df.csv')
#geo_total = pd.merge(geo_df,geo_coord_df)
#geo_total.to_csv('aggregated_geo_coord.csv')
# with open('geo_coordinates_'+date+'.pkl', 'w') as fp:
#      pickle.dump(all_listing_geo, fp)

