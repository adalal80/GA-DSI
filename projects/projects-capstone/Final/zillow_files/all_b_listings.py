import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import pickle
import json
import time

#setting the current date to append it to the output file of the scrape
from datetime import datetime
if datetime.now().day < 10:
    date = str(datetime.now().month)+"0"+str(datetime.now().day)+str(datetime.now().year)
else:
    date = str(datetime.now().month)+str(datetime.now().day)+str(datetime.now().year)
date
start = 1000
file_name = "all_b_listings_"+str(date)+".json"
#calling the zillow_scrape_df. Binning into links with /b (building) and /homedetails (specific listings)
zillow_scrape_df = pd.read_csv('zillow_scrape_df.csv')
building_list = []
home_list = []
other = []
for listing in zillow_scrape_df['link'].unique():
    if listing[1]=='b':
        building_list.append(listing)
    elif listing[1]=='h':
        home_list.append(listing)
    else:
        other.append(listing)

#scrape all the urls with "/b" which denotes apartment buildings. inside they will have individual listings
#gather all lists of /homedetail links then scrape all of them
all_b_listings = []
skip=[]
out = []
j=0
for url in building_list:
    zillow_url =  "http://www.zillow.com"+url
    soup_zlisting = requests.get(zillow_url)
    soup_zl = BeautifulSoup(soup_zlisting.text,"lxml")
    print zillow_url

    try:
        i=1
        address1 = 0
        address2 = 0
        for x in soup_zl.find_all("div",class_="individual-unit"):
            address1 = soup_zl.find('h1',class_="zsg-content_collapsed").text
            address2 = soup_zl.find('h2',class_="zsg-h5").text
            address = address1+" "+address2
            if 'Available' in str(x):
                address =  address
                list_text =  x.text
                list_url =  x.a['href']
                
                listing = {"address":address,"num_units":i,"list_url":list_url,"main_url":url,"list_text":list_text}
                print listing
                all_b_listings.append(listing)
                i+=1
            else:
                pass
    except:
        skip.append(url)
        print url

    j+=1
    if (j %10 == 0):
        time.sleep(10)
# with open('all_b_listings.pkl', 'w') as picklefile:
#     pickle.dump(all_b_listings, picklefile)

with open(file_name, 'w') as fp:
    json.dump(all_b_listings, fp)

all_b_listings_df = pd.DataFrame(all_b_listings)
all_b_listings_df.to_csv('all_b_listings_df.csv')
b_link_df = all_b_listings_df.loc[:,['main_url','list_url']]
b_link_df.to_csv('all_building_links.csv')

#combining all urls with /homedetails to pull all information
home_list_pull = []
for url in home_list:
    z_url = "http://www.zillow.com"+url
    home_list_pull.append(z_url)
for url in all_b_listings_df['list_url']:
    home_list_pull.append(url)
file_name = 'home_list_pull_'+str(date)+'.json'
with open(file_name, 'w') as fp:
    json.dump(home_list_pull, fp)


zipcode_list = []
for x in home_list_pull:
    zipcode = x.split('/')[4].split('-')[-1]
    zipcode_list.append(zipcode)
url_zip = pd.Series(home_list_pull).to_frame('list_url')
url_zip = pd.concat([url_zip,pd.Series(zipcode_list)],axis=1)
url_zip.rename(columns = {0:'zipcode'},inplace = True)