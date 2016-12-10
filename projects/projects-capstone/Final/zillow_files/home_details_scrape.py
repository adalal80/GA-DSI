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
file_name = 'home_'+str(start)+"_"+str(date)+".json"
#opening up the list with all the urls to the homedetails
with open('zillow_files/home_list_pull.json', 'r') as fp:
    home_list_pull = json.load(fp)

out = []
skip = []
def zillow_home_details(link,start):
    out = []
    all_home_listings = []
    home_listing = 0
    j=start
    for zillow_url in link:
        soup_zlisting = requests.get(zillow_url)
        soup_zl = BeautifulSoup(soup_zlisting.text,"lxml")
        address = "null"
        beds = "null"
        baths = "null"
        sqft = "null"
        print j, zillow_url
        try:
            i=0
            address = soup_zl.find("h1",class_="notranslate").text

            for x in soup_zl.find_all("span",class_="addr_bbs"):
                if ("bed" in str(x)) or ("Studio" in str(x)):
                    beds = x.text
                    #print beds
                if "bath" in str(x):
                    baths = x.text
                    #print baths
                if "sqft" in str(x):
                    sqft = x.text
                    #print sqft

            for x in soup_zl.find_all("div",class_="main-row home-summary-row"):
                price = str(x.text).strip()
            print address, beds, baths, sqft,price
            home_listing = {"list_url":zillow_url,"address":address,"beds":beds,"baths":baths,"price":price,"sqft":sqft,'listing_num':j}
            all_home_listings.append(home_listing)
    
        except:
            s=0
            skip.append(zillow_url)
            print s,zillow_url
            s+=1
        j+=1
        if (j %10 == 0):
            time.sleep(10)
        file_name = 'home_'+str(start)+"_"+str(date)+".json"
        with open(file_name, 'w') as zh1:
            json.dump(all_home_listings, zh1)
    return all_home_listings

home_1000 = zillow_home_details(home_list_pull[:1000],0)
with open('home_1000.json', 'w') as zh1:
    json.dump(home_1000, zh1)

home_2000 = zillow_home_details(home_list_pull[1000:2000],1000)
with open('home_2000.json', 'w') as zh2:
    json.dump(home_2000, zh2)

home_3000 = zillow_home_details(home_list_pull[2000:],2000)
with open('home_3000.json', 'w') as zh3:
    json.dump(home_3000, zh3)



#checking to see if there is any available information that can be scraped from the skipped list
new_skip = []
skipped = []
for x in skip:
    new_skip.append(x)
#pull all listings from home_list_pull. 
all_home_listings_skip = []
home_listing = 0
j=0
for zillow_url in new_skip:
    soup_zlisting = requests.get(zillow_url)
    soup_zl = BeautifulSoup(soup_zlisting.text,"lxml")

    print j, zillow_url
    try:
        i=0
        address = soup_zl.find("h1",class_="notranslate").text

        for x in soup_zl.find_all("span",class_="addr_bbs"):
            if ("bed" in str(x)) or ("Studio" in str(x)):
                beds = x.text
                #print beds
            if "bath" in str(x):
                baths = x.text
                #print baths
            if "sqft" in str(x):
                sqft = x.text
                #print sqft
            
        for x in soup_zl.find_all("div",class_="main-row home-summary-row"):
            price = str(x.text).strip()
        print address, beds, baths, sqft,price
        home_listing = {"list_url":zillow_url,"address":address,"beds":beds,"baths":baths,"price":price,"sqft":sqft,'listing_num':j}
        all_home_listings_skip.append(home_listing)
        #all_home_listings.append(home_listing)

    except:
        skipped.append([zillow_url])
        print zillow_url

    j+=1
    if (j %10 == 0):
        time.sleep(10)
skip_df = pd.DataFrame(all_home_listings_skip)
skip_df.to_csv('skipped_df.csv')


