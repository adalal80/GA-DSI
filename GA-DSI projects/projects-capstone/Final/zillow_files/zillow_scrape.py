import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import pickle
import json
import time

#import the scraping links dataframe
scraping_links = pd.read_csv("../../scraping_links.csv")

#manipulating specific links to include sorting features. These are because the lists exceed 20.
#in Zillow, the max pages you can get is 20.

zillow_urls = scraping_links["new_zillow_links"].drop_duplicates()
all_links = zillow_urls
all_links.pop(18)  #financial district results in issues, need to pull separately

i=0
i_list = [8,9,16,19,24,25,26]
link_list = []
sort_list = ["a","d"]
br_list = [1,2,3,4]
skip_list = []
for link in all_links:
    if i in i_list:
        for x in sort_list:
            for y in br_list:
                link_list.append(link+"payment"+str(x)+"_sort/"+str(y)+"-_beds/")
    else:
        link_list.append(link)
    i+=1

def get_zillow(links):
    all_listings = []
    out = []
    listings = {}
    total = 0
    #look at each link
    for page in links:
        z = requests.get(page)
        soup_z = BeautifulSoup(z.text,"lxml")
        # for each page, get total number of pages, so we know how many pages to scrape
        try:
            total = soup_z.find("title").get_text().split("-")[1].split("|")[0].split(" ")[1].replace(",","")
            total = int(total)
            total_pages = int(round(total/float(26)))
        #incase the formatting is different in zillow, 
        except(ValueError,IndexError):
            total = soup_z.find("title").get_text().split("-")[2].split("|")[0].split(" ")[1].replace(",","")
            total = int(total)
            total_pages = int(round(total/float(26)))
        #print page,total,total_pages
        i=0
        #for each page, go through the amount of total pages, with max being 20
        for y in range(1,min(total_pages+1,20)):

                page_np = page+str(y)+"_p/"
                np = requests.get(page_np)
                print page_np
                soup_np = BeautifulSoup(np.text,"lxml")
                zillow = soup_z.findAll('article')
                #for each listing in zillow, pull the information below
                for listing in zillow:
                    try:
                        address =  listing.find('span',{"class":"zsg-photo-card-address"}).text   #address
                        link =  listing.a["href"]  #link
                        for x in listing.find('div',{"class":"minibubble template hide"}):
                            sqft =  str(x).split("sqft")[-1].split(',')[0].replace(":","").replace("\"","") #sqft
                            beds =  int(str(x).split("\"bed\":")[1].split(',')[0]) #beds
                            if "priceRange" in str(x):
                                price =  str(x).split("\"priceRange\":")[1].split("\\\/mo")[0].replace("+","\"").replace("$","").replace(",","")
                            else:
                                if "title" in str(x):
                                    price =  str(x).split("\"title\":")[1].split("\\\/mo")[0].replace("$","").replace(",","").replace("\"","")
                    #add the information into a dictionary
                        listings = {"address":address,"link":link,"sqft":sqft,"beds":beds,"price":price,"listing_no":i,\
                                   "main_url":page,"url":page_np}
                    #adding the listings to a list
                        all_listings.append(listings)
                        i+=1
                    #if there are any errors, add that page to the scripe
                    except(IndexError,AttributeError,ValueError):
                        skip = {"main_url":page,"url":page_np}
                        skip_list.append(skip)
        time.sleep(10)
        out = out + all_listings
        with open('zillow_scrape.json', 'w') as fp:
            json.dump(out, fp)
    return out

zillow_scrape1 = get_zillow(link_list)

    #grabbing Financial District  because it gave an error while using get_zillow function
url="http://www.zillow.com/Financial-District-New-York-NY/rentals/"
z = requests.get(url)
soup_z = BeautifulSoup(z.text,"lxml")
soup_z.find("title").get_text()
fidi_urls = []
for x in range(1,5):
    fidi_urls.append(url+str(x)+"_p/")
fidi_urls
i=0
zillow = soup_z.findAll('article')
total = soup_z.find("title").get_text().split("-")[1].split("|")[0].split(" ")[1].replace(",","")
total = int(total)
#print total
fidi_listings = []
for fidi_url in fidi_urls:
    for listing in zillow:
        address =  listing.find('span',{"class":"zsg-photo-card-address"}).text   #address
        link =  listing.a["href"]  #link
        for x in listing.find('div',{"class":"minibubble template hide"}):
            sqft =  str(x).split("sqft")[-1].split(',')[0].replace(":","").replace("\"","") #sqft
            beds =  (str(x).split("\"bed\":")[1].split(',')[0]) #beds
            if "priceRange" in str(x):
                price =  str(x).split("\"priceRange\":")[1].split("\\\/mo")[0].replace("+","\"").replace("$","").replace(",","")
            else:
                if "title" in str(x):
                    price =  int(str(x).split("\"title\":")[1].split("\\\/mo")[0].replace("$","").replace(",","").replace("\"",""))
    #print i, address, link, sqft, beds, price
        fidi_lists = {"address":address,"link":link,"sqft":sqft,"beds":beds,"price":price,"listing_no":i,\
                           "main_url":url,"url":fidi_url}
        fidi_listings.append(fidi_lists)
        #print listings
        i+=1
    #print fidi_url
    
fidi_df = pd.DataFrame(fidi_listings)
fidi_df.to_json('financial_district.json')

zillow_scrape_df = pd.DataFrame(zillow_scrape1)
zillow_scrape_df = zillow_scrape_df.append(fidi_df)

zillow_scrape_df['prop_type'] = zillow_scrape_df['link'].apply(lambda x: 'building' if x[1]=='b' else 'home')
zillow_scrape_df['zipcode'] = zillow_scrape_df['link'].apply(lambda x: x.split('/')[2].split('-')[-1] if x[1]=='h' else 0)
zillow_scrape_df.to_csv('zillow_scrape_df.csv')
link_df = zillow_scrape_df.loc[:,['link','main_url','url']].drop_duplicates()