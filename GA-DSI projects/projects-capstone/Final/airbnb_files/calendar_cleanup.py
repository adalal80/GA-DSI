import pandas as pd
import numpy as np
import json
import time
from datetime import datetime

year = datetime.now().year
month = datetime.now().month
day = datetime.now().day

if day < 10:
    date = str(month)+"0"+str(day)+str(year)
else:
    date = str(month)+str(day)+str(year)

#takes in a path and aggregates into a dataframe by month
def grab_monthly_data(path):

    file_name = path.split('-')[-1].split('.')[0]+"_"+path.split('/')[-1].split('-')[0]
    df = pd.read_csv(path,usecols=range(0,4))
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(['listing_id','date'],inplace = True)
    df.reset_index(inplace = True,drop=True)
    df['available'] = df['available'].apply(lambda x: 1 if x=='t' else 0)
    df['price'] = df['price'].apply(lambda x: str(x).replace('$','').replace(',',''))
    df['price'] = df['price'].apply(lambda x: 0 if x=='nan' else x).astype(float).astype(int)
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    
    
    #calculating occupancy per month per listing
    nights = df.groupby(['listing_id','year','month'])['available'].sum().to_frame()
    nights.reset_index(inplace = True)

    #calculating the prices per month per listing
    price2 = df.groupby(['listing_id','year','month'])['price'].agg({len,np.mean,np.median,max,min})
    price2.reset_index(inplace = True)

    # merging the price and nights/occupancy dataframes
    metrics = pd.merge(price2,nights,on=['listing_id','year','month'])
    metrics['occupancy'] = (100*(metrics['len'] - metrics['available'])/metrics['len']).round(2)

    # some of the months are 0, so lets do a groupby for a year to get the average price for year and fill that in
    list_yr_price = metrics.groupby('listing_id')['median'].agg({np.mean,np.median,max})
    list_yr_price.reset_index(inplace = True)
    list_yr_price.rename(columns = {'median':'yr_median','mean':'yr_mean','max':'yr_max'},inplace = True)
    metrics1 = pd.merge(metrics,list_yr_price)
    metrics1['median'] = metrics1['median'].astype(int)
    metrics1['yr_median'] = metrics1['yr_median'].astype(int)
    metrics1['yr_max'] = metrics1['yr_max'].astype(int)
    # creating a column for the average price per month
    price_use = []
    for a,b,c,d,e,f in zip(metrics1['mean'],metrics1['median'],metrics1['max'],metrics1['min'],metrics1['yr_median'],metrics1['yr_max']):
        if (a>0) & (b>0) & (a==float(b)) & (a/e<3):
            price_use.append(a)
        elif (a==0) & (b==0) & (c==0) & (d==0) & (e==0) & (f==0):
            price_use.append(0)
        elif (a>0) & (b>0) & (c>0) & (b>=d) & (a>b):
            price_use.append(a)  #a
        elif (a<b) & ((c-b)/c < 1) & (d<c):
            price_use.append(b) #b
        elif (b==0) & (c>0) & (d==0) & ((f-c)/f<1):
            price_use.append(f) #f
        elif (a==0) & (b==0) & (c==0) & (d==0) & (f>0) & ((f-c)/f<1):
            price_use.append(f) #f
        elif (b==0) & (d==0) & ((c-f/f)<1) & (c-e/e<1) & (c>0):
            price_use.append(c) #c
        elif (a==0) & (b==0) & (c==0) & (e>0) & (f/e>3):
            price_use.append(e)
        elif (a==0) & (b==0) & (c==0) & (e>0) & (f/e<3):
            price_use.append(f)
        else:
            price_use.append(0)
    metrics1['price_use'] = price_use


    metrics1['price_use'] = metrics1['price_use'].round(0).astype(int)
    metrics1.rename(columns={'price_use':'avg_price'},inplace = True)

    # #subselecting certain columns
    avg_price_listing = metrics1.loc[:,['listing_id','year','month','avg_price','len','available','occupancy']]
    #renaming columns
    cols = {}
    for x in avg_price_listing.columns[3:]:
        cols[x] = x+"_"+file_name.split('_')[1]
    avg_price_listing.rename(columns = cols,inplace = True)    
    # exporting to csv
    file_name = "airbnb_calendars/"+file_name+"_"+date+'_df.csv'
    avg_price_listing.to_csv(file_name)
    print 'File saved to:', file_name
    return avg_price_listing 

calendar_012015 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2015/012015-calendar.csv')
calendar_032015 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2015/032015-calendar.csv')
calendar_062015 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2015/062015-calendar.csv')
calendar_092015 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2015/092015-calendar.csv')
calendar_102015 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2015/102015-calendar.csv')
calendar_112015 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2015/112015-calendar.csv')
calendar_012016 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2016/012016-calendar.csv')
calendar_022016 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2016/022016-calendar.csv')
calendar_042016 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2016/042016-calendar.csv')
calendar_052016 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2016/052016-calendar.csv')
calendar_062016 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2016/062016-calendar.csv')
calendar_072016 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2016/072016-calendar.csv')
calendar_102016 = grab_monthly_data('/Users/amishdalal/Desktop/AirBnB-NYC/2016/102016-calendar.csv')


#aggregating 2015 calendars
temp1 = pd.merge(calendar_012015,calendar_032015,how='left')
temp2 = pd.merge(temp1,calendar_062015,on=['listing_id','year','month'],how='left')
temp3 = pd.merge(temp2,calendar_092015,on=['listing_id','year','month'],how='left')
temp4 = pd.merge(temp3,calendar_102015,on=['listing_id','year','month'],how="left")
cal_15df = pd.merge(temp4,calendar_112015,on=['listing_id','year','month'],how="left")
cal_15df.rename(columns = {'len_012015':'days_per_month'},inplace = True)
cal_15df.to_csv('airbnb_calendars/calendar_15_'+date+'.csv')

#aggregating 2016 calendars
temp1_16 = pd.merge(calendar_012016,calendar_022016,how='left')
temp2_16 = pd.merge(temp1_16,calendar_042016,on=['listing_id','year','month'],how='left')
temp3_16 = pd.merge(temp2_16,calendar_052016,on=['listing_id','year','month'],how='left')
temp4_16 = pd.merge(temp3_16,calendar_062016,on=['listing_id','year','month'],how="left")
temp5_16 = pd.merge(temp4_16,calendar_072016,on=['listing_id','year','month'],how="left")
cal_16df = pd.merge(temp5_16,calendar_102016,on=['listing_id','year','month'],how="left")
cal_16df.rename(columns = {'len_012016':'days_per_month'},inplace = True)
cal_16df.to_csv('airbnb_calendars/calendar_16_'+date+'.csv')

#cleaning the calendar to fill in missing values
def clean_calendar(df,year):
    keepdf = df.iloc[:,[0,1,2,4]].copy()
    
    yr = datetime.now().year
    mo = datetime.now().month
    d = datetime.now().day
    if d<10:
        date = str(mo)+'0'+str(d)+str(yr)
    else:
        date = str(mo)+str(d)+str(yr)
        
    
    #creating occupancy columns
    occdf = df.iloc[:,5::4].fillna(method='ffill',axis=1)
    occdf['available'] = occdf.iloc[:,-1]

    #creating a price column
    pricedf = df.iloc[:,3::4].fillna(method='ffill',axis=1)
    pricedf['avg_price'] = pricedf.iloc[:,-1]
    
    #concatenating the three dataframes
    df_occ = pd.concat([keepdf,pricedf.iloc[:,-1],occdf.iloc[:,-1]],axis=1)
    df_occ['occupied'] = df_occ['days_per_month'] - df_occ['available']
    
    #finding the occupied days of the year
    occ_yr = df_occ.groupby('listing_id')['occupied'].sum().to_frame('occ_yr')
    occ_yr.reset_index(inplace = True)
    
    #finding the average price for the year
    avgprice_yr = df_occ.groupby('listing_id')['avg_price'].median().to_frame('avgprice_yr')
    avgprice_yr.reset_index(inplace = True)
    #avgprice_yr['avgprice_yr'] =avgprice_yr['avgprice_yr'].astype(int)
    
    #merging the year occupancy and year average price to the main dataframe
    final_df = pd.merge(df_occ,occ_yr)
    final_df = pd.merge(final_df,avgprice_yr)
    
    #calculations and type conversions
    final_df = final_df[(final_df['occ_yr'] > 0) & (final_df['year']==year)]
    final_df['avg_price'] = final_df['avg_price'].fillna(0)
    final_df['occupied'] = final_df['occupied'].fillna(0)

    final_df['avg_price'] = final_df['avg_price'].astype(int)
    final_df['occ%'] = (100*(final_df['occupied'] / final_df['days_per_month'])).round(2)
    final_df['available'] = final_df['available'].fillna(0)
    final_df['available'] = final_df['available'].astype(int)

    final_df['occ_yr'] = final_df['occ_yr']
    final_df['occ_yr%'] = (100*(final_df['occ_yr'] / 365)).round(2)

    avgp = []
    for x,y in zip(final_df['avg_price'],final_df['avgprice_yr']):
        if x==0:
            avgp.append(y)
        else:
            avgp.append(x)
    final_df['avg_p'] = avgp
    del final_df['avg_price'], final_df['avgprice_yr']
    final_df.rename(columns = {'avg_p':'avg_price'},inplace = True)
    file_name = 'calendar_'+str(yr)+'_df_'+date+'.csv'
    print file_name
    file_name = 'airbnb_calendars/'+file_name
    final_df.to_csv(file_name)
    return final_df

cal_2015df = clean_calendar(cal_15df,2015)
cal_2016df = clean_calendar(cal_16df,2016)

