#.py
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 11 08:57:21 2019

@author: OBar

Script Purpose: Create our short term stock universe.
"""

import pandas as pd
import datetime as dt
from requests_html import HTMLSession
from datetime import timedelta


def Earnings_Universe():
    target_day = 0 #first day in a week is sunday
    delta_day = target_day - dt.datetime.now().isoweekday()
    if delta_day >= 0: 
        delta_day -= 7 # go back 7 days
    week_strt = dt.datetime.now() + timedelta(days=delta_day)

    from_1 = week_strt.strftime("%Y-%m-%d") #create from date
    to_1 = (week_strt + dt.timedelta(days=6)).strftime('%Y-%m-%d') #create to date
        #Prep for the for statement
    daysvec = [] #create a list to store the different days
    tgt_tbl = []
    session = HTMLSession()
    dataframes = ('Day1','Day2','Day3','Day4','Day5')
    week_tbls = {}
    for i, dfs in zip(range(5), dataframes):
        daysvec.append((week_strt + dt.timedelta(days=1+i)).strftime('%Y-%m-%d'))  #fill the days list
        
        r = session.get('https://finance.yahoo.com/calendar/earnings?from={}&to={}&day={}'
                        .format(from_1, to_1, daysvec[i])) #perform get request

        content = r.html.find('tr') #find the table rows in the html doc
        for line in content: #loop through all table rows and pull out text
            tgt_tbl.append(line.text.split('\n')) #find the text and split it     
    
    
        FT_DF = pd.DataFrame(data=tgt_tbl[1:], #make the dataframe
                             columns=tgt_tbl[0])
        FT_DF.insert(0, 'Pull_Date', daysvec[i]) #Add the pull date
        week_tbls[dfs] = FT_DF #push each dataframe into the dictionary
    #print(week_tbls)
    FFT_DF = pd.concat([df for df in week_tbls.values()], ignore_index=True)
    FFT_DF = FFT_DF.loc[(FFT_DF['Earnings Call Time'] == 'After Market Close' )| (FFT_DF['Earnings Call Time'] == 'Before Market Close')] #Filter out the non Before the market or AFter the market records
    return FFT_DF



if __name__ == '__main__':
    Universe = Earnings_Universe()
    print(Universe)


