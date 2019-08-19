#.py
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 18 07:38:52 2019

@author:

Script Purpose: Begin pulling fundamental data
"""
SF_APIKEY = '<insert key>' #Sim Fin api key
import pandas as pd
import requests
def Universe_Fundamentals():
    Univ_DF = pd.read_csv('<Data Universe path>\\Universe.csv')
    tickers = []
    # find SimFin IDs
    sim_ids = []
    for x in Univ_DF['Symbol']:
        tickers.append(x)
        request_url = 'https://simfin.com/api/v1/info/find-id/ticker/{}?api-key={}'.format(x,SF_APIKEY)
        content = requests.get(request_url)
        data = content.json()
        
        if "error" in data or len(data) < 1: #quick error checking
            sim_ids.append(None)
        else:
            sim_ids.append(data[0]['simId']) #if no error append the simID
    
    #some of the stocks will be thinly traded or nonUS. So we'll remove whatever didn't come back
    sim_ids = [z for z in sim_ids if z] #use list comprehension
            
    # define time periods for quarterly financial statement data - need to update
    statement_type = "pl"
    time_periods = ["Q1", "Q2", "Q3", "Q4"]
    year_start = 2013
    year_end = 2019
    
    # publish to excel - will want to update this to push data to a database
    writer = pd.ExcelWriter("<output path>\\simfin_data.xlsx", engine='xlsxwriter')
        
    data = {}
             
    for idx, sim_id in enumerate(sim_ids):
        d = data[tickers[idx]] = {"Line Item": []}
        if sim_id is not None:
            for year in range(year_start, year_end + 1):
                for time_period in time_periods:
    
                    # make time period identifier, i.e. Q3-2019
                    period_identifier = time_period + "-" + str(year)
    
                    if period_identifier not in d:
                        d[period_identifier] = []
    
                    request_url = 'https://simfin.com/api/v1/companies/id/{}/statements/standardised?stype={}&fyear={}&ptype={}&api-key={}'.format(sim_id, statement_type, year, time_period, SF_APIKEY)
    
                    content = requests.get(request_url)
                    statement_data = content.json()
    
                    # collect line item names once, they are the same for all companies with the standardised data
                    if len(d['Line Item']) == 0 and 'values' in statement_data:
                        d['Line Item'] = [x['standardisedName'] for x in statement_data['values']]
    
                    if 'values' in statement_data:
                        for item in statement_data['values']:
                            d[period_identifier].append(item['valueChosen'])
                    else:
                        # no data found for time period
                        d[period_identifier] = [None for _ in d['Line Item']]
    
            # fix the periods where no values were available
            len_target = len(d['Line Item'])
            if len_target > 0:
                for k, v in d.items():
                    if len(v) != len_target:
                        d[k] = [None for _ in d['Line Item']]
    
        # convert to pandas dataframe
        df = pd.DataFrame(data=d)
        # save in the XLSX file configured earlier
        df.to_excel(writer, sheet_name=tickers[idx])

    writer.save()
    writer.close()
    
    
    
    
if __name__ == '__main__':
    
    Universe_Fundamentals()
