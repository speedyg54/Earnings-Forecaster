#.py
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 18 07:38:52 2019

@author:

Script Purpose: Begin pulling fundamental data
"""
SF_APIKEY = 'Cc58LXqMvB01UelR080Pux8c0PcoElwm' #Sim Fin api key
import pandas as pd
import requests
import canned_macs
import datetime
import sys
def Universe_Fundamentals():
#    tickers = []
    # find SimFin IDs
#    sim_ids = []
    Univ = pd.read_csv('C:\\Users\\OBar\\Documents\\General Research\\Data\\Universe.csv')
    sql =   """
            SELECT SF_ID, SF_Ticker, SF_Name
            FROM equities_master.sim_fin_indicator
            WHERE SF_Ticker in ({})
            ORDER BY SF_Ticker;
            """.format(str(Univ['Symbol'].tolist())[1:-1]) #str() and [1:-1] remove the brackets around the string
    #create a pandas dataframe from the SQL query - MUST BE AN EASY WAY TO LEVERAGE THE CANNED_MACS PROGRAM TO REPLACE BELOW
    host = 'localhost'
    user = 'root'
    passwd = 'Ronaldhino54@'
    db = 'equities_master'
    con=canned_macs.mdb.connect(host, user, passwd, db)
    df = pd.read_sql_query(sql, con=con)
    #output the dataframe tail



           
    # define time periods for quarterly financial statement data - need to update
    statement_type = "pl"
    time_periods = ["Q1", "Q2", "Q3", "Q4"]
    year_end = datetime.datetime.now().year
    year_start = year_end - 3 #always grab two years behind

    data = {}
    
    for idx, sim_id in enumerate(df["SF_ID"]):
        d = data[df['SF_Ticker'][idx]] = {"Line Item": []}
        if sim_id is not None:
            #the below request is performed immediately because there's no date logic involved
            #so we don't want to loop this request
            request_bsurl= 'https://simfin.com/api/v1/companies/id/{}/shares/aggregated?filter={}&api-key={}'.format(sim_id, 'common-outstanding-basic', SF_APIKEY)
    #example...https://simfin.com/api/v1/companies/id/178804/shares/aggregated?filter=common-outstanding&api-key=Cc58LXqMvB01UelR080Pux8c0PcoElwm
            contentbs = requests.get(request_bsurl)
            statement_databs = contentbs.json()
            
            for year in range(year_start, year_end + 1):
                for time_period in time_periods:
    
                    # make time period identifier, i.e. 2019Q3
                    period_identifier = str(year) + time_period
    
                    if period_identifier not in d:
                        d[period_identifier] = []
    
                    request_plurl = 'https://simfin.com/api/v1/companies/id/{}/statements/standardised?stype={}&fyear={}&ptype={}&api-key={}'.format(sim_id, statement_type, year, time_period, SF_APIKEY)
                    
                    contentpl = requests.get(request_plurl)
                    statement_datapl = contentpl.json()

                    #For Income Statement collect line item names once, they are the same for all companies with the standardised data
                    if len(d['Line Item']) == 0 and 'values' in statement_datapl:
                        d['Line Item'] = [x['standardisedName'] for x in statement_datapl['values']]
                        d['Line Item'].append('common-outstanding-basic')
                    elif 'values' in statement_datapl:
                        for item in statement_datapl['values']:
                            d[period_identifier].append(item['valueChosen'])
                    for y in statement_databs:
                        if (y['period'] == 'Q1' or \
                           y['period'] == 'Q2' or \
                           y['period'] == 'Q3' or \
                           y['period'] == 'Q4') and y['figure'] == 'common-outstanding-basic':
                            dt_check = pd.to_datetime(pd.Series((y['date'])))
                            if str(dt_check.dt.to_period('Q').iloc[0]).strip() == period_identifier:                            
                                d[period_identifier].append( y['value'])        

            #To ensure all arrays are the same length
            len_target = len(d['Line Item'])
            if len_target > 0:
                #d[] = dict([ (k,Series(v)) for k,v in d.items() ])
                for k, v in d.items():
                    if len(v) != len_target:
                        d[k] = [None for _ in d['Line Item']]
                        
            
    
        # convert to pandas dataframe
        try:
            funddf = pd.DataFrame(data=d) 
            funddf.to_csv('C:\\Users\\OBar\\Documents\\General Research\\Data\\SF_{}.csv'.format(df['SF_Ticker'][idx]))
        except ValueError:
            print("Value Error encountered the following was not published {}".format(df['SF_Ticker'][idx]))
        #Export instead to database.
    
    
    
    
if __name__ == '__main__':
    
    Universe_Fundamentals()
