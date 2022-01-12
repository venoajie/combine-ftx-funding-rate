#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclassy import dataclass
import requests

@dataclass(unsafe_hash=True, slots=True)
class Funding_Data ():
           
    def fetch_current_funding_rate (self):

        '''
        # Fetch current funding rate for all coin.
                    
                Return and rtype: next funding rate for selected coin

        '''   
        
        end_point_all_coin= (f' https://ftx.com/api/funding_rates')
                        
        return requests.get(end_point_all_coin).json() ['result']
    
           
    def fetch_next_funding_rate (self, coin: str):

        '''
        # Fetch next funding rate for each coin.
                    
                Return and rtype: next funding rate for selected coin

        '''   
        
        end_point_per_coin= (f' https://ftx.com/api/futures/{coin}/stats')
            
        return requests.get(end_point_per_coin).json() ['result']

    def combining_funding_rate (self):

        '''
        # Combining current and next funding rate
                    
                Return and rtype: funding rate.

        '''   
        
        # fetch current funding rate data
        funding_rates_all = self.fetch_current_funding_rate()

        # obtain most recent funding time (there were some funding time contained in the endpoint result)
        funding_rates_fut_max_time = max( [ (o['time']) for o in [o for o in funding_rates_all  ]])

        # update coin funding rate based on the most recent funding time 
        funding_rates_fut_ =  [ o for o in [o for o in funding_rates_all \
            if o['time'] == funding_rates_fut_max_time  ]]
        
        # prepare symbol coins
        symbols =  [ o['future'] for o in [o for o in funding_rates_fut_  ]]           
        
        data_funding =[]
        for coin in symbols:
            
            # obtain funding next rate based on individual coin
            nextFundingRate_= self.fetch_next_funding_rate (coin)  
            nextFundingRate = nextFundingRate_['nextFundingRate']  
            nextFundingTime = (nextFundingRate_['nextFundingTime']  )

            # obtain current funding time
            currentFundingTime =  [ o['time'] for o in [o for o in funding_rates_fut_ \
                if o['future'] == coin]][0]                

            # combining current and next coins rate
            dicttemp = {}                
            dicttemp = {'future': coin ,
                        'nextRate': nextFundingRate,
                        'nextFundingTime': nextFundingTime,
                        'time': currentFundingTime,
                        }
            
            data_funding_ = data_funding.append(dicttemp.copy())        
                    
        return data_funding


def main():
    funding_data=Funding_Data()
    return funding_data.combining_funding_rate()

if __name__ == "__main__":

    try:
        main=main()
        print(main)
            
    except Exception as error:
        import traceback
        from loguru import logger as log
        log.error(f"{error}")
        log.error(traceback.format_exc())
    
    
    
    