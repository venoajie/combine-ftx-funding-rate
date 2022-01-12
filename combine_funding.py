#!/usr/bin/env python
# -*- coding: utf-8 -*-

from loguru import logger as log
from dataclassy import dataclass
import requests

@dataclass(unsafe_hash=True, slots=True)
class Funding_Data ():
           
    def fetch_funding_per_coin (self, coin: str):

        '''
        # Fetch next funding rate for each coin.
                    
                Return and rtype: next funding rate for selected coin

        '''   
        
        end_point_per_coin= (f' https://ftx.com/api/futures/{coin}/stats')
        
        try:
            funding_rates_per_coin=(requests.get(end_point_per_coin).json()) ['result']
                
        except Exception as error:
            import traceback
            log.error(f"{error}")
            log.error(traceback.format_exc())
            
        return funding_rates_per_coin

    def combining_funding_rate (self):

        '''
        # Combining current and next funding rate
                    
                Return and rtype: funding rate.

        '''   
        
        funding_rates_all = self.tarik_data_funding_all()
        
        try:
 
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
                funding_rates_next_= self.fetch_funding_per_coin(coin)  
                funding_rates_next = funding_rates_next_['nextFundingRate']  
                nextFundingTime = (funding_rates_next_['nextFundingTime']  )

                # obtain current funding time
                currentFundingTime =  [ o['time'] for o in [o for o in funding_rates_fut_ \
                    if o['future'] == coin]][0]                

                # combining current and next coins rate
                dicttemp = {}                
                dicttemp = {'future': coin ,
                            'nextRate': funding_rates_next,
                            'nextFundingTime': nextFundingTime,
                            'time': currentFundingTime,
                            }
                
                data_funding_ = data_funding.append(dicttemp.copy())
                                
        except Exception as error:
            import traceback
            log.error(f"{error}")
            log.error(traceback.format_exc())
                    
        return data_funding
