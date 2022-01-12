#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import lru_cache
from loguru import logger as log
from unsync import unsync
from dataclassy import dataclass

@dataclass(unsafe_hash=True, slots=True)
class Funding_Data ():
           
    @lru_cache(maxsize=None)
    def tarik_data_funding_per_koin (self, koin: str):

        '''
        # Menarik semua funding rate.
                    
                Return and rtype: funding rate selected.

        '''   
        
        import requests
        
        end_point_per_koin= (f' https://ftx.com/api/futures/{koin}/stats')
        
        try:
            funding_rates_per_koin=(requests.get(end_point_per_koin).json()) ['result']
                
        except Exception as error:
            import traceback
            log.error(f"{error}")
            log.error(traceback.format_exc())
            
        return funding_rates_per_koin

    @unsync
    def kombinasi_data_funding (self, funding_rates_selected=None):

        '''
        # Membersihkan & menggabungkan funding rate.
                    
                Return and rtype: funding rate.

        '''   
        
        from dask import delayed, compute
        
        funding_rates_all = self.tarik_data_funding_all()
        #log.error(f"{funding_rates_selected}")
        #log.error(f"{funding_rates_all}")
        
        if funding_rates_selected != None:
                
            funding_rates_all = [ o for o in [o for o in funding_rates_all \
                if o['future'] in funding_rates_selected  ]]
            
        fetch_dask = []
        try:
 
            #mencari waktu funding terakhir (satu tarikan ada beberapa jam funding rates)
            funding_rates_fut_max_time = max( [ (o['time']) for o in [o for o in funding_rates_all  ]])

            #seleksi koin berdasarkan funding rates terakhir 
            funding_rates_fut_ =  [ o for o in [o for o in funding_rates_all \
                if o['time'] == funding_rates_fut_max_time  ]]
            funding_rates_fut =  [ o['future'] for o in [o for o in funding_rates_fut_  ]]
            #log.error(f"{funding_rates_fut_=}")
            
            
            data_funding =[]
            for coin in funding_rates_fut:
                
                funding_rates_next_= self.tarik_data_funding_per_koin(coin)  
                funding_rates_next = funding_rates_next_['nextFundingRate']  
                nextFundingTime = (funding_rates_next_['nextFundingTime']  )
                currentFundingTime =  [ o['time'] for o in [o for o in funding_rates_fut_ \
                    if o['future'] == coin]][0]                

                #log.error(f"{currentFundingTime=}")
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
            
        results_dask = compute(*fetch_dask) 
        
        return data_funding
