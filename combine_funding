#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import lru_cache
from loguru import logger as log
from unsync import unsync
import data_statis as data
from dataclassy import dataclass

@dataclass(unsafe_hash=True, slots=True)
class Data_Pasar ():
            
    @unsync
    def tarik_tickers (self):

        '''
        # Menarik tickers untuk seluruh koin
                Return and rtype:  tickers (list)
        '''   
                
        import requests
        end_point=(f' https://ftx.com/api/markets')

        try:
            tickers=(requests.get(end_point).json()) ['result']

        except Exception as error:
            import traceback
            log.error(f"{error}")
            log.error(traceback.format_exc())
            
        return tickers

    @unsync
    def detail_tickers(self, koin: str, tickers=None):
                
        '''
        # Memecah informasi json ticker agar siap olah
                Return and rtype: simbol, tipe, tick, bid, ask dst untuk setiap instrumen  dalam float
                
        '''   
    @lru_cache(maxsize=None)
    def tarik_data_funding_all (self):

        '''
        # Menarik semua funding rate.
                    
                Return and rtype: funding rate selected.

        '''   
        
        import requests
        
        end_point_all=(f' https://ftx.com/api/funding_rates')
        
        try:
            funding_rates_all=(requests.get(end_point_all).json()) ['result']
                
        except Exception as error:
            import traceback
            log.error(f"{error}")
            log.error(traceback.format_exc())
            
        return funding_rates_all


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
