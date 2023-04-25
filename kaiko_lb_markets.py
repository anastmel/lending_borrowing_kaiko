#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 14:06:47 2023

@author: anastasiamelachrinos
"""

import pandas as pd
import requests

## GET KAIKO LENDING AND BORROWING RATES & LIQUIDITY DATA 
def kaiko_lb_markets(apikey, protocols, assets, interval, quote_assets=None, blockchain=None, start_time=None, end_time=None, block_number=None, start_block=None, end_block=None):
    '''
    Parameters
    ----------
    apikey: an API key to authenticate the requests to the Kaiko API
    start_time: (optional) a string representing the start time in the format '%Y-%m-%dT%H:%M:%S.%fZ'
    end_time: (optional) a string representing the end time in the format '%Y-%m-%dT%H:%M:%S.%fZ'
    protocols: a list of strings representing the protocols to retrieve data for
    assets: a list of strings representing the assets to retrieve data for
    quote_assets: (optional) a list of strings representing the quote assets to retrieve data for
    interval: (optional) a string representing the time interval for the data, possible values are '1h', '1d', '1m'
    blockchain: (optional) a string representing the blockchain to retrieve data for
    block_number: (optional) a string representing the block number to retrieve data for
    start_block: (optional) a string representing the start block to retrieve data for
    end_block: (optional) a string representing the end block to retrieve data for
    '''
    ########################################################################################################################################################################################
    # GET LENDING & BORROWING RATES & LIQUIDITY DATA #######################################################################################################################################
    ########################################################################################################################################################################################
    headers_dict = {'Accept': 'application/json', 'X-Api-Key': apikey}
    df_list=[]
    for protocol in protocols:
        for asset in assets:
            try:
                url = 'https://eu.market-api.kaiko.io/v2/data/lending.v1/snapshots'
                params_dict = {'start_time': start_time, 'end_time': end_time, 'interval': interval, 'protocol': protocol, 'blockchain': blockchain, 'asset': asset, 'block_number': block_number, 'start_block': start_block, 'end_block': end_block}
                res = requests.get(url, headers=headers_dict, params=params_dict).json()
                df = pd.DataFrame(res['data'])
                while 'next_url' in res.keys():
                    res = requests.get(res['next_url'], headers=headers_dict).json()
                    df = pd.concat([df, pd.DataFrame(res['data'])], ignore_index=True)
            except:
                print('no market found for ' + str(asset) + ' ' + str(protocol))
            df_list.append(df)
    final_df=pd.concat(df_list)
    def flatten_metadata(df):
        df_metadata = pd.json_normalize(df['metadata'])
        df_metadata.columns = ['metadata.' + '.'.join(str(s) for s in col) if isinstance(col, tuple) else 'metadata.' + str(col) for col in df_metadata.columns]
        df = pd.concat([df.reset_index(drop=True), df_metadata.reset_index(drop=True)], axis=1).drop('metadata', axis=1)
        return df
    final_data = flatten_metadata(final_df)
    final_data['date'] = pd.to_datetime(final_data['datetime'], unit='s').dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    final_data['asset_symbol'] = final_data['asset_symbol'].str.lower()

    if quote_assets is not None:    
        ########################################################################################################################################################################################
        # GET CROSS PRICE FOR CONVERSIONS ######################################################################################################################################################
        ########################################################################################################################################################################################
        headers = {'Accept': 'application/json',
                   'X-Api-Key': apikey}
        df_list = []
        for base in assets:
            for quote in quote_assets:
                try:
                    url = f'https://us.market-api.kaiko.io/v2/data/trades.v1/spot_exchange_rate/{base}/{quote}?start_time={start_time}&end_time={end_time}&interval={interval}'
                    res = requests.get(url, headers=headers)
                    df = pd.DataFrame(res.json()['data'])
                    df['base'] = (base)
                    df['quote'] = (quote)
                    while 'next_url' in res.json():
                        res = requests.get(res.json()['next_url'], headers=headers)
                        data =  pd.DataFrame(res.json()['data'])
                        data['base'] = (base)
                        data['quote'] = (quote)
                        df = pd.concat([df,data], ignore_index=True)
                except:
                    print('no instrument found')
                df_list.append(df)    
        final_df = pd.concat(df_list)
        final_df['timestamp'] = final_df['timestamp'] / 1000
        final_df['date'] = pd.to_datetime(final_df['timestamp'], unit='s').dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        ########################################################################################################################################################################################
        # CONVERT ALL AMOUNTS IN USD ###########################################################################################################################################################
        ########################################################################################################################################################################################
        def add_time_columns(df):
            df['date'] = pd.to_datetime(df['date'])
            df['day'] = df['date'].dt.day
            df['hour'] = df['date'].dt.hour
            df['minute'] = df['date'].dt.minute
            df['second'] = df['date'].dt.second
            df['month'] = df['date'].dt.month
            df['year'] = df['date'].dt.year
            return df
        
        cross = add_time_columns(final_df)
        data = add_time_columns(final_data)
        
        def merge_dataframes(df1, df2, interval):
            if interval == '1d':
                merge_columns = ['year', 'month', 'day']
            elif interval == '1m':
                merge_columns = ['year', 'month', 'day', 'hour', 'minute']
            elif interval == '1h':
                merge_columns = ['year', 'month', 'day', 'hour']
            else:
                raise ValueError('Invalid interval specified. Please enter either "1d", "1m", or "1h"')
            merged_df = pd.merge(df1, 
                                 df2,
                                 left_on=merge_columns + ['asset_symbol'],
                                 right_on=merge_columns + ['base'])
            return merged_df
        
        merged_df = merge_dataframes(df1=data, df2=cross, interval=interval)
        words_to_drop = ['year', 'month', 'day', 'hour', 'minute', 'second']
        for col in merged_df.columns:
            for word in words_to_drop:
                if word in col:
                    merged_df.drop(col, axis=1, inplace=True)
        merged_df.drop('date_y', axis=1, inplace=True)
        merged_df.drop('timestamp', axis=1, inplace=True)
        merged_df.rename(columns={'date_x': 'date'}, inplace=True)
        merged_df['pair'] = merged_df['base'] + '-' + merged_df['quote']
        merged_df.drop(['base', 'quote'], axis=1, inplace=True)
        suffix = 'usd_'
        columns_to_convert = ['available_liquidity', 'total_borrowed', 'total_liquidity','metadata.total_borrowed_stable', 'metadata.total_borrowed_variable','metadata.total_reserves','metadata.debt_ceiling']
        for col in columns_to_convert:
            try:
                merged_df[suffix+col] = merged_df[col].astype(float) * merged_df['price'].astype(float)
            except KeyError:
                pass # ignore columns that are not present in the dataframe
        return merged_df
    return final_data
