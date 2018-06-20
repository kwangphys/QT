# -*- coding: utf-8 -*-
"""
Created on Sat Feb 11 22:51:39 2017

@author: zoum
"""

import os
import numpy as np
import pandas as pd
import datetime as dt
import quandl
import gzip
import shutil

AUTH_TOKEN="zgkposX-rhqh9WtCLwKc"

def get_quandl_data_by_contract(exchange, contract, start_date, end_date, use_settle_as_close = False):
    data = quandl.get(exchange +'/'+contract, authtoken=AUTH_TOKEN)
    data = data[start_date:end_date]
    # rename column 'Close' to 'Last'
    data.columns = ['Last' if x=='Close' else x for x in data.columns]
    if use_settle_as_close == False:
        data = data[['Open', 'High', 'Low', 'Last', 'Volume']]
    else:
        data = data[['Open', 'High', 'Low', 'Settle', 'Volume']]
    data.columns = ['open', 'high', 'low', 'close', 'volume']
    return data

def build_continuous_futures(exchange, fut_symbol, use_settle_as_close = False):
    roll_path = os.path.join(os.environ['QT_MKTDATA_PATH'], 'roll')
    roll_file = os.path.join(roll_path, 'roll_schedule_' + exchange + '_' + fut_symbol + '.csv')
    roll_df = pd.read_csv(roll_file)
    output_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'roll_adj'])
    roll_date = None
    prev_close = None
    prev_contract = None
    for index, row in roll_df.iterrows():
        start_date = row['startDate']
        end_date = row['endDate']
        contract = row['contract_id']
        if index == 0:
            data_df = get_quandl_data_by_contract(exchange, contract, start_date, end_date, use_settle_as_close)
            data_df['roll_adj'] = 0
        else:
            data_df = get_quandl_data_by_contract(exchange, contract, roll_date, end_date, use_settle_as_close)
            data_df['roll_adj'] = 0
            try:
                close = data_df.ix[roll_date]['close']
            except Exception as e:
                print(str(e))
                raise ValueError("Please check the roll schedule for %s %s on roll date %s!" % (exchange, fut_symbol, roll_date))
            if close is None:
                raise ValueError("last or settle price is not available, please change use_settle_as_close setting for %s %s" % (exchange, fut_symbol))
            roll_adj = close - prev_close
            print("roll %s from %s to %s %f" % (roll_date, prev_contract, contract, roll_adj))
            output_df.ix[roll_date]['roll_adj'] = roll_adj
        output_df = output_df.append(data_df[start_date:])
        roll_date = end_date
        prev_close = data_df['close'][-1]
        prev_contract = contract
    return output_df

def apply_roll_adjustment_additive(fut_df):
    df = fut_df.copy()
    df['time'] = df.index
    df['time'] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df = df.reset_index(drop=True)
    df['roll_adj_cum'] = df.ix[::-1, 'roll_adj'].cumsum()[::-1]
    roll_df = pd.DataFrame(columns=['time','open','high','low','close','volume','wap'])
    roll_df.time = [t + " 00:00:00" for t in df['time']]
    roll_df.open = df['open'] + df['roll_adj_cum']
    roll_df.high = df['high'] + df['roll_adj_cum']
    roll_df.low = df['low'] + df['roll_adj_cum']
    roll_df.close = df['close'] + df['roll_adj_cum']
    roll_df.volume = df['volume']
    roll_df['wap'] = (roll_df['open'] + roll_df['close'] + roll_df['high'] + roll_df['low']) / 4.0
    return roll_df
    
def apply_roll_adjustment_multiplicative(fut_df):
    df = fut_df.copy()
    df['time'] = df.index
    df['time'] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df = df.reset_index(drop=True)    
    df['log_mup_roll_adj'] = np.log((df['close'] + df['roll_adj'])/df['close'])
    df['roll_adj_cum'] = np.exp(df.ix[::-1, 'log_mup_roll_adj'].cumsum()[::-1])
    roll_df = pd.DataFrame(columns=['time','open','high','low','close','volume','wap'])
    roll_df.time = [t + " 00:00:00" for t in df['time']]
    roll_df.open = df['open'] * df['roll_adj_cum']
    roll_df.high = df['high'] * df['roll_adj_cum']
    roll_df.low = df['low'] * df['roll_adj_cum']
    roll_df.close = df['close'] * df['roll_adj_cum']
    roll_df.volume = df['volume']
    roll_df['wap'] = (roll_df['open'] + roll_df['close'] + roll_df['high'] + roll_df['low']) / 4.0
    return roll_df
    
if __name__ == "__main__":
#    # single contract download without rolling
#    exchange = 'HKEX'
#    contract = 'HSIK2009'
#    start_date = dt.date(2009,1,1)
#    end_date = dt.date(2009,12,30)
#    data = get_quandl_data_by_contract(exchange, contract, start_date, end_date, use_settle_as_close = False)
#    print(data)


    # continuous contract download with rolling    
    symbol_map = {
        # IB symbol key : ( exchange, fut_symbol, use_settle_as_close, output_folder )
        'CL': ('CME', 'CL', True, 'NYMEX'),
        'HO': ('CME', 'HO', True, 'NYMEX'),
        'NG': ('CME', 'NG', True, 'NYMEX'),
        'RB': ('CME', 'RB', True, 'NYMEX'),
        'GC': ('CME', 'GC', True, 'NYMEX'),
        'HG': ('CME', 'HG', True, 'NYMEX'),
        'PL': ('CME', 'PL', True, 'NYMEX'),
        'SI': ('CME', 'SI', True, 'NYMEX'),
        
        'KE': ('CME', 'KW', True, 'ECBOT'),
        'ZC': ('CME', 'C',  True, 'ECBOT'),
        'ZL': ('CME', 'BO', True, 'ECBOT'),
        'ZM': ('CME', 'SM', True, 'ECBOT'),
        'ZN': ('CME', 'TY', True, 'ECBOT'),
        'ZS': ('CME', 'S',  True, 'ECBOT'),
        'ZW': ('CME', 'W',  True, 'ECBOT'),

        'ES': ('CME', 'ES', True, 'GLOBEX'),
        'GF': ('CME', 'FC', True, 'GLOBEX'),
        'HE': ('CME', 'LN', True, 'GLOBEX'),
        'LE': ('CME', 'LC', True, 'GLOBEX'),

        'CC': ('ICE', 'CC', True, 'NYBOT'),
        'CT': ('ICE', 'CT', True, 'NYBOT'),
        'KC': ('ICE', 'KC', True, 'NYBOT'),
        'SB': ('ICE', 'SB', True, 'NYBOT'),

        'NIFTY' : ('SGX', 'IN',  False, 'SGX'),
        'SCI'   : ('SGX', 'FEF', False, 'SGX'),
        'SGXNK' : ('SGX', 'NK',  False, 'SGX'),
        'SSG'   : ('SGX', 'SG',  False, 'SGX'),
        'STW'   : ('SGX', 'TW',  False, 'SGX'),
        'XINA50': ('SGX', 'CN',  False, 'SGX'),
    }
        
    for key, value in sorted(symbol_map.items()):
        print("start downloading continuous futures for %s ..." % (key))
        exchange = value[0]
        fut_symbol = value[1]
        use_settle_as_close = value[2]
        output_folder = value[3]
        output_path = os.path.join(os.environ['QT_MKTDATA_PATH'], output_folder)
        fut_df = build_continuous_futures(exchange, fut_symbol, use_settle_as_close)
        csv_raw = output_path + '/' + key + '_1d_cont_raw.csv'
        fut_df.to_csv(csv_raw)
        roll_fut_df = apply_roll_adjustment_additive(fut_df)
        
        csv_roll_add = os.path.join(output_path, key + '_1d_cont_roll_add.csv')
        csv_roll_mup = os.path.join(output_path, key + '_1d_cont_roll_mup.csv')
        roll_fut_df.to_csv(csv_roll_add,index=False)
        roll_fut_df = apply_roll_adjustment_multiplicative(fut_df)
        roll_fut_df.to_csv(csv_roll_mup,index=False)
        print("download completed for %s! output file saved in %s" % (key, output_path))
        
        gz_csv_raw = output_path + '/' + key + '_1d_cont_raw.csv.gz'
        gz_csv_roll_add = os.path.join(output_path, key + '_1d_cont_roll_add.csv.gz')
        gz_csv_roll_mup = os.path.join(output_path, key + '_1d_cont_roll_mup.csv.gz')
        with open(csv_raw, 'rb') as f_in, gzip.open(gz_csv_raw, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        with open(csv_roll_add, 'rb') as f_in, gzip.open(gz_csv_roll_add, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        with open(csv_roll_mup, 'rb') as f_in, gzip.open(gz_csv_roll_mup, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        print("compressed files for %s saved in %s" % (key, output_path))