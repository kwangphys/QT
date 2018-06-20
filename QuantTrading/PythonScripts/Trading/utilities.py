# -*- coding: utf-8 -*-
"""
Created on Thu Jan 19 09:21:07 2017

@author: matlidia
"""

import numpy as np
import pandas as pd
import datetime as dt
from pandas.io.data import DataReader

#This function loads intraday market data exported by C#, converts time column to datetime format, and switches timezone
def load_intraday_mktdata(filename, data_timezone, strategy_timezone):
    data = pd.read_csv(filename)
    ts = data["time"].tolist()
    ts = [dt.datetime.strptime(t, '%Y-%m-%d %H:%M:%S') for t in ts]
    ts = [data_timezone.localize(t).astimezone(strategy_timezone).replace(tzinfo=None) for t in ts]
    data["time"] = ts
    return data

def load_daily_mktdata(filename):
    data = pd.read_csv(filename)
    ts = data["time"].tolist()
    ts = [dt.datetime.strptime(t, '%Y-%m-%d %H:%M:%S').date() for t in ts]
    data["time"] = ts
    return data
    
def download_pandas_mktdata(symbol, source, start_time, end_time):
    data = DataReader(symbol, source, start_time, end_time)
    df = pd.DataFrame(columns = ['time','open','high','low','close','volume','wap'])
    columns = set(data.columns.values)
    ratio = np.array(data['Adj Close']) / np.array(data['Close']) if 'Adj Close' in columns else np.ones(len(data))
    df['time'] = [dt.datetime.utcfromtimestamp(t.astype('O')/1e9)  for t in data.index.values]
    df['open'] = np.array(data['Open']) * ratio
    df['high'] = np.array(data['High']) * ratio
    df['low'] = np.array(data['Low']) * ratio
    df['close'] = np.array(data['Adj Close']) if 'Adj Close' in columns else np.array(data['Close'])
    df['volume'] = np.array(data['Volume']) / ratio
    df['wap'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4.0
    return df

#This function filter out any data that is outside of [start_time, end_time)
#It assumes the time column is of type datetime
def filter_data_by_time(data, start_time, end_time):
    ret = data
    ret['timeofday'] = [x.time() for x in data['time']]
    if start_time < end_time:
        ret1 = ret[ret['timeofday'] >= start_time]
        ret2 = ret1[ret1['timeofday'] < end_time]
        del ret2['timeofday']
        return ret2
    ret1 = ret[ret['timeofday'] >= start_time]
    ret2 = ret[ret['timeofday'] < end_time]
    ret3 = pd.concat([ret1, ret2])
    del ret3['timeofday']
    return ret3

#This function identifies anomalies based on the given threshold, and fixes its
#values according to nearest normal values
def fix_anomalies(data, anomaly_ths):
    lows = np.array(data["low"])
    highs = np.array(data["high"])
    opens = np.array(data["open"])
    closes = np.array(data["close"])
    times = data["time"].tolist()
    n = len(closes)
    for i in range(1,n-1):
        l = lows[i]
        h = highs[i]
        o = opens[i]
        c = closes[i]
        prev_l = lows[i-1]
        prev_h = highs[i-1]
        prev_o = opens[i-1]
        prev_c = closes[i-1]
        next_l = lows[i+1]
        next_h = highs[i+1]
        next_o = opens[i+1]
        next_c = closes[i+1]
        #handle anomalies
        ths = (prev_h + prev_l) / 2.0 * anomaly_ths
        if o - prev_c > ths and o - next_o > ths:
            ao = 1
        elif o - prev_c < -ths and o - next_o < -ths:
            ao = -1
        else:
            ao = 0
        if c - prev_c > ths and c - next_o > ths:
            ac = 1
        elif c - prev_c < -ths and c - next_o < -ths:
            ac = -1
        else:
            ac = 0
        if ao != 0 or ac != 0:
            r = (abs(prev_c-prev_o) + abs(next_c-next_o))/2.0
            if ao == 0 and ac != 0:
                c = o + r if c > o else o - r
            elif ac == 0 and ao != 0:
                o = c + r if o > c else c - r
            else:
                m = (prev_o + prev_c + next_o + next_c) / 4.0
                if c > o:
                    c = m + r / 2.0
                    o = m - r / 2.0
                else:
                    c = m - r / 2.0
                    o = m + r / 2.0
            if ao != 0:
                print('Abnormal open:', times[i], opens[i], o)
                opens[i] = o
            if ac != 0:
                print('Abnormal close:', times[i], closes[i], c)
                closes[i] = c
        if h - prev_h > ths and h - next_h > ths:
            maxoc = max([o,c])
            prev_h_diff = prev_h - max([prev_o, prev_c])
            next_h_diff = next_h - max([next_o, next_c])
            h = maxoc + (prev_h_diff + next_h_diff) / 2.0
            print('Abnormal high:', times[i], highs[i], h)
            highs[i] = h
        if prev_l - l > ths and next_l - l > ths:
            minoc = min([o,c])
            prev_l_diff = min([prev_o, prev_c]) - prev_l
            next_l_diff = min([next_o, next_c]) - next_l
            l = minoc - (prev_l_diff + next_l_diff) / 2.0
            print('Abnormal low:', times[i], lows[i], l)
            lows[i] = l
    data['open'] = opens
    data['high'] = highs
    data['low'] = lows
    data['close'] = closes
    return data

#This function only applies to intraday data. It identifies all missing bars. 
#If remove=True it will remove data of any date if there is any missing bar
#during the day.
def fix_incomplete_days(data, start_time, end_time, interval, remove=True):
    zero_time = dt.time(0,0,0)
    start_timedelta = dt.datetime.combine(dt.date.min,start_time) - dt.datetime.combine(dt.date.min,zero_time)
    end_time = (dt.datetime.combine(dt.date.today(),end_time) - start_timedelta).time()
    ts = data['time'].tolist()    
    ts = [t-start_timedelta for t in ts]    
    n = len(ts)
    #remove starting incomplete day
    removed = []
    for i in range(n):
        if ts[i].time()==zero_time:
            for rd in removed:
                print('First day is incomplete:', rd)
            break
        d = ts[i].date()
        if remove and len(removed) ==0 or removed[-1] != d:
            removed.append(d)
    if i == 0:
        i = 1
    while i < n:
        if ts[i] - ts[i-1] != interval:
            if ts[i].time() != zero_time:
                if ts[i-1].date() != ts[i].date():
                    print('Start of day is incomplete:', ts[i] + start_timedelta)
                else:
                    print('Middle of day is incomplete:', ts[i] + start_timedelta)
                if remove:
                    removed.append(ts[i].date())
            if ts[i-1].date() != ts[i].date() and (ts[i-1] + interval).time() != end_time:
                print('End of day is incomplete:', ts[i-1] + start_timedelta)
                if remove:
                    removed.append(ts[i-1].date())
        i+=1
    #remove end incomplete day
    if (ts[-1] + interval).time() != end_time:
        print('Last day is incomplete:', ts[i-1] + start_timedelta)
        if remove:
            removed.append(ts[-1].date())
    if remove:            
        removed = set(removed)
        valids = [t.date() not in removed for t in ts]    
        ret = data.copy()
        ret['valid'] = valids
        ret = ret[ret['valid']==True]
        del ret['valid']
        return ret
    
if __name__ == "__main__":
#    raw_data = {'time': [dt.datetime(2017,1,1,1,0,0), dt.datetime(2017,1,1,5,0,0), dt.datetime(2017,1,1,9,0,0), dt.datetime(2017,1,1,16,0,0), dt.datetime(2017,1,1,23,0,0)],
#            'value': [1,2,3,4,5]}
#    df = pd.DataFrame(raw_data, columns = ['time', 'value'])
#    print(df)
#    print(filter_data_by_time(df, dt.time(5,0,0), dt.time(16,0,0)))
#    print(filter_data_by_time(df, dt.time(16,0,0), dt.time(5,0,0)))
#    
    import os
#    import pytz
#    data_zone = pytz.utc
#    strategy_zone = pytz.utc
#    
    data_path = os.path.join(os.path.join(os.environ['QT_MKTDATA_PATH'], 'daily'), 'USStocks')
    symbols = pd.read_csv(os.path.join(data_path, 'symbols.csv'))
#    df = load_intraday_mktdata(os.path.join(data_path, 'XAUUSD_5min_5yr.csv'), data_zone, strategy_zone)
#    df = fix_incomplete_days(df, dt.time(18,0,0), dt.time(17,0,0), dt.timedelta(minutes = 5))
#    df = fix_anomalies(df, 0.02)
#    df.to_csv(os.path.join(data_path, 'XAUUSD_5min_5yr_cleaned.csv'), index=False)
    for symbol in symbols['Symbol']:
        print('Downloading', symbol)
        df = download_pandas_mktdata(symbol, 'yahoo', dt.datetime(2010,1,1), dt.datetime.today())
        if df['time'][0] >= dt.datetime(2017,1,1):
            print(symbol + ":", 'Too few data!')
        else:
            df['time'] = [dt.datetime.strftime(t, '%Y-%m-%d %H:%M:%S') for t in df['time']]
            df.to_csv(os.path.join(data_path, symbol + '.csv'), index = False)
    print('All Done')