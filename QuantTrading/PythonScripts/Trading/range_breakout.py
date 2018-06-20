# -*- coding: utf-8 -*-
"""
Created on Fri Feb 10 15:19:34 2017

@author: Wang Kai
"""

from backtest import backtest
from utilities import fix_anomalies
from run import run_outsampling
import datetime as dt
import numpy as np
import pandas as pd
import os
import pytz

class range_breakout(backtest):
    def __init__(self):
        super(range_breakout, self).__init__()
                
    def initialize(self):
        self.ranges = self.closes - self.opens
        self.is_stagnant = False
        self.maxr = 1e10
        self.minr = 0

    def update_params(self, params):
        #params = (
#            stagnant_lookback,
#            stagnant_ths,
#            anomaly_ths
        self.stagnant_lookback = params[0]
        self.stagnant_ths = params[1]
        fixed_data = fix_anomalies(self.data, params[2])
        self.mids = np.array((fixed_data['open'] + fixed_data['close'])) / 2.0
        self.fixed_highs = np.array(fixed_data['high'])
        self.fixed_lows = np.array(fixed_data['low'])
        
    def on_start_of_day(self):
        pass
            
    def update_stagnants(self,i):
        if i < self.stagnant_lookback:
            return
        #var method
#        hs = self.fixed_highs[i-self.stagnant_lookback:i]
#        ls = self.fixed_lows[i-self.stagnant_lookback:i]
#        avg_range = np.sqrt(np.dot(hs-ls,hs-ls)/self.stagnant_lookback)
#        maxh = np.max(hs)
#        minl = np.min(ls)
#        max_range = maxh - minl
#        ind = avg_range / max_range

        #range method
#        hs = self.fixed_highs[self.idx-self.stagnant_lookback:self.idx]
#        ls = self.fixed_lows[self.idx-self.stagnant_lookback:self.idx]
#        avg_range = np.mean(hs-ls)
#        maxh = np.max(hs)
#        minl = np.min(ls)
#        max_range = maxh - minl
#        ind = avg_range / max_range

        #avg move method
        hs = self.fixed_highs[self.idx-self.stagnant_lookback:self.idx]
        ls = self.fixed_lows[self.idx-self.stagnant_lookback:self.idx]
        maxh = np.max(hs)
        minl = np.min(ls)
        ind = (maxh - minl)/self.stagnant_lookback/np.mean(self.mids[self.idx-self.stagnant_lookback:self.idx])
        if self.is_stagnant:
            max_r = max([maxh,self.maxr])
            min_r = min([minl,self.minr])
        else:
            max_r = maxh
            min_r = minl
        is_stag = ind <= self.stagnant_ths
        return is_stag, max_r, min_r
        
    def should_entry(self):
        self.is_stagnant, self.maxr, self.minr = self.update_stagnants(self.idx)
        if abs(self.opens[self.idx] / self.closes[self.idx-1] - 1.0) > 0.02:
            return None
        #decide if breaks out latest high low
        if self.is_stagnant:
            if self.highs[self.idx] > self.maxr:
                self.entry_idx = self.idx
                return ('STP', 1, max([self.maxr, self.opens[self.idx]]), '')
            if self.lows[self.idx] < self.minr:
                self.entry_idx = self.idx
                return ('STP', -1, min([self.minr, self.opens[self.idx]]), '')
        return None

    def should_exit(self):
        self.is_stagnant, self.maxr, self.minr = self.update_stagnants(self.idx)
        down_ind = (self.fixed_highs[self.idx] - max([self.opens[self.idx],self.closes[self.idx]]))/(self.fixed_highs[self.idx]-self.fixed_lows[self.idx])
        up_ind = (min([self.opens[self.idx],self.closes[self.idx]]) - self.fixed_lows[self.idx])/(self.fixed_highs[self.idx]-self.fixed_lows[self.idx])
        is_up = 1 if self.ranges[self.idx] > 0 else -1
        ths = 0.5
        if down_ind > ths and up_ind <= ths:
            is_up = -1
        elif up_ind > ths and down_ind <= ths:
            is_up = 1

        if self.idx == self.entry_idx:
            if self.position * is_up < 0:
                return ('MKT', self.closes[self.idx], 'Same day close')
        else:        
            if self.position == 1 and self.lows[self.idx] < self.fixed_lows[self.entry_idx]:  
                return ('STP', min([self.fixed_lows[self.entry_idx], self.opens[self.idx]]), '')
            if self.position == -1 and self.highs[self.idx] > self.fixed_highs[self.entry_idx]:       
                return ('STP', max([self.fixed_highs[self.entry_idx], self.opens[self.idx]]), '')

            if self.position * is_up < 0:
                return ('MKT', self.closes[self.idx], '')
        return None

if __name__ == "__main__":
    data_zone = pytz.utc
    strategy_zone = pytz.utc
    symbols = [
           ('XAUUSD_daily_6yr', dt.date(2011,1,1), 0.02),
           ('XAGUSD_daily_6yr', dt.date(2011,1,1), 0.04),
           ('XPTUSD_daily_3yr', dt.date(2014,1,1), 0.02),
           ('EURUSD_daily_7yr', dt.date(2010,1,1), 0.02),
           ('GBPUSD_daily_7yr', dt.date(2010,1,1), 0.02),
           ('USDJPY_daily_7yr', dt.date(2010,1,1), 0.02),
           ('AUDUSD_daily_7yr', dt.date(2010,1,1), 0.02),
           ('NZDUSD_daily_7yr', dt.date(2010,1,1), 0.02),
           ('USDCAD_daily_7yr', dt.date(2010,1,1), 0.02),
           ('USDCHF_daily_7yr', dt.date(2010,1,1), 0.02)
    ]
    print('Started')
    results = []
    result_path = os.path.join(os.environ['QT_MKTDATA_PATH'], 'daily')
    stagnant_lookback = 6
    stagnant_threshold = 0.004

    #single run
    for symbol in symbols:
        test = range_breakout()
        test.set_data(os.path.join(result_path, symbol[0] + '.csv'), 86400, data_zone, strategy_zone)
        test.simulate_daily(symbol[1], dt.date(2017,2,1), [(symbol[1], (stagnant_lookback, stagnant_threshold, symbol[2]))])
        test.detailed_results.to_csv(os.path.join(result_path, symbol[0] + '_details_range_breakout.csv'))
        test.print_summary()
        results.append((symbol[0],test.summary_results[4]))
#        test.plot_all_trades(0, 10)
    for ret in results:
        print(ret[0], ret[1])
    print('Average Score:', np.mean([ret[1] for ret in results]))

    #out-sampling run
#    param_names = ['Stagnant Lookback','Stagnant Thrshold','Anomaly Threshold']
#    for symbol in symbols:
#        param_values = [
#            [4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20],
#            [0.002,0.0025,0.003,0.0035,0.004,0.0045,0.005,0.0055,0.006,0.0065,0.007],
#            [symbol[2]],
#        ]
#        test = range_breakout()
#        test.set_data(os.path.join(result_path, symbol[0] + '.csv'), 86400, data_zone, strategy_zone)
#        
#        run_outsampling(
#            test,
#            symbol[0], 
#            dt.date(2011,1,1),
#            dt.date(2017,2,1),
#            param_names,
#            param_values,
#            730,
#            182,
#            result_path,
#            'range_breakout',
#            [['N Trades Per Day', '>', 0.0]],
#            run_calibration = True
#        )
