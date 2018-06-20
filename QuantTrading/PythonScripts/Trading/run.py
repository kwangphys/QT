# -*- coding: utf-8 -*-
"""
Created on Sun Feb 12 11:11:08 2017

@author: Wang Kai
"""

import os
import datetime as dt
import numpy as np
import pandas as pd
import multiprocessing as mp
from copy import deepcopy
from scipy.ndimage.filters import gaussian_filter
from backtest import backtest

#assume set_data and set_time already called on tester
def run_scenarios(
    tester,
    symbol, 
    start_date,
    end_date,
    param_names,
    param_values,
    result_path,
    result_tag,
    report_measures = ['Sharpe Ratio'],
    report_period = 0,
    verbose = True
):
    cols = param_names + tester.summary_head
    results = pd.DataFrame(columns = cols)
    filename = symbol + '_' + result_tag + '.csv'
    param_lengths = [len(x) for x in param_values]
    n = 1
    for i in range(len(param_lengths)):
        n = n * param_lengths[i]
    for i in range(n):
        params = []
        v = i
        for idx in range(len(param_lengths)):
            l = param_lengths[idx]
            params.append(param_values[idx][v % l])
            v = int(v/l)            
        if tester.interval == 86400:
            params = [(start_date, params)]
        else:
            params = [(dt.datetime.combine(start_date, tester.entry_start_time), params)]            
        print(params[0][0].strftime('%Y%m%d'), params[0][1])
        if tester.interval == 86400:
            tester.simulate_daily(start_date, end_date, params)
        else:
            tester.simulate(start_date, end_date, params)
        if verbose:
            tester.print_summary()
        row = params[0][1] + tester.summary_results
        results.loc[len(results)] = row
        if report_period > 0 and i % report_period == report_period - 1:
            results.to_csv(os.path.join(result_path, filename), index=False)
    results = results.sort_values('Sharpe Ratio', ascending = False)
    results.to_csv(os.path.join(result_path, filename), index=False)
    best_ret = results[param_names + report_measures]
    best_ret = best_ret.sort_values('Sharpe Ratio', ascending = False).loc[0]
    print('=========================================')
    print('Best Result:')
    print(best_ret)    

def run_one_job(params):
    run_scenarios(
        params[0],
        params[1],
        params[2],
        params[3],
        params[4],
        params[5],
        params[6],
        params[7],
        verbose = params[8]
    )

#assume set_data and set_time already called on tester
def run_outsampling(
    tester,
    symbol, 
    start_date,
    end_date,
    param_names,
    param_values,
    calibration_length, #in number of days
    recalibration_period, #in number of days
    result_path,
    result_tag,
    measure_filters,
    run_calibration = True,
    smooth_sharpe_ratio = True,
    take_best_sharpe = False, #when false, it tries to find the best sharpe that is in the neighborhood of last best sharpe
    verbose = False,
    cpu_power = 0.75 #75% default. 0% means no multiprocess
):
    schedule = []        
    if run_calibration:
        calib_start_date = start_date
        calib_end_date = calib_start_date + dt.timedelta(days=calibration_length)
        calib_end_date = min([calib_end_date, end_date])
        calib_run_params = []
        while calib_end_date < end_date:            
            calib_end_date = min([calib_end_date, end_date])
            this_tag = result_tag + '_' + calib_end_date.strftime('%Y%m%d')
            if cpu_power == 0:
                run_scenarios(tester,symbol,calib_start_date,calib_end_date,param_names,param_values,result_path,this_tag,verbose=verbose)
            else:
                calib_run_params.append((deepcopy(tester),symbol,calib_start_date,calib_end_date,param_names,param_values,result_path,this_tag,verbose))
            calib_start_date = calib_start_date + dt.timedelta(days=recalibration_period)
            calib_end_date = calib_start_date + dt.timedelta(days=calibration_length)
        if cpu_power != 0:
            ncpu = mp.cpu_count()
            npool = max([1,min([int(ncpu * cpu_power), ncpu-1])])
            print('Running on', npool, 'cpu cores!')
            p = mp.Pool(npool)
            p.map(run_one_job, calib_run_params)

    calib_start_date = start_date
    calib_end_date = calib_start_date + dt.timedelta(days=calibration_length)
    calib_end_date = min([calib_end_date, end_date])
    calib_df = pd.DataFrame(columns = ['StartDate', 'EndDate'] + param_names + tester.summary_head + ['Smoothed Sharpe Ratio'])
    prev_best_params = None
    while calib_end_date < end_date:            
        calib_end_date = min([calib_end_date, end_date])
        this_tag = result_tag + '_' + calib_end_date.strftime('%Y%m%d')
        filename = symbol + '_' + this_tag + '.csv'
        calibration = pd.read_csv(os.path.join(result_path, filename))
        if smooth_sharpe_ratio:
            append_smoothed_sharpe(calibration, param_names, param_values)
        else:
            calibration['Smoothed Sharpe Ratio'] = calibration['Sharpe Ratio'] * 1.0
        if prev_best_params == None or take_best_sharpe:
            for flt in measure_filters:
                if flt[1] == '>':            
                    calibration = calibration[calibration[flt[0]] > flt[2]]
                elif flt[1] == '<':            
                    calibration = calibration[calibration[flt[0]] < flt[2]]
                elif flt[1] == '>=':            
                    calibration = calibration[calibration[flt[0]] >= flt[2]]
                elif flt[1] == '<=':            
                    calibration = calibration[calibration[flt[0]] <= flt[2]]
                elif flt[1] == '==':            
                    calibration = calibration[calibration[flt[0]] == flt[2]]
                elif flt[1] == '!=':            
                    calibration = calibration[calibration[flt[0]] != flt[2]]
            if len(calibration) == 0:
                raise "No Valid Calibration! Please consider to relax measure filters."
            calibration = calibration.sort_values('Smoothed Sharpe Ratio', ascending = False)
            best_idx = 0
        else:
            calibration = calibration.sort_values('Smoothed Sharpe Ratio', ascending = False)
            best_idx = find_best_neighbor(calibration, prev_best_params, param_names, param_values)                
        params = []
        for name in param_names:
            params.append(calibration.iloc[best_idx][name])
        prev_best_params = params
        if tester.interval == 86400:                    
            schedule.append((calib_end_date, params))
        else:
            schedule.append((dt.datetime.combine(calib_end_date,tester.entry_start_time), params))        
        row = [calib_start_date.strftime('%Y%m%d'), calib_end_date.strftime('%Y%m%d')] + calibration.iloc[best_idx].tolist()
        calib_df.loc[len(calib_df)] = row
        calib_df.to_csv(os.path.join(result_path, symbol + '_' + result_tag + '_calibration.csv'),index=False)        
        
        calib_start_date = calib_start_date + dt.timedelta(days=recalibration_period)
        calib_end_date = calib_start_date + dt.timedelta(days=calibration_length)            

    if tester.interval == 86400:        
        tester.simulate_daily(start_date + dt.timedelta(days=calibration_length), end_date, schedule)
    else:
        tester.simulate(start_date + dt.timedelta(days=calibration_length), end_date, schedule)
    tester.detailed_results.to_csv(os.path.join(result_path, symbol + '_' + result_tag + '_details.csv'),index=False)
    tester.daily_results.to_csv(os.path.join(result_path, symbol + '_' + result_tag + '_daily.csv'),index=False)
    print('Calibration Results:')
    print(calib_df[['StartDate','EndDate','Sharpe Ratio']])
    tester.print_summary()

def append_smoothed_sharpe(df, param_names, param_values):
    filtered_names = []
    filtered_values = []
    indices_map = dict()
    for i in range(len(param_names)):
        if len(param_values[i]) > 1:
            filtered_names.append(param_names[i])
            filtered_values.append(param_values[i])
            for j in range(len(param_values[i])):
                indices_map[(i, param_values[i][j])] = j
    dims = [len(x) for x in filtered_values]
    m = np.zeros(shape=dims)
    tags = []
    for idx in range(len(df)):
        tag = []
        for i in range(len(filtered_names)):
            tag.append(indices_map[(i,df[filtered_names[i]][idx])])
        m.itemset(tuple(tag),df['Sharpe Ratio'][idx])
        tags.append(tuple(tag))
    smoothed = gaussian_filter(m, 1, 0, mode='constant')
    smoothed_sharpes = [smoothed.item(tag) for tag in tags]
    df['Smoothed Sharpe Ratio'] = smoothed_sharpes
    
def find_best_neighbor(df, start_params, param_names, param_values):
    for idx in range(len(df)):
        tag = []
        for name in param_names:
            tag.append(df[name][idx])
        if tag == start_params:
            break
    q = [idx]
    best_idx = idx
    smoothed = np.array(df['Smoothed Sharpe Ratio'])
    best_sharpe = smoothed[idx]
    indices = []
    for i in range(q[0] + 1):
        indices.append(get_tag_index(df, i, param_names, param_values))
        params = []
        for name in param_names:
            params.append(df[name][i])
    while len(q) > 0:
        headi = q[0]
        head_index = indices[headi]
        for i in range(headi):
            this_index = indices[i]
            if np.sum(np.abs(this_index-head_index)) == 1:
                q.append(i)
                if smoothed[i] > best_sharpe:
                    best_sharpe = smoothed[i]
                    best_idx = i
                    
        q.pop(0)
    return best_idx                
            
def get_tag_index(df, idx, param_names, param_values):
    tag_index = []
    for i in range(len(param_names)):
        v = df[param_names[i]][idx]
        tag_index.append(param_values[i].index(v))
    return np.array(tag_index)

if __name__ == "__main__":
    import pytz
    
    class test_strategy(backtest):
        def __init__(self):
            super(test_strategy, self).__init__()
            
        def initialize(self, params):
            self.time1 = params[0]
            self.time2 = params[1]
            self.has_entry = False
        
        def on_start_of_day(self):
            self.has_entry = False
            
        def should_entry(self):            
            if self.currt.time() >= self.time1 and not self.has_entry:
                self.has_entry = True
                return ('MKT', -1, self.closes[self.idx], '')
            return None
        
        def should_exit(self):
            if self.currt.time() >= self.time2:
                return ('MKT', self.closes[self.idx], '')
            return None
            
    test = test_strategy()
    data_zone = pytz.timezone('Europe/London')
    strategy_zone = pytz.timezone('Asia/Singapore')
    test.set_data(os.path.join(os.environ['QT_MKTDATA_PATH'], 'test.csv'), 60, data_zone, strategy_zone)
    test.set_time(dt.time(9,0,0),dt.time(18,0,0),dt.time(21,0,0))

    param_names = ['StartTime', 'EndTime'] 
    param_values = [
        [dt.time(15,28,0), dt.time(15,29,0)], 
        [dt.time(16,28,0), dt.time(16,29,0)]
    ]
    run_scenarios(
        test,
        'COIL', 
        dt.date(2016,12,1),
        dt.date(2016,12,9),
        param_names,
        param_values,
        os.environ['QT_MKTDATA_PATH'],
        'timing'
    )
