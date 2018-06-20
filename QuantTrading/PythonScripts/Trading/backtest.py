# -*- coding: utf-8 -*-
"""
Created on Sun Dec 11 21:06:32 2016

@author: Wang Kai
"""

import datetime as dt
import pandas as pd
import numpy as np
import pytz
import math
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, HourLocator, MinuteLocator
from matplotlib.finance import candlestick_ohlc, date2num
from multiprocessing import Process
from utilities import load_intraday_mktdata, load_daily_mktdata

#This simple backtest engine only works with intraday strategies currently
#It takes 2 steps to set up a run:

#Step 1: implement a strategy inheriting from backtest class. It requires following interfaces:
#   initialize(): set up necessary initialization in this function for the simulation. It will be called in each simulate() run
#   update_params(): set up calibration/adjustable parameters in this function. It will be called on each calibration date in each simulate(). 
#   on_start_of_day(): set up daily routines in this one
#   should_entry(): decide if entry condition is met and returns entry position information. Note that it will only be called when there is not any position, and the engine will disable any entry if it's outside of entry time window, so no need to check entry time window
#   should_exit(): decide if an existing position should exit and returns exit position information. Note that it will only be called when there is a position, and the engine will force close the position at exit end time, so no need to check exit time window

#Step 2: set up a procedure with following steps to run the back test:
#   a. Call backtest.set_data(). It accepts a csv file with format exported from QuantTrading, and converts time from data_zone to strategy_zone as specified
#   b. Call backtest.set_time() to set up daily entry_start_time, entry_end_time and exit_end_time. This step is NOT required for interday simulation
#   c. Prepare initialization parameters and then call backtest.simulate() for intraday simulation and backtest.simulate_daily() for interday simulation
#        The params_schedule in input args should be a list of tuple (calibration_date, calibration_parameters)
#   d. Collect back test results. There are 3 ways to show the results:
#       1. backtest.detailed_results show the details data of each trade entry and exit
#       2. backtest.daily_results shows the daily pnl
#       3. backtest.summary_results show the summary measures of the backtest. You can also combine summary results from multiple runs into a csv to compare the performances
#       4. backtest.plot_all_trades plot bars around each trade entry and exit in a sequential way

#For more detailed information please refer to the example run at the end of this script
    
class backtest:
    def __init__(self, use_pct_return = False):
        self.use_pct_return = use_pct_return
        self.summary_head = ["Total PnL", "N Trading Days", "Annualized Return", "Daily Volatility", "Sharpe Ratio", "Winning Ratio", "Per Trade Return", "N Trades Per Day", "Mean Trade Duration", "Max Drawdown", "Long Ratio"]
        self.details_head = ['EntryTime', 'EntryPrice', 'Position', 'ExitTime', 'ExitPrice', 'EntryType', 'EntryComment', 'ExitType', 'ExitComment', 'PnL', 'CumPnL']
    
    def set_data(self, filename, interval, data_timezone, strategy_timezone):
        self.interval = interval
        if self.interval == 86400:
            self.data = load_daily_mktdata(filename)
        else:
            self.data = load_intraday_mktdata(filename, data_timezone, strategy_timezone)
        self.ts = self.data['time'].tolist()
        self.opens = np.array(self.data["open"])
        self.highs = np.array(self.data["high"])
        self.lows = np.array(self.data["low"])
        self.closes = np.array(self.data["close"])
        self.volumes = np.array(self.data["volume"])
        self.waps = np.array(self.data["wap"])
                   
    def set_time(self, entry_start_time, entry_end_time, exit_end_time):
        self.entry_start_time = entry_start_time
        self.entry_end_time = entry_end_time
        self.exit_end_time = exit_end_time
            
    def initialize(self):
        pass
    
    def update_params(self, params):
        pass        
        
    def on_start_of_day(self):
        pass
    
    def should_entry(self):
        #return (order_type, qty, price, comment)
        pass
    
    def should_exit(self):
        #return (order_type, price, comment)
        pass
    
    def append_extra_details(self):
        #append extra detailed columns into detailed_results
        pass
    
    def simulate(self, start_date, end_date, params_schedule):
        self.figs = []
        self.initialize()
        self.start_date = start_date
        self.end_date = end_date
        self.entry_times = []
        self.entry_prices = []
        self.entry_types = []
        self.entry_comments = []
        self.exit_times = []
        self.exit_prices = []
        self.exit_types = []
        self.exit_comments = []
        self.positions = []
        self.idx = 0
        ts = self.ts
        closes = self.data["close"]
        while ts[self.idx].date() < start_date:
            self.idx += 1
        can_entry = False
        self.position = 0
        self.entry_price = 0
        self.entry_time = None
        n = len(ts)
        t = ts[self.idx]
        self.params_schedule = params_schedule
        self.icalib = -1 if self.params_schedule == None or len(self.params_schedule) == 0 else 0
        started = False
        while self.idx < n and ts[self.idx].date() < end_date:
            t = ts[self.idx]
            self.currt = t
            if t.time() <= self.entry_start_time:
                if self.entry_start_time <= self.exit_end_time or t.time() >= self.exit_end_time:
                    started = False
            if t.time() >= self.entry_start_time and not started:
                if self.icalib != -1 and self.params_schedule[self.icalib][0] <= t:
                    self.update_params(self.params_schedule[self.icalib][1])
                    self.icalib += 1
                    if self.icalib >= len(self.params_schedule):
                        self.icalib = -1
                self.on_start_of_day()
                started = True
            if self.entry_start_time < self.entry_end_time:                
                can_entry = t.time() >= self.entry_start_time and t.time() < self.entry_end_time
            else:
                can_entry = t.time() >= self.entry_start_time or t.time() < self.entry_end_time                
            if can_entry and self.position == 0:
                entry = self.should_entry()
                if entry != None:
                    self.position = entry[1]
                    self.entry_price = entry[2]
                    self.entry_time = t
                    self.entry_times.append(t)
                    self.entry_prices.append(self.entry_price)
                    self.entry_types.append(entry[0])
                    self.entry_comments.append(entry[3])
                    self.positions.append(self.position)
            elif self.position != 0:
                exit_price = 0
                if t.time() >= self.exit_end_time and self.ts[self.idx-1].time() < self.exit_end_time:
                    exit_price = closes[self.idx]
                    exit_comment = "expire"
                    exit_type = "MKT"
                else:
                    exit_order = self.should_exit()
                    if exit_order != None:
                        (exit_type, exit_price, exit_comment) = exit_order
#                    elif t.time() == self.exit_end_time:
#                        exit_price = closes[self.idx]
#                        exit_comment = "expire"
#                        exit_type = "MKT"
                if exit_price != 0:
                    self.position = 0
                    self.exit_times.append(t)
                    self.exit_prices.append(exit_price)
                    self.exit_types.append(exit_type)
                    self.exit_comments.append(exit_comment)
            self.idx += 1
        if len(self.entry_times) == len(self.exit_times) + 1:
            self.entry_times.pop()
            self.entry_prices.pop()
            self.entry_types.pop()
            self.entry_comments.pop()
            self.positions.pop()
        ret = pd.DataFrame()
        ret["EntryTime"] = self.entry_times
        ret["EntryPrice"] = self.entry_prices
        ret["Position"] = self.positions
        ret["ExitTime"] = self.exit_times
        ret["ExitPrice"] = self.exit_prices
        ret["EntryType"] = self.entry_types
        ret["EntryComment"] = self.entry_comments
        ret["ExitType"] = self.exit_types
        ret["ExitComment"] = self.exit_comments
        ret["PnL"] = np.array(self.positions) * (np.array(self.exit_prices) - np.array(self.entry_prices))
        if self.use_pct_return:
            ret['PnL'] = ret['PnL'] / ret['EntryPrice']
        ret["CumPnL"] = ret["PnL"].cumsum()        
        self.detailed_results = ret
        [self.daily_results, self.summary_results] = self.generate_summary(self.detailed_results, self.start_date, self.end_date)
        
    def simulate_daily(self, start_date, end_date, params_schedule):
        self.figs = []
        self.initialize()
        self.start_date = start_date
        self.end_date = end_date
        self.entry_times = []
        self.entry_prices = []
        self.entry_types = []
        self.entry_comments = []
        self.exit_times = []
        self.exit_prices = []
        self.exit_types = []
        self.exit_comments = []
        self.positions = []
        self.idx = 0
        ts = self.ts
        while ts[self.idx] < start_date:
            self.idx += 1
        self.position = 0
        self.entry_price = 0
        self.entry_time = None
        n = len(ts)
        t = ts[self.idx]
        self.params_schedule = params_schedule
        self.icalib = -1 if self.params_schedule == None or len(self.params_schedule) == 0 else 0
        prevd = t + dt.timedelta(days=-1)
        while self.idx < n and ts[self.idx] < end_date:
            t = ts[self.idx]
            self.currt = t
            if t != prevd:
                prevd = t
                if self.icalib != -1 and self.params_schedule[self.icalib][0] <= t:
                    self.update_params(self.params_schedule[self.icalib][1])
                    self.icalib += 1
                    if self.icalib >= len(self.params_schedule):
                        self.icalib = -1
            self.on_start_of_day()
            if self.position == 0:
                entry = self.should_entry()
                if entry != None:
                    self.position = entry[1]
                    self.entry_price = entry[2]
                    self.entry_time = t
                    self.entry_times.append(t)
                    self.entry_prices.append(self.entry_price)
                    self.entry_types.append(entry[0])
                    self.entry_comments.append(entry[3])
                    self.positions.append(self.position)
            if self.position != 0:
                exit_price = 0
                exit_order = self.should_exit()
                if exit_order != None:
                    (exit_type, exit_price, exit_comment) = exit_order
                if exit_price != 0:
                    if self.entry_time == t and exit_price != self.closes[self.idx]:
                        raise "Same day exit price must be close price!"
                    self.position = 0
                    self.exit_times.append(t)
                    self.exit_prices.append(exit_price)
                    self.exit_types.append(exit_type)
                    self.exit_comments.append(exit_comment)
            self.idx += 1
        if len(self.entry_times) == len(self.exit_times) + 1:
            self.entry_times.pop()
            self.entry_prices.pop()
            self.entry_types.pop()
            self.entry_comments.pop()
            self.positions.pop()
        ret = pd.DataFrame()
        ret["EntryTime"] = self.entry_times
        ret["EntryPrice"] = self.entry_prices
        ret["Position"] = self.positions
        ret["ExitTime"] = self.exit_times
        ret["ExitPrice"] = self.exit_prices
        ret["EntryType"] = self.entry_types
        ret["EntryComment"] = self.entry_comments
        ret["ExitType"] = self.exit_types
        ret["ExitComment"] = self.exit_comments
        ret["PnL"] = np.array(self.positions) * (np.array(self.exit_prices) - np.array(self.entry_prices))
        if self.use_pct_return:
            ret['PnL'] = ret['PnL'] / ret['EntryPrice']
        ret["CumPnL"] = ret["PnL"].cumsum()
        self.detailed_results = ret
        [self.daily_results, self.summary_results] = self.generate_summary(self.detailed_results, self.start_date, self.end_date)
        
    def generate_summary(self, ret, startd, endd):
        summary = []
        #Total PnL
        summary.append(np.sum(ret["PnL"]))
        #N Trading Days
        d = startd
        ndays = 0
        while d < endd:
            if d.weekday() != 5 and d.weekday() != 6:
                ndays+=1
            d = d + dt.timedelta(days=1)
        summary.append(ndays)
        daily_ret = pd.DataFrame(columns = ["Date", "PnL", "CumPnL"])

        if len(ret) == 0:
            summary += [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            return [daily_ret, summary]
            
        if self.interval == 86400:
            itrade = 0
            pos = 0
            for idate in range(len(self.ts)):            
                if self.ts[idate] == self.entry_times[itrade]:
                    pos = self.positions[itrade]
                    if self.ts[idate] == self.exit_times[itrade]:
                        if not self.use_pct_return:
                            daily_ret.loc[len(daily_ret)] = [self.ts[idate], (self.exit_prices[itrade] - self.entry_prices[itrade]) * pos, 0]
                        else:
                            daily_ret.loc[len(daily_ret)] = [self.ts[idate], (self.exit_prices[itrade] - self.entry_prices[itrade]) * pos / self.entry_prices[itrade], 0]
                        pos = 0
                        itrade+=1
                        if itrade >= len(self.exit_times):
                            break
                    else:
                        if not self.use_pct_return:
                            daily_ret.loc[len(daily_ret)] = [self.ts[idate], (self.closes[idate] - self.entry_prices[itrade]) * pos, 0]
                        else:
                            daily_ret.loc[len(daily_ret)] = [self.ts[idate], (self.closes[idate] - self.entry_prices[itrade]) * pos / self.entry_prices[itrade], 0]
                elif self.ts[idate] == self.exit_times[itrade]:
                    if not self.use_pct_return:
                        daily_ret.loc[len(daily_ret)] = [self.ts[idate], (self.exit_prices[itrade] - self.closes[idate-1]) * pos, 0]
                    else:
                        daily_ret.loc[len(daily_ret)] = [self.ts[idate], (self.exit_prices[itrade] - self.closes[idate-1]) * pos / self.closes[idate-1], 0]                        
                    pos = 0
                    itrade+=1
                    if itrade >= len(self.exit_times):
                        break
                elif pos != 0:
                    daily_ret.loc[len(daily_ret)] = [self.ts[idate], (self.closes[idate] - self.closes[idate-1]) * pos, 0]
            daily_ret["CumPnL"] = daily_ret["PnL"].cumsum()
        else:
            daily_ret["EntryTime"] = ret["EntryTime"]
            daily_ret["PnL"] = ret["PnL"]
            daily_ret["Date"] = [t if self.interval==86400 else t.date() for t in daily_ret["EntryTime"]]
            del daily_ret['EntryTime']
            daily_ret["CumPnL"] = daily_ret["PnL"].cumsum()
            daily_ret = daily_ret.groupby('Date').aggregate(np.sum)
        #Annualized Return
        summary.append(summary[0]/ndays*252.0)
        #Daily Volatility
        vol = math.sqrt((np.dot(daily_ret["PnL"], daily_ret["PnL"]) - summary[0] * summary[0] / ndays) / (ndays-1.0))
        summary.append(vol)
        #Sharpe Ratio
        summary.append(summary[0]/float(ndays)/vol*math.sqrt(252.0))
        #WinningRatio
        cp = (ret["PnL"] > 0.0).sum()
        cn = (ret["PnL"] < 0.0).sum()
        summary.append(float(cp)/(float(cp) + float(cn) + 1e-10))
        #PerTradeReturn
        summary.append(summary[0]/(float(len(ret))+1e-10))
        #NTradesPerDay
        summary.append(float(len(ret))/(float(ndays)+1e-10))
        #MeanTradeDuration
        summary.append(np.mean(ret["ExitTime"] - ret["EntryTime"]))
        #MaxDrawdown
        maxDown = 0.0
        down = 0.0;
        prevpeak = -1e100;
        cumpnls = daily_ret["CumPnL"]
        for cumpnl in cumpnls:
            if cumpnl > prevpeak:
                if down >= maxDown:
                    maxDown = down
                    down = 0
                prevpeak = cumpnl
            else:
                if prevpeak - cumpnl > down:
                    down = prevpeak - cumpnl
        summary.append(maxDown)
        #LongRatio
        nl = (ret["Position"] > 0.0).sum()
        summary.append(float(nl)/(float(len(ret))+1e-10))
        return [daily_ret, summary]
        
    def print_summary(self):
        if self.summary_results != None:
            summary = self.summary_results
            if self.use_pct_return:
                print("Total PnL:", str(summary[0] * 100) + '%')
            else:
                print("Total PnL:", summary[0])
            print("N Trading Days:", summary[1])
            if self.use_pct_return:
                print("Annualized Return:", str(summary[2] * 100) + '%')
                print("Daily Volatility:", str(summary[3] * 100) + '%')
            else:
                print("Annualized Return:", summary[2])
                print("Daily Volatility:", summary[3])
            print("Sharpe Ratio:", summary[4])
            print("Winning Ratio:", str(summary[5] * 100) + '%')
            if self.use_pct_return:
                print("Per Trade Return:", str(summary[6] * 100) + '%')
            else:
                print("Per Trade Return:", summary[6])
            print("N Trades per Day:", summary[7])
            print("Mean Trade Duration:", summary[8])
            if self.use_pct_return:
                print("Max Drawdown:", str(summary[9] * 100) + '%')
            else:
                print("Max Drawdown:", summary[9])
            print("Long Trade Ratio:", str(summary[10] * 100) + '%')
        
    def prepare_plot_trade_args(self, trade_idx, preoffset_bars, postoffset_bars):
        entry_time = self.detailed_results["EntryTime"][trade_idx]
        entry_idx = self.data[self.data["time"] == entry_time].index.tolist()[0]
        start_idx = max([0, entry_idx - preoffset_bars])
        exit_time = self.detailed_results["ExitTime"][trade_idx]
        exit_idx = self.data[self.data["time"] == exit_time].index.tolist()[0]
        end_idx = min([len(self.data), exit_idx + postoffset_bars + 1])
        ts = self.ts
        opens = self.opens
        highs = self.highs
        lows = self.lows
        closes = self.closes
        d = [(date2num(ts[i]), opens[i], highs[i], lows[i], closes[i]) for i in range(start_idx, end_idx)]
        position = self.detailed_results["Position"][trade_idx]
        arrow_color = 'green' if position > 0 else 'red'
        entry_price = self.detailed_results["EntryPrice"][trade_idx]
        exit_price = self.detailed_results["ExitPrice"][trade_idx]
        return (d, self.interval, (date2num(entry_time), entry_price), (date2num(exit_time), exit_price), arrow_color, "Trade #" + str(trade_idx))
        
    def plot_trade(self, trade_idx, preoffset_bars, postoffset_bars):
        args = self.prepare_plot_trade_args(trade_idx, preoffset_bars, postoffset_bars)
        plot_trade_func(args[0], args[1], args[2], args[3], args[4], args[5])

    def plot_all_trades(self, preoffset_bars, postoffset_bars):
        ps = []
        for i in range(len(self.detailed_results)):
            p = Process(target=plot_trade_func, args=self.prepare_plot_trade_args(i,preoffset_bars,postoffset_bars))
            p.start()
            ps.append(p)            
            print('[1]Close Last Plot and Continue')
            print('[2]Close All Plots and Continue')
            print('[3]Keep All Plots and Continue')
            print('[4]Exit')
            decision=input("Choose one from [1/2/3/4]:")
            if decision == '1':
                if ps[-1].is_alive():
                    ps[-1].terminate()
                ps.remove(p)
            elif decision == '2' or decision == '4':
                for pp in ps:
                    if pp.is_alive():
                        pp.terminate()
                ps = []
                if decision == '4':
                    break
        
def plot_trade_func(data, interval, arrow_start, arrow_end, arrow_color, title):
        fig, ax = plt.subplots()
        fig.canvas.set_window_title(title)
        fig.subplots_adjust(bottom=0.2) 
        total_minutes = len(data) * interval / 60
        bin_minutes = total_minutes / 10.0
        if bin_minutes <= 60:
            if bin_minutes <= 1:
                bin_minutes = 1
            elif bin_minutes <= 5:
                bin_minutes = 5
            elif bin_minutes <= 10:
                bin_minutes = 10
            elif bin_minutes <= 15:
                bin_minutes = 15
            elif bin_minutes <= 20:
                bin_minutes = 20
            elif bin_minutes <= 30:
                bin_minutes = 30
            elif bin_minutes <= 60:
                bin_minutes = 60
            locator = MinuteLocator(range(0,60,bin_minutes))
        else:
            if bin_minutes <= 90:
                bin_minutes = 90
            elif bin_minutes <= 120:
                bin_minutes = 120
            elif bin_minutes <= 180:
                bin_minutes = 180
            elif bin_minutes <= 240:
                bin_minutes = 240
            elif bin_minutes <= 360:
                bin_minutes = 360
            else:
                bin_minutes = 480
            locator = HourLocator(range(0,24,bin_minutes/60))
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(DateFormatter("%H:%M:%S"))
        candlestick_ohlc(ax, data, width=interval / 60.0 * 0.0005, colorup='g')
        ax.annotate("", xy=arrow_end, xytext=arrow_start,
                    arrowprops=dict(arrowstyle="-|>", connectionstyle="arc3", color=arrow_color, linestyle='dashed'))
        ax.xaxis_date()
        ax.autoscale_view()
        plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
        plt.grid()
        plt.show()    

if __name__ == "__main__":
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
            
    import os
    test = test_strategy()
    data_zone = pytz.timezone('Europe/London')
    strategy_zone = pytz.timezone('Asia/Singapore')
    test.set_data(os.path.join(os.environ['QT_MKTDATA_PATH'], 'test.csv'), 60, data_zone, strategy_zone)
    test.set_time(dt.time(9,0,0),dt.time(18,0,0),dt.time(21,0,0))

    #Single run
    params = (dt.time(15,29,0), dt.time(16,29,0))
    test.simulate(dt.date(2016,12,1), dt.date(2016,12,9), params)
    print(test.detailed_results)
    test.print_summary()
    test.plot_all_trades(30, 30)
