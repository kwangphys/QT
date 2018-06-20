using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Xml;
using DBAccess;
using IBApi;

namespace IBStrategy
{
    public class BacktestEngine : EClientInterface
    {
        EWrapperImpl _wrapper;
        DBAccess.DBAccess _db;
        ThreadManager _tm;
        XmlNode _config;
        Dictionary<int, Bar[]> _mktData;
        string _account;
        DateTime _startDate;
        DateTime _endDate;
        string _barType;
        int _minBarSize;
        TimeSpan _interval;
        string _source;
        string _reportFolder;
        int _slippage;
        bool _limitIncludeTouch;
        protected readonly Object _lock = new Object();
        private static DateTime baseTime = new DateTime(1970, 1, 1, 0, 0, 0);
        private static HashSet<string> supportedOrderTypes = new HashSet<string>(new string[] { "MKT", "LMT", "STP", "STP PRT", "STP LMT" });

        //internal states
        DateTime _currTime; //local time
        DateTime _currd;
        Dictionary<int, int> _currMktIndices;
        List<int> _barIds;
        Dictionary<int, int> _barTickerMap;
        Dictionary<int, int> _tickTickerMap;
        Dictionary<int, TimeZoneInfo> _mktTimeZoneMap;
        Dictionary<int, int> _contractBarMap;
        Dictionary<int, Contract> _orderContractMap;
        Dictionary<int, double> _barSlippageMap;
        SortedDictionary<int, Order> _liveOrders;
        Dictionary<string, List<int>> _ocaGroups;
        Dictionary<int, List<int>> _childOrders;
        SortedDictionary<int, Execution> _executions;
        SortedDictionary<int, List<onTime>> _timeEventHandlers;
        Dictionary<int, bool> _timeEventRefreshed;
        Dictionary<int, bool> _isDone;
        Dictionary<int, AutoResetEvent> _sleepEvents;
        ManualResetEvent _nextBarEvent;
        int _sleepId;

        public BacktestEngine(EWrapperImpl wrapper, DBAccess.DBAccess db, XmlNode config)
        {
            _wrapper = wrapper;
            _db = db;
            _tm = ThreadManager.instance;
            lock (_lock)
            {
                _wrapper.nextOrderId = DBAccess.DBAccess.instance.getMaxOrderId() + 1;
            }
            _config = config;
            _account = config["Account"].InnerText;
            _startDate = DateTime.Parse(config["StartDate"].InnerText);
            string endDateStr = config["EndDate"].InnerText;
            if (endDateStr == "Today")
                _endDate = DateTime.Today;
            else
                _endDate = DateTime.Parse(endDateStr);
            _barType = config["BarType"].InnerText;
            _minBarSize = Int32.Parse(config["MinBarSize"].InnerText);
            _slippage = Int32.Parse(config["Slippage"].InnerText);
            _limitIncludeTouch = Boolean.Parse(config["LimitIncludeTouch"].InnerText);
            _source = config["MarketDataSource"].InnerText;
            _reportFolder = config["ReportFolder"].InnerText;
            _interval = new TimeSpan(0, 0, _minBarSize);
            _mktData = new Dictionary<int, Bar[]>();
            _currMktIndices = new Dictionary<int, int>();
            _barIds = new List<int>();
            _barTickerMap = new Dictionary<int, int>();
            _tickTickerMap = new Dictionary<int, int>();
            _mktTimeZoneMap = new Dictionary<int, TimeZoneInfo>();
            _contractBarMap = new Dictionary<int, int>();
            _orderContractMap = new Dictionary<int, Contract>();
            _barSlippageMap = new Dictionary<int, double>();
            _currTime = _startDate;
            _currd = _currTime.Date;
            _liveOrders = new SortedDictionary<int, Order>();
            _ocaGroups = new Dictionary<string, List<int>>();
            _childOrders = new Dictionary<int, List<int>>();
            _executions = new SortedDictionary<int, Execution>();
            _timeEventHandlers = new SortedDictionary<int, List<onTime>>();
            _timeEventRefreshed = new Dictionary<int, bool>();
            _isDone = new Dictionary<int, bool>();
            _sleepId = 0;
            _sleepEvents = new Dictionary<int, AutoResetEvent>();
            _nextBarEvent = new ManualResetEvent(true);
        }
        //Connection and Server
        public void eDisconnect()
        {
            throw new NotImplementedException();
        }
        public void setServerLogLevel(int logLevel)
        {
            throw new NotImplementedException();
        }
        public void reqCurrentTime()
        {
            throw new NotImplementedException();
        }
        //Market Data
        public void reqMktData(int tickerId, Contract contract, string genericTickList, bool snapshot, List<TagValue> mktDataOptions)
        {
            int barId = _db.getBarDataId(contract.ConId, _barType, _minBarSize, _source);
            loadMktData(barId, _minBarSize);
            if(snapshot)
            {
                Bar bar = _mktData[barId][_currMktIndices[barId]];
                _wrapper.tickPrice(tickerId, 1, bar.close, 1);
                _wrapper.tickPrice(tickerId, 2, bar.close, 1);
                _wrapper.tickPrice(tickerId, 4, bar.close, 1);
                _wrapper.tickPrice(tickerId, 6, bar.high, 0);
                _wrapper.tickPrice(tickerId, 7, bar.low, 0);
                _wrapper.tickPrice(tickerId, 9, bar.close, 1);
            }
            else
            {
                lock (_lock)
                {
                    _tickTickerMap.Add(tickerId, barId);
                    if (_contractBarMap.ContainsKey(contract.ConId))
                        _contractBarMap[contract.ConId] = barId;
                    else
                        _contractBarMap.Add(contract.ConId, barId);
                    if (!_mktTimeZoneMap.ContainsKey(barId))
                    {
                        ContractDetails details = _db.getContractDetails(contract);
                        _mktTimeZoneMap.Add(barId, Strategy.mapDotNetTimeZone(details.TimeZoneId));
                        _barSlippageMap.Add(barId, _slippage * details.MinTick);
                    }
                }
            }
        }
        public void cancelMktData(int tickerId)
        {
            if (_tickTickerMap.ContainsKey(tickerId))
            {
                int barId = _tickTickerMap[tickerId];
                lock (_lock)
                {
                    _mktTimeZoneMap.Remove(barId);
                    _barSlippageMap.Remove(barId);
                    _mktData.Remove(barId);
                    _currMktIndices.Remove(barId);
                    _isDone.Remove(barId);
                    _barIds = _currMktIndices.Keys.ToList();
                    _tickTickerMap.Remove(tickerId);
                }
            }
        }
        public void calculateImpliedVolatility(int reqId, Contract contract, double optionPrice, double underPrice, List<TagValue> impliedVolatilityOptions)
        {
            throw new NotImplementedException();
        }
        public void cancelCalculateImpliedVolatility(int reqId)
        {
            throw new NotImplementedException();
        }
        public void calculateOptionPrice(int reqId, Contract contract, double volatility, double underPrice, List<TagValue> optionPriceOptions)
        {
            throw new NotImplementedException();
        }
        public void cancelCalculateOptionPrice(int reqId)
        {
            throw new NotImplementedException();
        }
        public void reqMarketDataType(int marketDataType)
        {
            throw new NotImplementedException();
        }
        //Orders
        public void placeOrder(int id, Contract contract, Order order)
        {
            if (!supportedOrderTypes.Contains(order.OrderType))
            {
                _wrapper.error("Unsupported order type " + order.OrderType + " for order " + order.OrderId.ToString());
                return;
            }
            if (order.Tif == null)
                order.Tif = "DAY";
            order.Account = _account;
            if (order.OrderType != "LMT")
                order.LmtPrice = 0;
            if (order.OrderType != "STP" && order.OrderType != "STP PRT" && order.OrderType != "STP LMT")
                order.AuxPrice = 0;
            if (order.OcaGroup == null && order.ParentId != 0)
                order.OcaGroup = order.ParentId.ToString();
            lock (_lock)
            {
                if (_liveOrders.ContainsKey(order.OrderId))
                {
                    _liveOrders[order.OrderId] = order;
                }
                else
                {
                    _liveOrders.Add(order.OrderId, order);
                    _orderContractMap.Add(order.OrderId, contract);
                    if (order.OcaGroup != null)
                    {
                        if (!_ocaGroups.ContainsKey(order.OcaGroup))
                            _ocaGroups.Add(order.OcaGroup, new List<int>());
                        _ocaGroups[order.OcaGroup].Add(order.OrderId);
                    }
                    if (order.ParentId > 0)
                    {
                        if (!_childOrders.ContainsKey(order.ParentId))
                            _childOrders.Add(order.ParentId, new List<int>());
                        _childOrders[order.ParentId].Add(order.OrderId);
                    }
                }
            }
            _wrapper.openOrder(order.OrderId, contract, order, new OrderState("Submitted", "", "", "", 0, 0, 0, contract.Currency, ""));
            _wrapper.orderStatus(order.OrderId, "Submitted", 0, order.TotalQuantity, 0, 0, order.ParentId, 0, 0, "");
            _wrapper.openOrderEnd();
            //tryExecuteOrder(contract, order);
        }
        public void cancelOrder(int orderId)
        {
            // cancel its child orders if any
            if (_childOrders.ContainsKey(orderId))
            {
                foreach (int id in _childOrders[orderId])
                {
                    cancelSingleOrder(id);
                }
                _childOrders.Remove(orderId);
            }
            cancelSingleOrder(orderId);
        }
        private void cancelSingleOrder(int orderId)
        {
            if (_liveOrders.ContainsKey(orderId))
            {
                lock (_lock)
                {
                    int parentId = _liveOrders[orderId].ParentId;
                    string ocaKey = _liveOrders[orderId].OcaGroup;
                    _liveOrders.Remove(orderId);
                    _orderContractMap.Remove(orderId);
                    if (ocaKey != null && _ocaGroups.ContainsKey(ocaKey))
                    {
                        _ocaGroups[ocaKey].Remove(orderId);
                        if (_ocaGroups[ocaKey].Count() == 0)
                            _ocaGroups.Remove(ocaKey);
                    }
                    _wrapper.orderStatus(orderId, "Cancelled", 0, 0, 0, 0, parentId, 0, 0, "");
                }
            }
        }
        public void reqOpenOrders()
        {
            throw new NotImplementedException();
        }
        public void reqAllOpenOrders()
        {
            _wrapper.openOrderEnd();
        }
        public void reqAutoOpenOrders(bool autoBind)
        {
            throw new NotImplementedException();
        }
        public void reqIds(int numIds)
        {
            throw new NotImplementedException();
        }
        public void exerciseOptions(int tickerId, Contract contract, int exerciseAction, int exerciseQuantity, string account, int ovrd)
        {
            throw new NotImplementedException();
        }
        public void reqGlobalCancel()
        {
            throw new NotImplementedException();
        }
        //Account and Portfolio
        public void reqAccountUpdates(bool subscribe, string acctCode)
        {
        }
        public void reqAccountSummary(int reqId, string group, string tags)
        {
            throw new NotImplementedException();
        }
        public void cancelAccountSummary(int reqId)
        {
            throw new NotImplementedException();
        }
        public void reqAccountUpdatesMulti(int requestId, string account, string modelCode, bool ledgerAndNLV)
        {
            throw new NotImplementedException();
        }
        public void cancelAccountUpdatesMulti(int requestId)
        {
            throw new NotImplementedException();
        }
        public void reqPositions()
        {
            throw new NotImplementedException();
        }
        public void cancelPositions()
        {
            throw new NotImplementedException();
        }
        public void reqPositionsMulti(int requestId, string account, string modelCode)
        {
            throw new NotImplementedException();
        }
        public void cancelPositionsMulti(int requestId)
        {
            throw new NotImplementedException();
        }
        //Executions
        public void reqExecutions(int reqId, ExecutionFilter filter)
        {
            throw new NotImplementedException();
        }
        //Contract Details
        public void reqContractDetails(int reqId, Contract contract)
        {
            ContractDetails details = _db.getContractDetails(contract);
            _wrapper.contractDetails(reqId, details);
            _wrapper.contractDetailsEnd(reqId);
        }
        //Market Depth
        public void reqMarketDepth(int tickerId, Contract contract, int numRows, List<TagValue> mktDepthOptions)
        {
            throw new NotImplementedException();
        }
        public void cancelMktDepth(int tickerId)
        {
            throw new NotImplementedException();
        }
        //News Bulletins
        public void reqNewsBulletins(bool allMessages)
        {
            throw new NotImplementedException();
        }
        public void cancelNewsBulletin()
        {
            throw new NotImplementedException();
        }
        //Financial Advisors
        public void reqManagedAccts()
        {
            _wrapper.managedAccounts(_account);
        }
        public void requestFA(int faDataType)
        {
            throw new NotImplementedException();
        }
        public void replaceFA(int faDataType, string xml)
        {
            throw new NotImplementedException();
        }
        //Market Scanners
        public void reqScannerParameters()
        {
            throw new NotImplementedException();
        }
        public void reqScannerSubscription(int reqId, ScannerSubscription subscription, List<TagValue> scannerSubscriptionOptions)
        {
            throw new NotImplementedException();
        }
        public void cancelScannerSubscription(int tickerId)
        {
            throw new NotImplementedException();
        }
        //Historical Data
        public void reqHistoricalData(int tickerId, Contract contract, string endDateTime,
            string durationString, string barSizeSetting, string whatToShow, int useRTH, int formatDate, List<TagValue> chartOptions)
        {
            throw new NotImplementedException();
        }
        public void cancelHistoricalData(int reqId)
        {
            throw new NotImplementedException();
        }
        //Real Time Bars
        public void reqRealTimeBars(int tickerId, Contract contract, int barSize, string whatToShow, bool useRTH, List<TagValue> realTimeBarsOptions)
        {
            if (useRTH)
                throw new NotImplementedException();
            if (barSize == 0)
                barSize = _minBarSize;
            int barId = _db.getBarDataId(contract.ConId, whatToShow, barSize, _source);
            if (!_mktTimeZoneMap.ContainsKey(barId))
            {
                ContractDetails cd = _db.getContractDetails(contract);
                lock (_lock)
                {
                    _mktTimeZoneMap.Add(barId, Strategy.mapDotNetTimeZone(cd.TimeZoneId));
                    _barSlippageMap.Add(barId, _slippage * cd.MinTick);
                }
            }
            loadMktData(barId, barSize);
            lock (_lock)
            {
                _barTickerMap.Add(tickerId, barId);
                if (_contractBarMap.ContainsKey(contract.ConId))
                    _contractBarMap[contract.ConId] = barId;
                else
                    _contractBarMap.Add(contract.ConId, barId);
            }
        }
        public void cancelRealTimeBars(int tickerId)
        {
            if (_barTickerMap.ContainsKey(tickerId))
            {
                int barId = _barTickerMap[tickerId];
                lock (_lock)
                {
                    _mktTimeZoneMap.Remove(barId);
                    _barSlippageMap.Remove(barId);
                    _mktData.Remove(barId);
                    _currMktIndices.Remove(barId);
                    _isDone.Remove(barId);
                    _barIds = _currMktIndices.Keys.ToList();
                    _barTickerMap.Remove(tickerId);
                }
            }
        }
        //Fundamental Data
        public void reqFundamentalData(int reqId, Contract contract, String reportType, List<TagValue> fundamentalDataOptions)
        {
            throw new NotImplementedException();
        }
        public void cancelFundamentalData(int reqId)
        {
            throw new NotImplementedException();
        }
        //Display Groups
        public void queryDisplayGroups(int requestId)
        {
            throw new NotImplementedException();
        }
        public void subscribeToGroupEvents(int requestId, int groupId)
        {
            throw new NotImplementedException();
        }
        public void updateDisplayGroup(int requestId, string contractInfo)
        {
            throw new NotImplementedException();
        }
        public void unsubscribeFromGroupEvents(int requestId)
        {
            throw new NotImplementedException();
        }
        //Other
        public void reqSecDefOptParams(int reqId, string underlyingSymbol, string futFopExchange, string underlyingSecType, int underlyingConId)
        {
            throw new NotImplementedException();
        }
        //User Defined
        public void subscribeTimerEvent(TimeSpan t, onTime handler)
        {
            int nt = (int)(t.TotalMinutes);
            lock (_lock)
            {
                if (!_timeEventHandlers.ContainsKey(nt))
                    _timeEventHandlers.Add(nt, new List<onTime>());
                _timeEventHandlers[nt].Add(handler);
                _timeEventRefreshed[nt] = true;
            }
        }
        public DateTime getCurrentLocalTime()
        {
            return _currTime;
        }
        public void sleepUntil(DateTime expiry, ManualResetEvent blockEvent)
        {
            int id;
            lock (_lock)
            {
                _sleepEvents.Add(_sleepId, new AutoResetEvent(false));
                id = _sleepId++;
            }
            while (expiry > _currTime)
            {
                blockEvent.Set();
                _nextBarEvent.WaitOne();
                blockEvent.Reset();
                _sleepEvents[id].Set();
            }
            blockEvent.Reset();
            lock (_lock)
            {
                _sleepEvents[id].Set();
                _sleepEvents[id].Close();
                _sleepEvents.Remove(id);
            }
            return;
        }
        //Kernel
        public void run()
        {
            Stopwatch sw = new Stopwatch();
            reqManagedAccts();
            List<int> nts = _timeEventRefreshed.Keys.ToList();
            nts.Sort();
            Console.WriteLine("Backtest for account " + _account + " starting...");
            sw.Start();
            Console.WriteLine(_currTime.Date.ToString());
            while (_currTime < _endDate)
            {
                //Handling time events
                if (_currd != _currTime.Date)
                {
                    lock (_lock)
                    {
                        foreach (int nt in nts)
                            _timeEventRefreshed[nt] = true;
                        _currd = _currTime.Date;
                    }
                    Console.WriteLine(_currd.ToString());
                }
                foreach (int nt in nts)
                {
                    if (_timeEventRefreshed[nt] && _currTime.TimeOfDay.TotalMinutes >= nt)
                    {
                        lock (_lock)
                        {
                            _timeEventRefreshed[nt] = false;
                        }
                        foreach (onTime handler in _timeEventHandlers[nt])
                            handler();
                    }
                }
                //Update current bars
                foreach (int barId in _barIds)
                {
                    if (!_isDone[barId])
                    {
                        int currId = _currMktIndices[barId];
                        Bar[] bars = _mktData[barId];
                        while (Strategy.getLocalTime(bars[currId].time, _mktTimeZoneMap[barId]) < _currTime)
                        {
                            currId++;
                            if (currId == bars.Count())
                            {
                                lock (_lock)
                                {
                                    _isDone[barId] = true;
                                }
                                break;
                            }
                        }
                        if (!_isDone[barId])
                            lock (_lock)
                            {
                                _currMktIndices[barId] = currId;
                            }
                    }
                }
                DateTime minTime = DateTime.MaxValue;
                foreach(var pair in _currMktIndices)
                {
                    if (!_isDone[pair.Key])
                    {
                        DateTime endTime = Strategy.getLocalTime(_mktData[pair.Key][pair.Value].time, _mktTimeZoneMap[pair.Key]);
                        if (endTime < minTime)
                            minTime = endTime;
                    }
                }
                lock(_lock)
                {
                    if(minTime != DateTime.MaxValue)
                        _currTime = minTime;
                }
                _nextBarEvent.Set();
                //Handling time events
                if (_currd != _currTime.Date)
                {
                    lock (_lock)
                    {
                        foreach (int nt in nts)
                            _timeEventRefreshed[nt] = true;
                        _currd = _currTime.Date;
                    }
                    Console.WriteLine(_currd.ToString());
                }
                foreach (int nt in nts)
                {
                    if (_timeEventRefreshed[nt] && _currTime.TimeOfDay.TotalMinutes >= nt)
                    {
                        lock (_lock)
                        {
                            _timeEventRefreshed[nt] = false;
                        }
                        foreach (onTime handler in _timeEventHandlers[nt])
                            handler();
                    }
                }
                //Sending market data
                foreach (var pair in _tickTickerMap)
                {
                    int barId = pair.Value;
                    if (!_isDone[barId])
                    {
                        Bar bar = _mktData[barId][_currMktIndices[barId]];
                        if (Strategy.getLocalTime(bar.time, _mktTimeZoneMap[barId]) == _currTime)
                        {
                            int tickerId = pair.Key;
                            _wrapper.tickPrice(tickerId, 1, bar.low, 1);
                            _wrapper.tickPrice(tickerId, 2, bar.high, 1);
                            _wrapper.tickPrice(tickerId, 4, bar.open, 1);
                            _wrapper.tickPrice(tickerId, 6, bar.high, 0);
                            _wrapper.tickPrice(tickerId, 7, bar.low, 0);
                            _wrapper.tickPrice(tickerId, 9, bar.close, 1);
                        }
                    }
                }
                foreach(var pair in _barTickerMap)
                {
                    int barId = pair.Value;
                    if (!_isDone[barId])
                    {
                        Bar bar = _mktData[barId][_currMktIndices[barId]];
                        DateTime localTime = Strategy.getLocalTime(bar.time, _mktTimeZoneMap[barId]);
                        if (localTime == _currTime)
                        {
                            int tickerId = pair.Key;
                            _wrapper.realtimeBar(tickerId, (long)((localTime.ToUniversalTime() - baseTime).TotalSeconds), bar.open, bar.high, bar.low, bar.close, (long)(bar.volume), bar.wap, 0);
                        }
                    }
                }
                //Wait for all relevant threads
                lock (_lock)
                {
                    foreach (var pair in _sleepEvents)
                        pair.Value.WaitOne();
                }
                _nextBarEvent.Reset();
                foreach (var pair in _tm.Events)
                    pair.Value.WaitOne();
                //Execute orders
                List<int> orderIds = _liveOrders.Keys.ToList();
                foreach (var orderId in orderIds)
                    if(_liveOrders.ContainsKey(orderId))
                        tryExecuteOrder(_orderContractMap[orderId], _liveOrders[orderId]);
                _currTime = _currTime.AddSeconds(_minBarSize);
            }
            Console.WriteLine("Backtest done!");
            DBUtils.generatePnLReport(_account, _startDate, _endDate, _reportFolder);
            string path = Path.Combine(_reportFolder, _account);
            using (StringWriter writer = new StringWriter())
            {
                using (var xw = new System.Xml.XmlTextWriter(writer))
                {
                    xw.Formatting = System.Xml.Formatting.Indented;
                    xw.Indentation = 4;
                    _config.WriteContentTo(xw);
                }
                string configFilename = Path.Combine(path, "AccountConfig.xml");
                File.WriteAllText(configFilename, writer.ToString());
            }
            sw.Stop();
            Console.WriteLine("Time Elapsed: " + sw.Elapsed.TotalSeconds.ToString() + "sec" );
        }
        //Internal functions
        private void loadMktData(int barId, int barLength)
        {
            if (!_mktData.ContainsKey(barId))
            {
                DataTable dt = _db.getHistoricalData(barId, Strategy.getExchangeTime(_currTime, _mktTimeZoneMap[barId]), Strategy.getLocalTime(_endDate, _mktTimeZoneMap[barId]));
                Bar[] bars = dt.Rows.Cast<DataRow>().Select(x => new Bar(barId, x)).Select(x => { x.time = x.time.AddSeconds(barLength); return x; }).ToArray();
                lock (_lock)
                {
                    _mktData.Add(barId, bars);
                    _currMktIndices.Add(barId, 0);
                    _barIds = _currMktIndices.Keys.ToList();
                    _isDone.Add(barId, false);
                }
            }
        }
        private bool tryExecuteOrder(Contract contract, Order order)
        {
            int barId = _contractBarMap[contract.ConId];
            if (_isDone[barId])
                return false;
            if (order.ParentId != 0 && _liveOrders.ContainsKey(order.ParentId))
                return false;
            Bar b = _mktData[barId][_currMktIndices[barId]];
            if (b.volume == 0)
                return false;
            double price = 0;
            if (order.OrderType == "MKT")
            {
                if (order.GoodAfterTime != null && _currTime < DateTime.ParseExact(order.GoodAfterTime, "yyyyMMdd HH:mm:ss", CultureInfo.InvariantCulture))
                    return false;
                price = b.close + (order.Action.Equals("BUY") ? _barSlippageMap[barId] / 2.0 : -_barSlippageMap[barId] / 2.0);
            }
            else if (order.OrderType == "LMT")
            {
                if (_limitIncludeTouch)
                    if ((order.Action.Equals("BUY") && b.low > order.LmtPrice) || (order.Action.Equals("SELL") && b.high < order.LmtPrice))
                        return false;
                    else
                    if ((order.Action.Equals("BUY") && b.low >= order.LmtPrice) || (order.Action.Equals("SELL") && b.high <= order.LmtPrice))
                        return false;
                price = order.LmtPrice;
            }
            else if (order.OrderType == "STP" || order.OrderType == "STP PRT" || order.OrderType == "STP LMT")
            {
                if ((order.Action.Equals("BUY") && b.high < order.AuxPrice) || (order.Action.Equals("SELL") && b.low > order.AuxPrice))
                    return false;
                price = order.AuxPrice;
            }
            else
                return false;   //Or throw not supported error?
            Execution e = new Execution(order.OrderId, 0, order.OrderId.ToString(), _currTime.ToString("yyyyMMdd  HH:mm:ss"), _account, contract.Exchange, order.Action.Equals("BUY") ? "BOUGHT" : "SLD", order.TotalQuantity, price, 0, 0, (int)(order.TotalQuantity), price, "", "", 1, "");
            lock (_lock)
            {
                if (_liveOrders.ContainsKey(order.OrderId))
                {
                    _executions.Add(order.OrderId, e);
                    _liveOrders.Remove(order.OrderId);
                    _orderContractMap.Remove(order.OrderId);
                    _wrapper.orderStatus(order.OrderId, "Filled", order.TotalQuantity, 0, price, 0, order.ParentId, price, 0, "");
                    _wrapper.execDetails(0, contract, e);
                    if (order.OcaGroup != null)
                    {
                        foreach (int id in _ocaGroups[order.OcaGroup])
                        {
                            if (id != order.OrderId)
                            {
                                _liveOrders.Remove(id);
                                _wrapper.orderStatus(id, "Cancelled", 0, 0, 0, 0, order.ParentId, 0, 0, "");
                                // cancel its child orders if any
                                if (_childOrders.ContainsKey(id))
                                {
                                    foreach (int cid in _childOrders[id])
                                    {
                                        _liveOrders.Remove(cid);
                                        _wrapper.orderStatus(cid, "Cancelled", 0, 0, 0, 0, id, 0, 0, "");
                                    }
                                    _childOrders.Remove(id);
                                }
                            }
                        }
                        _ocaGroups.Remove(order.OcaGroup);
                    }
                    return true;
                }
            }
            return false;
        }
    }
}
