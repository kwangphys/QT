using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using IBApi;

namespace IBStrategy
{
    public class MarketDataManager
    {
        private readonly Object _lock = new Object();
        EClientInterface _socket;
        TickerGenerator _tg;
        DBAccess.DBAccess _db;
        Dictionary<int, int> _barTickerMap;
        Dictionary<int, int> _tickerBarMap;
        Dictionary<int, int> _tickTickerMap;
        Dictionary<int, int> _tickerTickMap;
        Dictionary<int, int> _tickerContractMap;
        Dictionary<int, HashSet<int>> _tickerStrategiesMap;

        public MarketDataManager()
        {
            _tg = TickerGenerator.instance;
            _db = DBAccess.DBAccess.instance;
            _barTickerMap = new Dictionary<int, int>();
            _tickerBarMap = new Dictionary<int, int>();
            _tickTickerMap = new Dictionary<int, int>();
            _tickerTickMap = new Dictionary<int, int>();
            _tickerContractMap = new Dictionary<int, int>();
            _tickerStrategiesMap = new Dictionary<int, HashSet<int>>();
        }
        public void setupSocket(EClientInterface socket)
        {
            lock (_lock)
                _socket = socket;
        }
        public int reqRealTimeBars(int strategy_id, Contract con, int bar_size, string whatToShow, string source)
        {
            int bar_id = _db.getBarDataId(con.ConId, whatToShow, bar_size, source);
            if (bar_id == 0)
                bar_id = _db.addBarDataId(con.ConId, whatToShow, bar_size, source, "");
            lock (_lock)
            {
                if (!_barTickerMap.ContainsKey(bar_id))
                {
                    int ticker = _tg.get();
                    _barTickerMap.Add(bar_id, ticker);
                    _tickerBarMap.Add(ticker, bar_id);
                    _tickerContractMap.Add(ticker, con.ConId);
                    _tickerStrategiesMap.Add(ticker, new HashSet<int>(new int[] { strategy_id }));
                    _socket.reqRealTimeBars(ticker, con, bar_size, whatToShow, false, new List<TagValue>());
                }
                else
                {
                    _tickerStrategiesMap[_barTickerMap[bar_id]].Add(strategy_id);
                }
            }
            return _barTickerMap[bar_id];
        }
        public void cancelRealTimeBars(int strategy_id, int ticker)
        {
            if(_tickerStrategiesMap.ContainsKey(ticker))
            {
                lock(_lock)
                {
                    _tickerStrategiesMap[ticker].Remove(strategy_id);
                    if(_tickerStrategiesMap[ticker].Count == 0)
                    {
                        _tickerStrategiesMap.Remove(ticker);
                        _tickerContractMap.Remove(ticker);
                        int bar_id = _tickerBarMap[ticker];
                        _tickerBarMap.Remove(ticker);
                        _barTickerMap.Remove(bar_id);
                        _socket.cancelRealTimeBars(ticker);
                    }
                }
            }
        }
        public int reqMktData(int strategy_id, Contract con, string source)
        {
            int tick_id = _db.getTickDataId(con.ConId, source);
            lock (_lock)
            {
                if (!_tickTickerMap.ContainsKey(tick_id))
                {
                    int ticker = _tg.get();
                    _tickTickerMap.Add(tick_id, ticker);
                    _tickerTickMap.Add(ticker, tick_id);
                    _tickerContractMap.Add(ticker, con.ConId);
                    _tickerStrategiesMap.Add(ticker, new HashSet<int>(new int[] { strategy_id }));
                    _socket.reqMktData(ticker, con, "", false, new List<TagValue>());
                }
                else
                {
                    _tickerStrategiesMap[_tickTickerMap[tick_id]].Add(strategy_id);
                }
            }
            return _tickTickerMap[tick_id];
        }
        public void cancelMktData(int strategy_id, int ticker)
        {
            if (_tickerStrategiesMap.ContainsKey(ticker))
            {
                lock (_lock)
                {
                    _tickerStrategiesMap[ticker].Remove(strategy_id);
                    if (_tickerStrategiesMap[ticker].Count == 0)
                    {
                        _tickerStrategiesMap.Remove(ticker);
                        _tickerContractMap.Remove(ticker);
                        int tick_id = _tickerTickMap[ticker];
                        _tickerTickMap.Remove(ticker);
                        _tickTickerMap.Remove(tick_id);
                        _socket.cancelMktData(ticker);
                    }
                }
            }
        }
        public int getContractID(int ticker)
        {
            return _tickerContractMap.ContainsKey(ticker) ? _tickerContractMap[ticker] : 0;
        }
        public HashSet<int> getStrategyIDs(int ticker)
        {
            return _tickerStrategiesMap.ContainsKey(ticker) ? _tickerStrategiesMap[ticker] : new HashSet<int>();
        }
        //Singleton interface
        private static MarketDataManager _instance;
        public static MarketDataManager instance
        {
            get
            {
                if (_instance == null)
                    _instance = new MarketDataManager();
                return _instance;
            }
        }
    }
}
