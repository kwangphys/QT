using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;
using IBApi;
using Utils;

namespace IBStrategy
{
    public struct ErrorParameters
    {
        public int reqId;
        public int errorCode;
        public string errorMsg;
        public ErrorParameters(int ReqId, int ErrorCode, string ErrorMsg)
        {
            reqId = ReqId;
            errorCode = ErrorCode;
            errorMsg = ErrorMsg;
        }
    }
    public struct HistoricalDataParameters
    {
        public int reqId;
        public DateTime date;
        public double open;
        public double high;
        public double low;
        public double close;
        public int volume;
        public double wap;
        public HistoricalDataParameters(int ReqId, DateTime Date, double Open, double High, double Low, double Close, int Volume, double Wap)
        {
            reqId = ReqId;
            date = Date;
            open = Open;
            high = High;
            low = Low;
            close = Close;
            volume = Volume;
            wap = Wap;
        }
    }
    public struct HistoricalDataEndParameters
    {
        public int reqId;
        public DateTime startDate;
        public DateTime endDate;
        public HistoricalDataEndParameters(int ReqId, DateTime StartDate, DateTime EndDate)
        {
            reqId = ReqId;
            startDate = StartDate;
            endDate = EndDate;
        }
    }
    public struct RealtimeBarParameters
    {
        public int reqId;
        public DateTime date;
        public double open;
        public double high;
        public double low;
        public double close;
        public long volume;
        public double wap;
        public int count;
        public RealtimeBarParameters(int ReqId, DateTime Date, double Open, double High, double Low, double Close, long Volume, double Wap, int Count)
        {
            reqId = ReqId;
            date = Date;
            open = Open;
            high = High;
            low = Low;
            close = Close;
            volume = Volume;
            wap = Wap;
            count = Count;
        }
    }
    public abstract class Strategy
    {
        protected ThreadManager _tm;
        protected TickerGenerator _tg;
        protected DBAccess.DBAccess _db;
        protected MarketDataManager _mm;
        protected OrderManager _om;
        protected Messenger _msg;

        protected IBApi.EClientInterface _socket;
        protected string _time_zone;
        protected TimeZoneInfo _zone;
        protected TimeSpan _start_time;
        protected TimeSpan _end_time;
        protected string _bar_type;
        protected string _calendar_name;
        protected Utils.Calendar _calendar;
        protected readonly Object _lock = new Object();
        protected int _id;
        protected string _account;

        //common states
        protected DateTime _currDate;
        private Dictionary<int, int> _reqContractMap;
        private Dictionary<int, int> _contractReqMap;
        private List<int> _contracts;
        private Dictionary<int, ContractDetails> _contractDetailMap;
        private ManualResetEvent _realtimeBarEvent;

        public int id
        {
            get { return this._id; }
            set { this._id = value; }
        }
        public string account
        {
            get { return this._account; }
            set
            {
                lock (_lock)
                {
                    this._account = value;
                }
            }
        }
        //Time Zone Supports
        public static DateTime getExchangeTime(DateTime localTime, TimeZoneInfo zone)
        {
            return TimeZoneInfo.ConvertTimeFromUtc(localTime.ToUniversalTime(), zone);
        }
        public static DateTime getLocalTime(DateTime exchangeTime, TimeZoneInfo zone)
        {
            return TimeZoneInfo.ConvertTime(exchangeTime, zone, TimeZoneInfo.Local);
        }
        public static TimeZoneInfo mapDotNetTimeZone(string zone)
        {
            switch(zone)
            {
                case "America/Belize": return TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time");
                case "EST": return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                case "GMT": return TimeZoneInfo.FindSystemTimeZoneById("GMT Standard Time");
                case "Asia/Hong_Kong": return TimeZoneInfo.FindSystemTimeZoneById("China Standard Time");
                case "UTC": return TimeZoneInfo.Utc;
                default: return TimeZoneInfo.FindSystemTimeZoneById(zone);
            }
        }
        public static Utils.Calendar mapCalendar(string calendar_name)
        {
            switch(calendar_name)
            {
                case "ICE": return new UnitedKingdom(UnitedKingdom.Market.ICE);
                case "SGX": return new Singapore(Singapore.Market.SGX);
                case "ICE_SGX": return new JointCalendar(new UnitedKingdom(UnitedKingdom.Market.ICE), new Singapore(Singapore.Market.SGX));
                case "NYSE": return new UnitedStates(UnitedStates.Market.NYSE);
                case "CME": return new UnitedStates(UnitedStates.Market.CME);
                default: return new WeekendsOnly();
            }
        }
        public TimeZoneInfo getTimeZone() { return _zone; }
        public Strategy(int strategy_id, string config, IBApi.EClientInterface socket)
        {
            _socket = socket;
            id = strategy_id;
            XmlDocument xml = new XmlDocument();
            xml.LoadXml(config);
            XmlNode xn1 = xml.SelectSingleNode("/Config");
            XmlNode common = xn1.SelectSingleNode("Common");
            _time_zone = common["TimeZone"].InnerText;
            _zone = mapDotNetTimeZone(_time_zone);
            _start_time = TimeSpan.Parse(common["StartTime"].InnerText);
            _end_time = TimeSpan.Parse(common["EndTime"].InnerText);
            _calendar_name = common["Calendar"].InnerText;
            _calendar = mapCalendar(_calendar_name);
            if (common.SelectSingleNode("BarType") == null)
                _bar_type = "TRADES";
            else
                _bar_type = (common["BarType"].InnerText).ToUpper();
            _db = DBAccess.DBAccess.instance;
            _tg = TickerGenerator.instance;
            _tm = ThreadManager.instance;
            _mm = MarketDataManager.instance;
            _om = OrderManager.instance;
            _msg = Messenger.instance;
            _realtimeBarEvent = _tm.createEvent();
            _reqContractMap = new Dictionary<int, int>();
            _contractReqMap = new Dictionary<int, int>();
            _contracts = new List<int>();
            _contractDetailMap = new Dictionary<int, ContractDetails>();
        }
        public void initialize()
        {
            initStrategy();
            refreshContractsAndMktData();
            DateTime now = getExchangeTime(_socket.getCurrentLocalTime(), _zone);
            DateTime startTime = now.Date + _start_time;
            DateTime endTime = now.Date + _end_time;
            _socket.subscribeTimerEvent(getLocalTime(startTime, _zone).TimeOfDay, onStartOfDayWrapper);
            _socket.subscribeTimerEvent(getLocalTime(endTime, _zone).TimeOfDay, onEndOfDay);
            if (!_calendar.isBusinessDay(now))
                Console.WriteLine("Today {0} is not a business day according to calendar {1}!", now.Date.ToString(), _calendar_name);
            if (now > startTime && now < endTime && _calendar.isBusinessDay(now))
                onStartOfDayWrapper();
        }
        //Internal utility functions
        private void refreshContractsAndMktData()
        {
            _currDate = getExchangeTime(_socket.getCurrentLocalTime(), _zone).Date;
            foreach (int conId in _contracts)
            {
                int oldConId = _contractDetailMap.ContainsKey(conId) ? _contractDetailMap[conId].Summary.ConId : 0;
                int reqId = _tg.get();
                lock (_lock)
                    _reqContractMap.Add(reqId, conId);
                DateTime currd = getExchangeTime(_socket.getCurrentLocalTime(), _zone).Date;
                Contract con = _db.getContractByDate(conId, currd);
                _socket.reqContractDetails(reqId, con);
                while (_reqContractMap.ContainsKey(reqId)) Thread.Sleep(1);
                if (oldConId == 0)
                    _contractReqMap.Add(conId, _mm.reqRealTimeBars(id, _contractDetailMap[conId].Summary, 0, getBarType(), "IB"));
                else if (oldConId != _contractDetailMap[conId].Summary.ConId)
                {
                    _mm.cancelRealTimeBars(id, _contractReqMap[conId]);
                    _contractReqMap[conId] = _mm.reqRealTimeBars(id, _contractDetailMap[conId].Summary, 0, getBarType(), "IB");
                }
            }
        }
        //Utility functions for sub classes
        protected void writeCommonConfig(XmlWriter writer)
        {
            writer.WriteStartElement("Common");
            writer.WriteElementString("TimeZone", _time_zone);
            writer.WriteElementString("Calendar", _calendar_name);
            writer.WriteElementString("StartTime", _start_time.ToString(@"hh\:mm\:ss"));
            writer.WriteElementString("EndTime", _end_time.ToString(@"hh\:mm\:ss"));
            writer.WriteEndElement();
        }
        protected void addContract(int conId)
        {
            lock (_lock)
                _contracts.Add(conId);
        }
        protected BlockOrder createBlockOrder(int conId, XmlNode config)
        {
            return new BlockOrder(_id, _socket, _zone, _contractDetailMap[conId], config, conId);
        }
        protected ContractDetails getContractDetails(int conId)
        {
            return _contractDetailMap[conId];
        }
        //Public interface
        public string getBarType() { return _bar_type; }
        public void setConstractDetails(int reqId, ContractDetails details)
        {
            lock(_lock)
            {
                int conId = _reqContractMap[reqId];
                _contractDetailMap[conId] = details;
                _reqContractMap.Remove(reqId);
            }
        }
        public List<Contract> getContracts()
        {
            return _contractDetailMap.Values.Select(x => x.Summary).ToList();
        }
        public Contract getContract(int conId)
        {
            return _contractDetailMap[conId].Summary;
        }
        public void enableRealTimeBarEvent()
        {
            _realtimeBarEvent.Reset();
        }
        //Public wrappers
        public bool hasReqIDWrapper(int req_id)
        {
            if (_reqContractMap.ContainsKey(req_id))
                return true;
            return hasReqID(req_id);
        }
        public void onStartOfDayWrapper()
        {
            refreshContractsAndMktData();
            onStartOfDay();
        }
        public void onErrorWrapper(object op)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            ErrorParameters p = (ErrorParameters)op;
            onError(p.reqId, p.errorCode, p.errorMsg);
        }
        public void onHistoricalDataWrapper(object op)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            HistoricalDataParameters p = (HistoricalDataParameters)op;
            onHistoricalData(p.reqId, p.date, p.open, p.high, p.low, p.close, p.volume, p.wap);
        }
        public void onHistoricalDataEndWrapper(object op)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            HistoricalDataEndParameters p = (HistoricalDataEndParameters)op;
            onHistoricalDataEnd(p.reqId, p.startDate, p.endDate);
        }
        public void onRealtimeBarWrapper(object op)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            RealtimeBarParameters p = (RealtimeBarParameters)op;
            onRealtimeBar(p.reqId, p.date, p.open, p.high, p.low, p.close, p.volume, p.wap, p.count);
            _realtimeBarEvent.Set();
        }

        /////////////////////////////////////////////////////////////////
        // Below interfaces must be implemented for every strategy     //
        /////////////////////////////////////////////////////////////////

        //Internal pure interfaces for sub classes
        protected abstract void initStrategy(); //Strategy-specific initialization
        protected abstract bool hasReqID(int req_id);
        //Public pure interfaces
        public abstract void fromConfig(string config);
        public abstract string toConfig();
        public abstract bool calibrate(DateTime date, bool read_existing, bool save, bool overwrite_existing);

        /////////////////////////////////////////////////////////////////
        // Below interfaces can be overriden by each strategy          //
        /////////////////////////////////////////////////////////////////

        //Internal virtual interfaces for sub classes
        protected virtual void onStartOfDay() { }
        protected virtual void onError(int reqId, int errorCode, string errorMsg) { }
        protected virtual void onHistoricalData(int reqId, DateTime date, double open, double high, double low, double close, int volume, double WAP) { }
        protected virtual void onHistoricalDataEnd(int reqId, DateTime startDate, DateTime endDate) { }
        protected virtual void onRealtimeBar(int reqId, DateTime date, double open, double high, double low, double close, long volume, double wap, int count) { }
        //Public virtual interfaces
        public virtual List<Contract> getComboContracts() { return new List<Contract>(); }
        public virtual void onEndOfDay() { }
        public virtual bool onNextOrderId(int orderId) { return false; }    //true means the orderId is consumed by this strategy
        public virtual void onOpenOrder(int orderId, Contract contract, Order order, OrderState orderState) { }
        public virtual void onOrderStatus(int orderId, string status, double filled, double remaining, double avgFillPrice, int permId, int parentId, double lastFillPrice, int clientId, String whyHeld) { }
        public virtual void onTickPrice(int reqId, int field, double price, int canAutoExecute) { }
    }
}
