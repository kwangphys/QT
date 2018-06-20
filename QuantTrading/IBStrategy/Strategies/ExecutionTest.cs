using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Xml;
using DBAccess;
using IBApi;

namespace IBStrategy
{
    public class ExecutionTest : Strategy
    {
        TimeSpan _interval;
        Dictionary<string, OrderTemplate> _templates;
        int _con_id;
        int _qty;

        //states
        bool _initialized;
        bool _longOrShort;
        DateTime _lastTradeTime;
        HashSet<int> _executed;

        public ExecutionTest(int strategy_id, string config, EClientInterface socket)
            : base(strategy_id, config, socket)
        {
            _initialized = false;
            fromConfig(config);
        }
        public override void fromConfig(string config)
        {
            XmlDocument xml = new XmlDocument();
            xml.LoadXml(config);
            XmlNode xn1 = xml.SelectSingleNode("/Config");
            _con_id = Int32.Parse(xn1["Contract"].InnerText);
            addContract(_con_id);
            _qty = Int32.Parse(xn1["Quantity"].InnerText);
            _interval = TimeSpan.Parse(xn1["Interval"].InnerText);
            _templates = new Dictionary<string, OrderTemplate>();
            XmlNodeList orderNodes = xn1["Orders"].GetElementsByTagName("Order");
            foreach (XmlNode node in orderNodes)
                _templates.Add(node["Name"].InnerText, new OrderTemplate(node["Template"]));
        }
        public override string toConfig()
        {
            throw new Exception("Not Implemented!");
        }
        public override bool calibrate(DateTime date, bool read_existing, bool save, bool overwrite_existing)
        {
            //DO NOTHING
            return false;
        }
        protected override bool hasReqID(int reqId)
        {
            return false;
        }
        protected override void initStrategy()
        { }
        //Interfaces
        protected override void onStartOfDay()
        {
            lock (_lock)
            {
                _longOrShort = true;
                _lastTradeTime = Strategy.getExchangeTime(_socket.getCurrentLocalTime(),_zone).AddDays(-1);
                _executed = new HashSet<int>();
                _executed.Add(1);   //dummy
                _executed.Add(2);   //dummy
                _initialized = true;
            }
        }
        public override void onEndOfDay()
        {
            lock (_lock)
            {
                _initialized = false;
            }
        }
        public override bool onNextOrderId(int orderId) //true means the orderId is consumed by this strategy
        {
            return false;
        }
        public override void onOrderStatus(int orderId, string status, double filled, double remaining, double avgFillPrice, int permId, int parentId, double lastFillPrice, int clientId, String whyHeld)
        {
            if (remaining == 0)
            {
                lock (_lock)
                {
                    _executed.Add(orderId);
                    _lastTradeTime = Strategy.getExchangeTime(_socket.getCurrentLocalTime(), _zone);
                }
            }
        }
        public override void onTickPrice(int reqId, int field, double price, int canAutoExecute)
        {
        }
        protected override void onRealtimeBar(int reqId, DateTime date, double open, double high, double low, double close, long volume, double wap, int count)
        {
            if (!_initialized)
                return;
            lock (_lock)
            {
                if (_executed.Count() == _templates.Count() && date - _lastTradeTime >= _interval)
                {
                    _executed = new HashSet<int>();
                    List<Order> orders = new List<Order>();
                    foreach (var pair in _templates)
                    {
                        Order ord = pair.Value.clone();
                        ord.TotalQuantity = _qty;
                        ord.Action = _longOrShort ? "BUY" : "SELL";
                        ord.OrderRef = pair.Key;
                        orders.Add(ord);
                    }
                    Contract c = getContract(_con_id);
                    foreach (Order ord in orders)
                        _om.placeOrder(id, c, ord, _con_id);
                    _longOrShort = !_longOrShort;
                }
            }
        }
    }
}
