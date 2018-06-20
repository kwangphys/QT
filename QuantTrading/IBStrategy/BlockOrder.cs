using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;
using IBApi;

namespace IBStrategy
{
    public class BlockOrder
    {
        private readonly Object _lock = new Object();
        private OrderManager _om = OrderManager.instance;
        private TickerGenerator _tg = TickerGenerator.instance;
        private Messenger _msg = Messenger.instance;
        private ThreadManager _tm = ThreadManager.instance;

        EClientInterface _socket;
        Contract _con;
        int _ref_con_id;
        int _stg_id;
        int _unitSize;
        int _maxOrderSize;
        int _maxNOrders;
        double _minTick;
        int _maxBidAskUnit;
        TimeZoneInfo _zone;

        //Order specific parameters
        //bool _mktOrdersOnly;
        double _profitTarget;
        double _stopTarget;
        DateTime? _expiryStartTime;
        DateTime? _expiryEndTime;
        DateTime? _cancelAfter;

        //internal states
        List<int> _ids;
        List<int> _tickers;
        Dictionary<int, double> _quotes;
        List<Tuple<Order, DateTime>> _orderSchedule;
        Dictionary<int, Order> _openOrders;
        Dictionary<int, double> _filledQtys;
        OrderTemplate _entryTemplate;
        OrderTemplate _profitTemplate;
        OrderTemplate _stopTemplate;
        OrderTemplate _expiryTemplate;
        ManualResetEvent _placeOrderEvent;
        List<AutoResetEvent> _mktDataEvents;
        public BlockOrder(
            int stg_id,
            EClientInterface socket,
            TimeZoneInfo zone,
            ContractDetails details,
            XmlNode config,
            int ref_con_id = 0
            )
        {
            _stg_id = stg_id;
            _socket = socket;
            _con = details.Summary;
            _ref_con_id = ref_con_id;
            _minTick = details.MinTick;
            _unitSize = int.Parse(config["UnitSize"].InnerText);
            _maxOrderSize = int.Parse(config["MaxBlockSize"].InnerText);
            _maxNOrders = int.Parse(config["MaxNumberOfOrders"].InnerText);
            _maxBidAskUnit = config["MaxBidAskUnit"] == null ? 3 : int.Parse(config["MaxBidAskUnit"].InnerText);
            _zone = zone;
            //_mktOrdersOnly = mktOrdersOnly;
            _entryTemplate = getOrderTemplate(config, "EntryOrderTemplate");
            _profitTemplate = getOrderTemplate(config, "ProfitOrderTemplate");
            _stopTemplate = getOrderTemplate(config, "StopOrderTemplate");
            _expiryTemplate = getOrderTemplate(config, "ExpiryOrderTemplate");
            _ids = new List<int>();
            _tickers = new List<int>();
            _quotes = new Dictionary<int, double>();
            _filledQtys = new Dictionary<int, double>();
            _openOrders = new Dictionary<int, Order>();
            _quotes.Add(1, 0);  //bid
            _quotes.Add(2, 0);  //ask
            _quotes.Add(4, 0);  //last
            _quotes.Add(6, 0);  //high
            _quotes.Add(7, 0);  //low
            _quotes.Add(9, 0);  //close
            _placeOrderEvent = _tm.createEvent();
            _mktDataEvents = new List<AutoResetEvent>();
            for(int i = 0; i <= 14; ++i)
                _mktDataEvents.Add(null);
            _mktDataEvents[1] = new AutoResetEvent(false);
            _mktDataEvents[2] = new AutoResetEvent(false);
            _mktDataEvents[4] = new AutoResetEvent(false);
        }

        public void sendOrder(
            Order ord,
            DateTime endTime,
            //bool mktOrdersOnly
            double profitTarget = 0,
            double stopTarget = 0,
            DateTime? expiryStartTime = null,
            DateTime? expiryEndTime = null,
            DateTime? cancelAfter = null
            )
        {
            _profitTarget = profitTarget;
            _stopTarget = stopTarget;
            _expiryStartTime = expiryStartTime;
            _expiryEndTime = expiryEndTime;
            _cancelAfter = cancelAfter;
            //Create order schedules
            int qty = (int)(ord.TotalQuantity);

            //Test Only
            //_unitSize = 2;
            //_maxOrderSize = 4;
            //_maxNOrders = 12;
            //qty = 20;

            int maxNOrders = Math.Min(_maxNOrders, (int)((qty - 0.5) / (double)(_unitSize)) + 1);
            int minNOrders = (int)((qty - 0.5) / (double)(_maxOrderSize)) + 1;
            Random r = new Random();
            int nOrders = minNOrders >= maxNOrders ? maxNOrders : r.Next(minNOrders, maxNOrders + 1);
            int minUnitSize = Math.Min(_unitSize, qty / nOrders);
            int remainQty = qty - minUnitSize * nOrders;
            int maxRemainOrderSize = _maxOrderSize - minUnitSize;
            List<double> qtySchedule = new List<double>();
            if (remainQty == 0)
            {
                for (int i = 0; i < nOrders; ++i)
                    qtySchedule.Add(0);
            }
            else
            {
                for (int i = 0; i < nOrders; ++i)
                    qtySchedule.Add(r.NextDouble());
                double qsum = qtySchedule.Sum();
                double qsmall = 0;
                double qsmallnew = 0;
                for (int i = 0; i < nOrders; ++i)
                {
                    qtySchedule[i] *= remainQty / qsum;
                    if (qtySchedule[i] > maxRemainOrderSize)
                    {
                        qsmallnew += qtySchedule[i] - maxRemainOrderSize;
                        qtySchedule[i] = maxRemainOrderSize;
                    }
                    else
                    {
                        qsmall += qtySchedule[i];
                        qsmallnew += qtySchedule[i];
                    }
                }
                if (qsmallnew != qsmall)
                {
                    double qratio = qsmallnew / qsmall;
                    for (int i = 0; i < nOrders; ++i)
                    {
                        if (qtySchedule[i] < maxRemainOrderSize)
                            qtySchedule[i] *= qratio;
                    }
                }
                for (int i = 1; i < nOrders; ++i)
                    qtySchedule[i] += qtySchedule[i - 1];
            }
            List<double> timeSchedule = new List<double>();
            for(int i = 0; i < nOrders; ++i)
                timeSchedule.Add(r.NextDouble());
            timeSchedule.Sort();

            _orderSchedule = new List<Tuple<Order, DateTime>>();
            DateTime tstart = Strategy.getExchangeTime(_socket.getCurrentLocalTime(), _zone);
            long remainTicks = (endTime - tstart).Ticks;
            int prerq = 0;
            for (int i = 0; i < nOrders; ++i)
            {
                int rq = (int)(Math.Round(qtySchedule[i], 0));
                int q = minUnitSize + rq - prerq;
                prerq = rq;
                long ticks = (long)(timeSchedule[i] * remainTicks);
                Order o = _entryTemplate.clone();
                o.Action = ord.Action;
                o.TotalQuantity = q;
                if (!_entryTemplate.isSet("OrderType"))
                    o.OrderType = "MKT";
                //o.LmtPrice = 0;
                o.Transmit = false;
                //o.TrailStopPrice = 0;
                //o.TrailingPercent = 0;
                //o.SmartComboRoutingParams = new List<TagValue>();
                //o.SmartComboRoutingParams.Add(new TagValue("NonGuaranteed", "1"));
                DateTime t = tstart.AddTicks(ticks);
                _orderSchedule.Add(new Tuple<Order, DateTime>(o, t));
                _msg.logMessage(t, 1, "Information", "BlockOrder", _con.ConId, _stg_id, "Scheduled " + o.OrderType + " " + o.Action + " " + o.TotalQuantity.ToString());
            }
            _placeOrderEvent.Reset();
            _mktDataEvents[1].Reset();
            _mktDataEvents[2].Reset();
            _mktDataEvents[4].Reset();
            ThreadPool.QueueUserWorkItem(new WaitCallback(placeOrders));
        }

        private OrderTemplate getOrderTemplate(XmlNode config, string node)
        {
            var tmpNode = config[node];
            if (tmpNode == null)
                return new OrderTemplate();
            return new OrderTemplate(tmpNode);
        }

        public void assignOrderID(int id)
        {
            lock (_lock)
            {
                if (_ids.Count() == 0 || id != _ids.Last())
                    _ids.Add(id);
            }
        }
        public void assignQuote(int field, double price)
        {
            lock (_lock)
            {
                _quotes[field] = price;
                if(_mktDataEvents[field] != null)
                    _mktDataEvents[field].Set();
            }
        }
        public bool hasOpenOrder(int orderId)
        {
            return _openOrders.ContainsKey(orderId);
        }
        public List<int> getOrderIDs()
        {
            return _ids;
        }
        public List<int> getTickerIDs()
        {
            return _tickers;
        }
        public int Count()
        {
            return _orderSchedule.Count();
        }
        public int Filled()
        {
            return _ids.Count();
        }
        public double getRemainingQty()
        {
            return _filledQtys.Values.Sum();
        }
        public void onOrderStatus(int orderId, string status, double filled, double remaining, double avgFillPrice, int permId, int parentId, double lastFillPrice, int clientId, String whyHeld) 
        {
            lock (_lock)
            {
                if (status == "Filled" && filled > 0)
                {
                    if (_openOrders.ContainsKey(orderId))
                    {
                        if (_openOrders[orderId].ParentId == 0)
                            _filledQtys[orderId] = filled;
                        else
                            _filledQtys[orderId] = -filled;
                    }
                    if (remaining == 0)
                        _openOrders.Remove(orderId);
                    else
                        _openOrders[orderId].TotalQuantity = remaining;
                }
                else if (status == "Cancelled")
                {
                    if (_openOrders.ContainsKey(orderId))
                    {
                        if (_openOrders[orderId].ParentId == 0)
                            _filledQtys[orderId] = filled;
                        else
                            _filledQtys[orderId] = -filled;
                    }
                    _openOrders.Remove(orderId);
                }
            }
        }
        private void placeOrders(object dummy)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
            int iorder = 0;
            Random r = new Random();
            long nticks = _expiryStartTime != null && _expiryEndTime != null ? ((DateTime)(_expiryEndTime) - (DateTime)(_expiryStartTime)).Ticks : 0;
            while (iorder < Count())
            {
                DateTime t = Strategy.getLocalTime(_orderSchedule[iorder].Item2, _zone);
                _socket.sleepUntil(t, _placeOrderEvent);
                Order ord = _orderSchedule[iorder].Item1;
                double price = 0;
                if(_profitTarget != 0 || _stopTarget != 0)
                {
                    bool success = false;
                    while (!success)
                    {
                        int reqId = _tg.get();
                        lock (_lock)
                        {
                            _quotes[1] = 0;
                            _quotes[2] = 0;
                            _quotes[4] = 0;
                            _quotes[6] = 0;
                            _quotes[7] = 0;
                            _quotes[9] = 0;
                            _tickers.Add(reqId);
                        }
                        _socket.reqMktData(reqId, _con, "", true, null);
                        _mktDataEvents[1].WaitOne();
                        _mktDataEvents[2].WaitOne();
                        if (Math.Abs(_quotes[2] - _quotes[1]) <= _minTick * _maxBidAskUnit)
                        {
                            if (_quotes[1] == 0 || _quotes[2] == 0)
                            {
                                _mktDataEvents[4].WaitOne();
                                if (_quotes[4] != 0)
                                {
                                    price = _quotes[4];
                                    success = true;
                                }
                                else
                                {
                                    string m = "Quote Discrepancy. Bid=" + _quotes[1].ToString() + "/Ask=" + _quotes[2].ToString() + "/Last=" + _quotes[4].ToString();
                                    _msg.logError(_socket.getCurrentLocalTime(), -3, "User", _con.ConId, _stg_id, m, "");
                                    Thread.Sleep(100);
                                }
                            }
                            else
                            {
                                price = (_quotes[1] + _quotes[2]) / 2.0;
                                price = Math.Round((price / _minTick), 0) * _minTick;
                                success = true;
                            }
                        }
                        else
                        {
                            string m = "Quote Discrepancy. Bid=" + _quotes[1].ToString() + "/Ask=" + _quotes[2].ToString();
                            _msg.logError(_socket.getCurrentLocalTime(), -3, "User", _con.ConId, _stg_id, m, "");
                            Thread.Sleep(100);
                        }
                    }
                }
                double profit = _profitTarget == 0 ? 0 : price + (ord.Action == "BUY" ? _profitTarget : -_profitTarget);
                double stop = _stopTarget == 0 ? 0 : price + (ord.Action == "BUY" ? -_stopTarget : _stopTarget);
                DateTime expiry = nticks == 0 ? DateTime.MinValue : (Strategy.getLocalTime((DateTime)(_expiryStartTime), _zone)).AddTicks((long)(r.NextDouble() * nticks));
                List<Order> ordSet = OrderFactory.createBracketOrder(ord, profit, stop, expiry, _profitTemplate, _stopTemplate, _expiryTemplate);
                _om.placeBracketOrders(_stg_id, _con, ordSet, _ref_con_id);
                _msg.logMessage(_socket.getCurrentLocalTime(), 1, "Information", "BlockOrder", _con.ConId, _stg_id, "Executed " + iorder.ToString() + ": " + ord.OrderId.ToString() + " " + ord.OrderType + " " + ord.Action + " " + ord.TotalQuantity.ToString());
                foreach (Order order in ordSet)
                {
                    lock (_lock)
                    {
                        _openOrders.Add(order.OrderId, order);
                        _filledQtys.Add(order.OrderId, 0);
                    }
                    assignOrderID(order.OrderId);
                }
                iorder++;
            }
            _placeOrderEvent.Set();
            if(_cancelAfter != null)
            {
                _socket.sleepUntil(Strategy.getLocalTime((DateTime)_cancelAfter, _zone), null);
                cancelUnfilledEntryOrders();
            }
        }
        public void cancelUnfilledEntryOrders()
        {
            foreach (var pair in _openOrders)
                if(pair.Value.ParentId == 0)
                    _socket.cancelOrder(pair.Key);
        }
    }
}
