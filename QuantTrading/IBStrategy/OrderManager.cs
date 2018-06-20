using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using IBApi;

namespace IBStrategy
{
    public class OrderManager
    {
        private readonly Object _lock = new Object();
        private EClientInterface _socket;
        private DBAccess.DBAccess _db;
        private int _currTicker;
        private bool _isBacktest;
        private List<Tuple<Contract, Order>> _orders;
        private Dictionary<int, Tuple<int, int, Order>> _orderMap;
        private AutoResetEvent _newOrderEntryEvent;
        private AutoResetEvent _newOrderSubmittedEvent;
        public void set(int id)
        {
            lock (_lock)
            {
                _currTicker = id;
            }
        }
        public int get()
        {
            lock (_lock)
            {
                ++_currTicker;
            }
            return _currTicker;
        }
        public void setupSocket(EClientInterface socket)
        {
            lock (_lock)
            {
                _socket = socket;
                _isBacktest = _socket.GetType() == typeof(BacktestEngine);
            }
            Thread t = new Thread(new ThreadStart(runOrderAgent));
            t.CurrentCulture = CultureInfo.InvariantCulture;
            t.CurrentUICulture = CultureInfo.InvariantCulture;
            t.Start();
        }
        public void placeOrder(int stg_id, Contract con, Order order, int ref_con_id = 0, bool modify = false)
        {
            lock (_lock)
            {
                int refConId = ref_con_id == 0 ? con.ConId : ref_con_id;
                if (modify)
                    _orderMap[order.OrderId] = new Tuple<int, int, Order>(stg_id, refConId, order);
                else
                {
                    order.OrderId = ++_currTicker;
                    _orderMap.Add(order.OrderId, new Tuple<int, int, Order>(stg_id, refConId, order));
                }
                _orders.Add(new Tuple<Contract, Order>(con, order));
                _newOrderEntryEvent.Set();
            }
            if (_isBacktest)
                _newOrderSubmittedEvent.WaitOne();
        }
        public void placeBracketOrders(int stg_id, Contract con, List<Order> orders, int ref_con_id = 0, bool modify = false)    //The first order is always the parent
        {
            foreach (Order o in orders)
            {
                if (o != orders[0])
                    o.ParentId = orders[0].OrderId;
                placeOrder(stg_id, con, o, ref_con_id, modify);
            }
        }
        public int getOrderStrategyID(int order_id)
        {
            return _orderMap.ContainsKey(order_id) ? _orderMap[order_id].Item1 : -1;
        }
        public int getOrderContractID(int order_id)
        {
            return _orderMap.ContainsKey(order_id) ? _orderMap[order_id].Item2 : -1;
        }
        public Order getOrder(int order_id)
        {
            return _orderMap.ContainsKey(order_id) ? _orderMap[order_id].Item3 : null;
        }
        public List<Order> getOrdersByStrategy(int strategy_id)
        {
            return _orderMap.Values.Where(x => x.Item1 == strategy_id).Select(x => x.Item3).ToList();
        }
        public void updateOrder(Order order)
        {
            lock(_lock)
            {
                Tuple<int, int, Order> info = _orderMap[order.OrderId];
                _orderMap[order.OrderId] = new Tuple<int, int, Order>(info.Item1, info.Item2, order);
            }
        }
        private void runOrderAgent()
        {
            int count = 0;
            while (true)
            {
                _newOrderEntryEvent.WaitOne();
                for (; count < _orders.Count; ++count)
                    _socket.placeOrder(_orders[count].Item2.OrderId, _orders[count].Item1, _orders[count].Item2);
                if (_isBacktest)
                    _newOrderSubmittedEvent.Set();
            }
        }

        public OrderManager()
        {
            set(0);
            lock (_lock)
            {
                _db = DBAccess.DBAccess.instance;
                _orders = new List<Tuple<Contract, Order>>();
                _orderMap = new Dictionary<int, Tuple<int, int, Order>>();
                _newOrderEntryEvent = new AutoResetEvent(false);
                _newOrderSubmittedEvent = new AutoResetEvent(false);
            }
        }
        //Singleton interface
        private static OrderManager _instance;
        public static OrderManager instance
        {
            get
            {
                if (_instance == null)
                    _instance = new OrderManager();
                return _instance;
            }
        }
    }
}
