using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Xml;
using IBApi;

namespace IBStrategy
{
    public class EWrapperImpl : EWrapper
    {
        DBAccess.DBAccess db;
        Messenger msg;
        TickerGenerator tg;
        OrderManager om;
        MarketDataManager mm;
        EClientInterface clientSocket;
        public readonly EReaderSignal Signal = new EReaderMonitorSignal();

        public Dictionary<int, ContractDetails> contractDetailsMap;
        Dictionary<int, Strategy> strategies;
        Dictionary<int, HashSet<string>> orderStatuses;
        HashSet<int> orders;
        public int nextOrderId;
        public string accountName;
        private bool gettingOpenOrders;
        private int sleepTime;
        private string engine_type;
        private Boolean verbose;
        List<TimeSpan> pnlSchedule;
        Dictionary<int, int> currPnlIndices;    // contract id -> pnl report schedule
        protected readonly Object _lock = new Object();

        public static DateTime baseTime = new DateTime(1970, 1, 1, 0, 0, 0);
        //public Dictionary<int, int> dbTickMap;          //con to db tick

        //Controls
        bool requireLiveBars;
        bool recordLiveBars;
        bool overwriteLiveBars;
        public EWrapperImpl(
            string account_config,
            bool require_live_bars = true,
            bool record_live_bars = false,
            bool overwrite_live_bars = false,
            int sleep_time = 5000
        )
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;

            requireLiveBars = require_live_bars;
            recordLiveBars = record_live_bars;
            overwriteLiveBars = overwrite_live_bars;
            sleepTime = sleep_time;
            gettingOpenOrders = false;

            XmlDocument xml = new XmlDocument();
            xml.LoadXml(account_config);
            XmlNode xn1 = xml.SelectSingleNode("/Config");
            engine_type = xn1["EngineType"].InnerText;
            verbose = xn1["Verbose"] == null ? true : Boolean.Parse(xn1["Verbose"].InnerText);

            msg = Messenger.instance;
            XmlNode xnemail = xn1.SelectSingleNode("Email");
            if (xnemail != null)
            {
                string smtp = xnemail["SMTP"].InnerText;
                int port = Int32.Parse(xnemail["Port"].InnerText);
                string address = xnemail["Address"].InnerText;
                string password = xnemail["Password"].InnerText;
                bool enableSsl = Boolean.Parse(xnemail["EnableSSL"].InnerText);
                msg.setupEmail(smtp, port, address, password, enableSsl);
            }
            pnlSchedule = new List<TimeSpan>();
            XmlElement pnlScheduleNode = xn1["PnLReportSchedule"];
            if (pnlScheduleNode != null)
            {
                XmlNodeList sub_nodes = pnlScheduleNode.GetElementsByTagName("Time");
                foreach (XmlNode node in sub_nodes)
                    pnlSchedule.Add(TimeSpan.Parse(node.InnerText));
            }
            contractDetailsMap = new Dictionary<int, ContractDetails>();
            currPnlIndices = new Dictionary<int, int>();
            strategies = new Dictionary<int, Strategy>();
            //dbTickMap = new Dictionary<int, int>();
            orders = new HashSet<int>();
            orderStatuses = new Dictionary<int, HashSet<string>>();

            db = DBAccess.DBAccess.instance;
            tg = TickerGenerator.instance;
            om = OrderManager.instance;
            mm = MarketDataManager.instance;
            if (engine_type == "TWS")
                clientSocket = new EClientSocket(this, Signal);
            else
                clientSocket = new BacktestEngine(this, db, xn1["BacktestConfig"]);
            om.setupSocket(clientSocket);
            mm.setupSocket(clientSocket);
        }

        public void initialize(List<int> strategy_ids)
        {
            lock (_lock)
            {
                gettingOpenOrders = true;
                clientSocket.reqAllOpenOrders();
            }
            while (gettingOpenOrders) { Thread.Sleep(1000); }

            foreach (int id in strategy_ids)
            {
                Strategy strategy = StrategyBuilder.buildStrategy(id, clientSocket);
                strategy.account = accountName;
                lock (_lock)
                    strategies.Add(id, strategy);
                strategy.initialize();
            }
            //foreach (KeyValuePair<int, Strategy> pair in strategies)
            //{
            //    Strategy stg = pair.Value;
            //    stg.initialize();
            //    List<Contract> combo_contracts = stg.getComboContracts();
            //    foreach (Contract con in combo_contracts)
            //    {
            //        if (!contracts.ContainsKey(con.ConId))
            //        {
            //            lock (_lock)
            //            {
            //                ContractDetails details = new ContractDetails();
            //                details.Summary = con;
            //                contracts.Add(con.ConId, details);
            //                currPnlIndices.Add(con.ConId, pnlSchedule.Count > 0 ? 0 : -1);

            //                int db_bar_id = db.getBarDataId(con.ConId, "live", 5, "IB");
            //                if (db_bar_id == 0)
            //                    db_bar_id = db.addBarDataId(con.ConId, "live", 5, "IB", "");
            //                dbBarMap.Add(con.ConId, db_bar_id);

            //                int db_tick_id = db.getTickDataId(con.ConId, "IB");
            //                if (db_tick_id == 0)
            //                    db_tick_id = db.addTickDataId(con.ConId, "IB", "");
            //                mm.reqMktData(stg.id, con, "IB");
            //            }
            //        }
            //        stg.setConstractDetails(con.ConId, contracts[con.ConId]);
            //    }
            //}
            Thread.Sleep(sleepTime);
        }
        //Interfaces
        //Connection and Server
        public virtual void error(int tickerId, int errorCode, string errorMsg)
        {
            if (errorCode == 504)   //Not connected
            {
                Console.WriteLine("Connecting again...");
                msg.logError(clientSocket.getCurrentLocalTime(), errorCode, "TWS", 0, 0, errorMsg, "");
                (clientSocket as EClientSocket).eConnect("127.0.0.1", 7496, 0);
                Thread.Sleep(sleepTime);
                return;
            }
            if (errorCode == 1100 || errorCode == 1300) // Connectivity between IB and the TWS has been lost
            {
                string subject = "Sys Error " + errorCode + " : " + errorMsg;
                string body = "";
                msg.sendEmail(subject, body);
            }
            if (errorCode == 1102) // Connectivity between IB and TWS has been restored- data maintained
            {
                string subject = "Reconnected " + errorCode + " : " + errorMsg;
                string body = "";
                msg.sendEmail(subject, body);
            }
            //if (errorCode == 2108) //Market data farm connection is inactive but should be available upon demand.hfarm
            //{
            //    if (requireLiveBars)
            //    {
            //        Console.WriteLine("Requesting Live Bars...");
            //        msg.logError(clientSocket.getCurrentLocalTime(), errorCode, "TWS", 0, 0, errorMsg, "");
            //        foreach (var pair in contracts)
            //        {
            //            int bar_id = tg.get();
            //            clientSocket.reqRealTimeBars(pair.Key, pair.Value, 0, "TRADES", false, new List<TagValue>());
            //        }
            //    }
            //}
            bool found_owner = false;
            foreach (KeyValuePair<int, Strategy> pair in strategies)
            {
                Strategy strategy = pair.Value;
                if (strategy.hasReqIDWrapper(tickerId))
                {
                    found_owner = true;
                    ThreadPool.UnsafeQueueUserWorkItem(new WaitCallback(strategy.onErrorWrapper), new ErrorParameters(tickerId, errorCode, errorMsg));
                }
            }
            if (!found_owner)
            {
                int contract_id = mm.getContractID(tickerId);
                msg.logError(clientSocket.getCurrentLocalTime(), errorCode, "TWS", contract_id, 0, errorMsg, "");
            }
        }
        public virtual void error(Exception e)
        {
            msg.logError(clientSocket.getCurrentLocalTime(), -1, "QT", 0, 0, e);
        }
        public virtual void error(string str)
        {
            msg.logError(clientSocket.getCurrentLocalTime(), -2, "User", 0, 0, str, "");
        }
        public virtual void currentTime(long time)  //current system time
        {
            if (verbose)
            {
                string m = "Current Time. Time: " + time.ToString();
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Time", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void connectionClosed()
        {
            if (verbose)
            {
                string m = "Connection Closed";
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Connection", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void verifyMessageAPI(string apiData)
        {
            if (verbose)
            {
                string m = "Verify Message API. API Data: " + apiData;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Connection", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void verifyCompleted(bool isSuccessful, string errorText)
        {
            if (verbose)
            {
                string m = "Verify Completed. Is Successful: " + isSuccessful + ", Error Text: " + errorText;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Connection", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void verifyAndAuthMessageAPI(string apiData, string xyzChallenge)
        {
            if (verbose)
            {
                string m = "verifyAndAuthMessageAPI. API Data: " + apiData + " xyzChallenge: " + xyzChallenge;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Connection", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void verifyAndAuthCompleted(bool isSuccessful, string errorText)
        {
            if (verbose)
            {
                string m = "verifyAndAuth Completed. IsSuccessful: " + isSuccessful + " Error: " + errorText;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Connection", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        //Market Data
        public virtual void tickPrice(int tickerId, int field, double price, int canAutoExecute)
        {
            string m = "Tick Price. Ticker Id: " + tickerId.ToString() + ", Field: " + field.ToString() + ", Price: " + price.ToString() + ", CanAutoExecute: " + canAutoExecute.ToString();
            foreach (KeyValuePair<int, Strategy> pair in strategies)
            {
                if (pair.Value.hasReqIDWrapper(tickerId))
                {
                    if (verbose)
                        msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Quote", 0, pair.Value.id, m);
                    pair.Value.onTickPrice(tickerId, field, price, canAutoExecute);
                    break;
                }
            }
        }
        public virtual void tickSize(int tickerId, int field, int size)
        {
            Console.WriteLine("Tick Size. Ticker Id: " + tickerId + ", Field: " + field + ", Size: " + size);
        }
        public virtual void tickOptionComputation(int tickerId, int field, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice)
        {
            Console.WriteLine("Tick Option Computation. Ticker Id: " + tickerId + ", Field: " + field + ", Implied Vol: " + impliedVol + ", Delta: " + delta + ", Option Price: " + optPrice + ", PV Divident: " + pvDividend + ", Gamma: " + gamma + ", Vega: " + vega + ", Theta: " + theta + ", Underlying Price: " + undPrice);
        }
        public virtual void tickGeneric(int tickerId, int tickType, double value)
        {
            Console.WriteLine("Tick Generic. Ticker Id: " + tickerId + ", Tick Type: " + tickType + ", Value: " + value);
        }
        public virtual void tickString(int tickerId, int tickType, string value)
        {
            Console.WriteLine("Tick String. Ticker Id: " + tickerId + ", Tick Type: " + tickType + ", Value: " + value);
        }
        public virtual void tickEFP(int tickerId, int tickType, double basisPoints, string formattedBasisPoints, double impliedFuture, int holdDays, String futureExpiry, double dividendImpact, double dividendsToExpiry)
        {
            Console.WriteLine("Tick EFP. Ticker Id: " + tickerId + ", Tick Type: " + tickType + ", Basis Points: " + basisPoints + ", Formatted Basis Points: " + formattedBasisPoints + ", Implied Future: " + impliedFuture + ", Hold Days: " + holdDays + ", Future Expiry: " + futureExpiry + ", Divident Impact: " + dividendImpact + ", Dividents To Expiry: " + dividendsToExpiry);
        }
        public virtual void tickSnapshotEnd(int tickerId)
        {
            Console.WriteLine("Tick Snapshot End. Ticker Id: " + tickerId);
        }
        public virtual void marketDataType(int reqId, int marketDataType)   //Indication if frozen or real-time mode
        {
            int contract_id = mm.getContractID(reqId);
            if (verbose)
            {
                string m = "Market Data Type. ReqId: " + reqId + ", Market Data Type: " + marketDataType;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Market Data", contract_id, 0, m);
                Console.WriteLine(m);
            }
        }
        //Orders
        public virtual void orderStatus(int orderId, string status, double filled, double remaining, double avgFillPrice, int permId, int parentId, double lastFillPrice, int clientId, String whyHeld)
        {
            string s = status == "PartialFill" ? status + "_" + remaining.ToString() : status;
            //FixeMe: partial fill is not supported!
            if (orderStatuses.ContainsKey(orderId) && orderStatuses[orderId].Contains(s))
                return;
            lock (_lock)
            {
                if (!orderStatuses.ContainsKey(orderId))
                    orderStatuses.Add(orderId, new HashSet<string>());
                orderStatuses[orderId].Add(s);
            }
            if (gettingOpenOrders)
                return;
            db.runNonQuery("insert into order_status values (" + orderId.ToString() + "," + db.getSqlDateTime(clientSocket.getCurrentLocalTime()) + ",'" + status + "'," + ((int)(filled)).ToString() + "," + avgFillPrice.ToString() + ",'')");
            if(verbose)
                Console.WriteLine("Order Status. Order Id: " + orderId + ", Status: " + status + ", Filled: " + filled + ", Remaining: " + remaining + ", Avg Fill Price: " + avgFillPrice + ", Perm Id: " + permId + ", Parent Id: " + parentId + ", Last Fill Price: " + lastFillPrice + ", Client Id: " + clientId + ", Why Held: " + whyHeld);
            int stg_id_value = om.getOrderStrategyID(orderId);
            Strategy stg = strategies.ContainsKey(stg_id_value) ? strategies[stg_id_value] : null;
            if (stg != null)
                stg.onOrderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld);
            if(status == "Filled")
            {
                Contract con = stg.getContract(om.getOrderContractID(orderId));
                Order ord = om.getOrder(orderId);
                string subject = (ord.Action == "BUY" ? "Bought " : "Sold ") + filled.ToString() + " " + con.LocalSymbol + " @" + avgFillPrice.ToString();
                string body = "StrategyId:\t" + stg_id_value.ToString() + "\nOrderId:\t" + orderId.ToString() + "\nFilled:\t" + filled.ToString() + "\nPrice:\t" + avgFillPrice.ToString() + "\nRemaining:\t" + remaining.ToString() + "\nParent:\t" + parentId.ToString();
                msg.sendEmail(subject, body);
            }
            //else if (status == "Cancelled")
            //{
            //    string subject = "Order " + orderId.ToString() + " of strategy " + stg_id_value.ToString() + " cancelled";
            //    string body = "OrderId:\t" + orderId.ToString() + "\nParent:\t" + parentId.ToString();
            //    msg.sendEmail(subject, body);
            //}
        }
        public virtual void openOrder(int orderId, Contract contract, Order order, OrderState orderState )
        {
            if (orders.Contains(orderId))
                return;
            lock (_lock)
            {
                orders.Add(orderId);
            }
            om.updateOrder(order);
            if (gettingOpenOrders)
                return;
            long qty = (int)(order.TotalQuantity);
            if (order.Action != "BUY")
                qty = -qty;
            //db.runNonQuery("insert into order_status values (" + orderId.ToString() + "," + db.getSqlDateTime(clientSocket.getCurrentLocalTime()) + ",'" + orderState.Status + "',0,0,'')");
            if(verbose)
                Console.WriteLine("Open Order. Order Id: " + orderId + ", Contract: " + contract.ToString() + ", Order: " + order.ToString() + ", Order State: " + orderState.ToString());
            int stg_id_value = om.getOrderStrategyID(orderId);
            Strategy stg = strategies.ContainsKey(stg_id_value) ? strategies[stg_id_value] : null;
            string stg_id = stg == null ? "null" : stg.id.ToString();
            string limit_price = order.LmtPrice == 0 ? "null" : order.LmtPrice.ToString();
            string stop_price = order.AuxPrice == 0 ? "null" : order.AuxPrice.ToString();
            bool hasOCA = order.OcaGroup != null && order.OcaGroup != "";
            string oca_type = hasOCA ? order.OcaType.ToString() : "null";
            string oca_group = hasOCA ? "'" + order.OcaGroup + "'" : "null";
            string parent_id = order.ParentId.ToString();
            string order_ref = order.OrderRef == null ? "" : order.OrderRef;
            int conId = contract.SecType == "BAG" ? om.getOrderContractID(orderId) : contract.ConId;
            db.runNonQuery("insert into orders values (" + orderId.ToString() + "," + conId.ToString() + "," + db.getSqlDateTime(clientSocket.getCurrentLocalTime()) + "," + qty.ToString() + ",'" + order.OrderType + "'," + limit_price + "," + stop_price + ",'" + order.Tif + 
                "'," + stg_id + "," + parent_id + "," + oca_group + "," + oca_type + ",'" + order.PermId.ToString() + "','" + order_ref + "','" + order.Account + "')");
            if (stg != null)
                stg.onOpenOrder(orderId, contract, order, orderState);
        }
        public virtual void openOrderEnd()
        {
            lock(_lock)
            {
                gettingOpenOrders = false;
            }
            if (verbose)
            {
                string m = "Open Order End";
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Order", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void nextValidId(int orderId)
        {
            if (verbose)
            {
                string m = "Next Valid Id. Order Id: " + orderId;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "System", 0, 0, m);
                Console.WriteLine(m);
            }
            lock (_lock)
            {
                nextOrderId = orderId;
            }
            foreach (var pair in strategies)
                if (pair.Value.onNextOrderId(orderId))
                    break;
        }
        public virtual void deltaNeutralValidation(int reqId, UnderComp underComp)
        {
            if (verbose)
            {
                int contract_id = mm.getContractID(reqId);
                string m = "Delta Neutral Validation. ReqId: " + reqId + ", Underlying Component: " + underComp.ConId.ToString() + "," + underComp.Price.ToString() + "," + underComp.Delta.ToString();
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Trading", contract_id, 0, m);
                Console.WriteLine(m);
            }
        }
        //Account and Portfolio
        public virtual void updateAccountValue(string key, string value, string currency, string account_name)
        {
            if (verbose)
            {
                string m = "Update Account Value. Key: " + key + ", Value: " + value + ", Currency: " + currency + ", Account Name: " + account_name;
                //msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void updatePortfolio(Contract contract, double position, double marketPrice, double marketValue, double averageCost, double unrealizedPNL, double realizedPNL, string account_name)
        {
            int contract_id = contract.ConId;
            int currPnlIdx = currPnlIndices.ContainsKey(contract_id) ? currPnlIndices[contract_id] : -1;
            if (currPnlIdx != -1 && clientSocket.getCurrentLocalTime().TimeOfDay >= pnlSchedule[currPnlIdx])
            {
                lock (_lock)
                {
                    currPnlIdx++;
                    if (currPnlIdx >= pnlSchedule.Count)
                        currPnlIdx = -1;
                    currPnlIndices[contract_id] = currPnlIdx;
                }
                if (verbose)
                {
                    string m = "Update Portfolio. Contract Id: " + contract.ConId + ", Position: " + position + ", Market Price: " + marketPrice + ", Market Value: " + marketValue + ", Average Cost: " + averageCost + ", Unrealized PnL: " + unrealizedPNL + ", Realized PnL: " + realizedPNL + ", Account Name: " + accountName;
                    msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", contract_id, 0, m);
                    Console.WriteLine(m);
                }
                string subject = "PnL " + account_name + "/" + contract.LocalSymbol;
                string body = "Position:\t" + position.ToString() +
                    "\nTotal PnL:\t" + (realizedPNL + unrealizedPNL).ToString() +
                    "\nRealized PnL:\t" + realizedPNL.ToString() +
                    "\nUnrealized PnL:\t" + unrealizedPNL.ToString() +
                    "\nAverage Cost:\t" + averageCost.ToString() +
                    "\nMarket Price:\t" + marketPrice.ToString();
                msg.sendEmail(subject, body);
            }
        }
        public virtual void updateAccountTime(string timeStamp)
        {
            if (verbose)
            {
                string m = "Update Account Time. Time Stamp: " + timeStamp;
                //msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void accountDownloadEnd(string account_name)
        {
            if (verbose)
            {
                string m = "Account Download End. Account Name: " + account_name;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void accountSummary(int reqId, string account, string tag, string value, string currency)
        {
            if (verbose)
            {
                string m = "Account Summary. Req Id: " + reqId + ", Account: " + account + ", Tag: " + tag + ", Value: " + value + ", Currency: " + currency;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void accountSummaryEnd(int reqId)
        {
            if (verbose)
            {
                string m = "Account Summary End. Req Id: " + reqId;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void accountUpdateMulti(int reqId, string account, string modelCode, string key, string value, string currency)
        {
            //Need to handle this
            //Console.WriteLine("Account Update Multi. Request: " + reqId + ", Account: " + account + ", ModelCode: " + modelCode + ", Key: " + key + ", Value: " + value + ", Currency: " + currency + "\n");
        }
        public virtual void accountUpdateMultiEnd(int reqId)
        {
            if (verbose)
            {
                string m = "Account Update Multi End. Request: " + reqId;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void position(string account, Contract contract, double pos, double avgCost)
        {
            db.runNonQuery("insert into positions values (" + contract.ConId.ToString() + "," + db.getSqlDateTime(clientSocket.getCurrentLocalTime()) + "," + ((int)(pos)).ToString() + "," + avgCost.ToString() + ")");
            if (verbose)
                Console.WriteLine("Position. Account: " + account + ", Contract ID: " + contract.ConId + ", Average Cost: " + avgCost);
        }
        public virtual void positionEnd()
        {
            if (verbose)
            {
                string m = "Position End";
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void positionMulti(int reqId, string account, string modelCode, Contract contract, double pos, double avgCost)
        {
            //need to handle this
            //Console.WriteLine("Position Multi. Request: " + reqId + ", Account: " + account + ", ModelCode: " + modelCode + ", Symbol: " + contract.Symbol + ", SecType: " + contract.SecType + ", Currency: " + contract.Currency + ", Position: " + pos + ", Avg cost: " + avgCost + "\n");
        }
        public virtual void positionMultiEnd(int reqId)
        {
            if (verbose)
            {
                string m = "Position Multi End. Request: " + reqId;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        //Contract Details
        public virtual void contractDetails(int ReqId, ContractDetails contractDetails)
        {
            bool owned = false;
            foreach (var pair in strategies)
                if (pair.Value.hasReqIDWrapper(ReqId))
                {
                    pair.Value.setConstractDetails(ReqId, contractDetails);
                    if (!currPnlIndices.ContainsKey(contractDetails.Summary.ConId))
                    {
                        lock (_lock)
                        {
                            currPnlIndices.Add(contractDetails.Summary.ConId, pnlSchedule.Count > 0 ? 0 : -1);
                        }
                        //int tick_id = tg.get();
                        //lock (_lock)
                        //{
                        //    tickerContractMap.Add(tick_id, con.ConId);
                        //    contractTickMap.Add(con.ConId, tick_id);
                        //}
                        //clientSocket.reqMktData(tick_id, con, "", false, null);
                    }
                    owned = true;
                    break;
                }
            if (!owned)
            {
                lock (_lock)
                    contractDetailsMap.Add(ReqId, contractDetails);
            }
            if (verbose)
            {
                string m = "Contract Details. Req Id: " + ReqId + ", Contract Details: " + contractDetails.Summary;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Contract", contractDetails.UnderConId, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void contractDetailsEnd(int reqId)
        {
            if (verbose)
            {
                string m = "Contract Details End. Req Id: " + reqId;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Contract", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void bondContractDetails(int ReqId, ContractDetails contractDetails)
        {
            if (verbose)
            {
                string m = "Bond Contract Details. Req Id: " + ReqId + ", Contract Details: " + contractDetails.ToString();
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Contract", contractDetails.UnderConId, 0, m);
                Console.WriteLine(m);
            }
        }
        //Executions
        public virtual void execDetails(int reqId, Contract contract, Execution execution)
        {
            //For combo legs the IB contract ID could be different with the db contract ID
            int conId = contract.SecType == "BAG" ? om.getOrderContractID(execution.OrderId) : contract.ConId;
            db.addExecution(conId, execution);
            if (verbose)
                Console.WriteLine("Execution Details. ReqId: " + reqId + ", Contract Id: " + conId + ", Execution: " + execution.ToString());
        }
        public virtual void execDetailsEnd(int reqId)
        {
            if (verbose)
            {
                int contract_id = mm.getContractID(reqId);
                string m = "Execution Details End. Req Id: " + reqId;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Trading", contract_id, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void commissionReport(CommissionReport commissionReport)
        {
            db.runNonQuery("insert into commissions values ('" + commissionReport.ExecId + "'," + db.getSqlDateTime(clientSocket.getCurrentLocalTime()) + "," + commissionReport.Commission.ToString() + ",'" + commissionReport.Currency + "'," + db.getSqlDouble(commissionReport.RealizedPNL) + "," + db.getSqlDouble(commissionReport.Yield) + ",'')");
            if (verbose)
                Console.WriteLine("Commission Report. Report: " + commissionReport.ToString());
        }
        //Market Depth
        public virtual void updateMktDepth(int tickerId, int position, int operation, int side, double price, int size)
        {
            if (verbose)
                Console.WriteLine("Update Market Depth. Ticker Id: " + tickerId + ", Position: " + position + ", Operation: " + operation + ", Side: " + side + ", Price: " + price + ", Size: " + size);
        }
        public virtual void updateMktDepthL2(int tickerId, int position, string marketMaker, int operation, int side, double price, int size)
        {
            if (verbose)
                Console.WriteLine("Update Market Depth L2. Ticker Id: " + tickerId + ", Position: " + position + ", Market Maker: " + marketMaker + ", Operation: " + operation + ", Side: " + side + ", Price: " + price + ", Size: " + size);
        }
        //News Bulletin
        public virtual void updateNewsBulletin(int reqId, int msgType, string message, string origExchange)
        {
            if (verbose)
            {
                int contract_id = mm.getContractID(reqId);
                string m = "News Bulletin. Msg Id: " + reqId + ", Msg Type: " + msgType + ", Message: " + message + ", Original Exchange: " + origExchange;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Finance", contract_id, 0, m);
                Console.WriteLine(m);
            }
        }
        //Financial Advisers
        public virtual void managedAccounts(string accountsList)
        {
            lock(_lock)
            {
                accountName = accountsList;
                foreach (var pair in strategies)
                    pair.Value.account = accountName;
                if(engine_type == "TWS")
                    clientSocket.reqAccountUpdates(true, accountName);
            }
            if (verbose)
            {
                string m = "Managed Accounts. Account list: " + accountsList;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void receiveFA(int faDataType, string faXmlData)
        {
            if (verbose)
            {
                string m = "Receive FA. FA Data Type: " + faDataType.ToString() + ", FA XML Data: " + faXmlData;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Finance", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        //Historical Data
        public virtual void historicalData (int reqId, string date, double open, double high, double low, double close, int volume, int count, double WAP, bool hasGaps)
        {
            try
            {
                foreach (KeyValuePair<int, Strategy> pair in strategies)
                {
                    Strategy strategy = pair.Value;
                    if (strategy.hasReqIDWrapper(reqId))
                    {
                        string date_format = date.Contains(" ") ? "yyyyMMdd  HH:mm:ss" : "yyyyMMdd";
                        ThreadPool.UnsafeQueueUserWorkItem(new WaitCallback(strategy.onHistoricalDataWrapper), new HistoricalDataParameters(reqId, DateTime.ParseExact(date, date_format, CultureInfo.InvariantCulture), open, high, low, close, volume, WAP));
                    }
                }
            }
            catch (Exception ex)
            {
                error(ex);
            }
        }
        public virtual void historicalDataEnd(int reqId, string startDate, string endDate)
        {
            foreach (KeyValuePair<int, Strategy> pair in strategies)
            {
                Strategy strategy = pair.Value;
                if (strategy.hasReqIDWrapper(reqId))
                    ThreadPool.UnsafeQueueUserWorkItem(new WaitCallback(strategy.onHistoricalDataEndWrapper), new HistoricalDataEndParameters(reqId, DateTime.ParseExact(startDate, "yyyyMMdd  HH:mm:ss", CultureInfo.InvariantCulture), DateTime.ParseExact(endDate, "yyyyMMdd  HH:mm:ss", CultureInfo.InvariantCulture)));
            }
        }
        //Market Scanners
        public virtual void scannerParameters(string xml)
        {
            if (verbose)
            {
                string m = "Scanner Parameters. Xml: " + xml;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Market Data", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void scannerData(int reqId, int rank, ContractDetails contractDetails, string distance, string benchmark, string projection, string legsStr)
        {
            if (verbose)
            {
                string m = "Scanner Data. Req Id: " + reqId + ", Rank: " + rank + ", Contract Details: " + contractDetails.ToString() + ", Distance: " + distance + ", Benchmark: " + benchmark + ", Projection: " + projection + ", Legs Str: " + legsStr;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Market Data", contractDetails.UnderConId, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void scannerDataEnd(int reqId)
        {
            if (verbose)
            {
                int contract_id = mm.getContractID(reqId);
                string m = "Scanner Data End.";
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Market Data", contract_id, 0, m);
                Console.WriteLine(m);
            }
        }
        //Real Time Bars
        public virtual void realtimeBar(int reqId, long time, double open, double high, double low, double close, long volume, double wap, int count)
        {
            try
            {
                int contract_id = mm.getContractID(reqId);
                DateTime localTime = Strategy.getLocalTime(baseTime.AddSeconds(time), TimeZoneInfo.Utc);
                //bool recorded = false;
                HashSet<int> owners = mm.getStrategyIDs(reqId);
                foreach (int owner in owners)
                {
                    Strategy strategy = strategies[owner];
                    DateTime exchangeTime = Strategy.getExchangeTime(localTime, strategy.getTimeZone());
                    //if (recordLiveBars && !recorded)
                    //{
                    //    int bar_data_id = dbBarMap[contract_id];
                    //    lock (_lock)
                    //    {
                    //        db.setBar(bar_data_id, exchangeTime, open, high, low, close, volume, wap, overwriteLiveBars);
                    //    }
                    //    recorded = true;
                    //}
                    strategy.enableRealTimeBarEvent();
                    ThreadPool.UnsafeQueueUserWorkItem(new WaitCallback(strategy.onRealtimeBarWrapper), new RealtimeBarParameters(reqId, exchangeTime, open, high, low, close, volume, wap, count));
                }
            }
            catch (Exception ex)
            {
                error(ex);
            }
            if (verbose)
                Console.WriteLine("Real Time Bars. Req Id: " + reqId + ", Time: " + time + ", Open: " + open + ", High: " + high + ", Low: " + low + ", Close: " + close + ", Volume: " + volume + ", Count: " + count + ", WAP: " + wap);
        }
        //Fundamental Data
        public virtual void fundamentalData(int reqId, string data)
        {
            if (verbose)
            {
                int contract_id = mm.getContractID(reqId);
                string m = "Fundamental Data. Req Id: " + reqId + ", Data: " + data;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Market Data", contract_id, 0, m);
                Console.WriteLine(m);
            }
        }
        //Display Groups
        public virtual void displayGroupList(int reqId, string groups)
        {
            if (verbose)
            {
                string m = "Display Group List. Req Id: " + reqId + ", Groups: " + groups;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "System", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public virtual void displayGroupUpdated(int reqId, string contractInfo)
        {
            if (verbose)
            {
                string m = "Display Group Updated. Req Id: " + reqId + ", Contract Info: " + contractInfo;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "System", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public void securityDefinitionOptionParameter(int reqId, int underlyingConId, string tradingClass, string multiplier, HashSet<string> expirations, HashSet<double> strikes)
        {
            //Need to handle this
            //Console.WriteLine("Security Definition Option Parameter. Reqest: {0}, Undrelying contract id: {1}, Trading class: {2}, Multiplier: {3}, Expirations: {4}, Strikes: {5}",
            //                  reqId, underlyingConId, tradingClass, multiplier, string.Join(", ", expirations), string.Join(", ", strikes));
        }
        public void securityDefinitionOptionParameterEnd(int reqId)
        {
            if (verbose)
            {
                string m = "Security Definition Option Parameter End. Request: " + reqId;
                msg.logMessage(clientSocket.getCurrentLocalTime(), 1, "Information", "Account", 0, 0, m);
                Console.WriteLine(m);
            }
        }
        public void connectAck()
        {
            if (IBClientSocket.AsyncEConnect)
                IBClientSocket.startApi();
        }
        public EClientSocket IBClientSocket
        {
            get { return clientSocket as EClientSocket; }
            set { clientSocket = value; }
        }
        public BacktestEngine BacktestClientSocket
        {
            get { return clientSocket as BacktestEngine; }
            set { clientSocket = value; }
        }
    }
}
