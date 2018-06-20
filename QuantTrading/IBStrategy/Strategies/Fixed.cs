using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Xml;
using System.Text.RegularExpressions;
using DBAccess;
using IBApi;
using HtmlAgilityPack;
using Utils;

namespace IBStrategy
{
    public class Fixed : Strategy
    {
        TimeSpan _entryStart;
        TimeSpan _entryEnd;
        TimeSpan _exitStart;
        TimeSpan _exitEnd;
        TimeSpan _expiry;
        TimeSpan _bufferTime;
        bool _trendOrReversion;
        int _side;      //+1/0/-1
        int _model_id;
        int _qty;
        int _con_id;
        double _refLevel;
        double _entryOffset;
        double _profitTarget;
        double _stopTarget;
        string _settlementUrl;
        string _settlementXPathLast;
        string _settlementXPathDate;
        string _settlementUrl2;
        string _settlementXPathLast2;
        string _settlementXPathDate2;
        XmlNode _blockConfig;
        OrderTemplate _stopTemplate;

        //states
        DateTime _entryStartTime;
        DateTime _entryEndTime;
        DateTime _exitStartTime;
        DateTime _exitEndTime;
        DateTime _expireTime;
        double _longLevel;
        double _shortLevel;
        Order _entryLongOrd;
        Order _entryShortOrd;
        Order _profitLongOrd;
        Order _stopLongOrd;
        Order _profitShortOrd;
        Order _stopShortOrd;
        Order _expireOrd;
        double _filledQty;
        double _profitQty;
        double _stopQty;
        double _expireQty;
        double _remainingFilledQty;
        double _remainingProfitQty;
        double _remainingStopQty;
        bool _initialized;

        //calibration only
        bool _useClose;
        TimeSpan _refClose;
        bool _calibrationFailed;
        public Fixed(int strategy_id, string config, EClientInterface socket)
            : base(strategy_id, config, socket)
        {
            _calibrationFailed = false;
            _initialized = false;
            fromConfig(config);
        }
        public override void fromConfig(string config)
        {
            XmlDocument xml = new XmlDocument();
            xml.LoadXml(config);
            XmlNode xn1 = xml.SelectSingleNode("/Config");
            _model_id = Int32.Parse(xn1["Model"].InnerText);
            _qty = Int32.Parse(xn1["Quantity"].InnerText);
            _blockConfig = xn1["BlockOrder"];
            var tmpNode = _blockConfig["StopOrderTemplate"];
            if (tmpNode == null)
                _stopTemplate = null;
            else
                _stopTemplate = new OrderTemplate(tmpNode);

            string model_xml = DBAccess.DBAccess.instance.getModel(_model_id);
            XmlDocument xmlm = new XmlDocument();
            xmlm.LoadXml(model_xml);
            XmlNode xn = xmlm.SelectSingleNode("/Parameters");
            _con_id = Int32.Parse(xn["Asset"].InnerText);
            addContract(_con_id);

            string sideStr = xn["Side"].InnerText;
            _side = sideStr == "Long" ? 1 : sideStr == "Short" ? -1 : 0;
            _trendOrReversion = xn["TrendOrReversion"].InnerText == "Trend";
            _entryStart = TimeSpan.Parse(xn["EntryStartTime"].InnerText);
            _entryEnd = TimeSpan.Parse(xn["EntryEndTime"].InnerText);
            _exitStart = TimeSpan.Parse(xn["ExitStartTime"].InnerText);
            _exitEnd = TimeSpan.Parse(xn["ExitEndTime"].InnerText);
            _expiry = TimeSpan.Parse(xn["Expiry"].InnerText);

            _refClose = TimeSpan.Parse(xn["RefCloseTime"].InnerText);
            _useClose = Boolean.Parse(xn["UseClose"].InnerText);

            _entryOffset = Double.Parse(xn["EntryOffset"].InnerText);
            _profitTarget = Double.Parse(xn["ProfitTarget"].InnerText);
            _stopTarget = Double.Parse(xn["StopTarget"].InnerText);

            XmlNode _settlementNode = xn["SettlementPriceHtml"];
            _settlementUrl = _settlementNode["Url"].InnerText;
            _settlementXPathLast = _settlementNode["XPathLast"].InnerText;
            _settlementXPathDate = _settlementNode["XPathDate"].InnerText;

            XmlNode _settlementNode2 = xn["SettlementPriceHtml2"];
            _settlementUrl2 = _settlementNode2["Url"].InnerText;
            _settlementXPathLast2 = _settlementNode2["XPathLast"].InnerText;
            _settlementXPathDate2 = _settlementNode2["XPathDate"].InnerText;

            _bufferTime = new TimeSpan(0, 0, 30); //wait for 30 secs

            //Testing only
            //_entryEnd = new TimeSpan(20, 0, 0);
            //_exitEnd = new TimeSpan(21, 0, 0);
        }
        public override string toConfig()
        {
            StringBuilder sb = new StringBuilder();
            XmlWriterSettings settings = new XmlWriterSettings();
            settings.Encoding = Encoding.UTF8;
            settings.OmitXmlDeclaration = true;
            using (XmlWriter writer = XmlWriter.Create(sb, settings))
            {
                //writer.WriteStartDocument();
                writer.WriteStartElement("Config");
                writeCommonConfig(writer);
                writer.WriteElementString("Model", _model_id.ToString());
                writer.WriteElementString("Quantity", _qty.ToString());
                writer.WriteEndElement();
                //writer.WriteEndDocument();
            }
            return sb.ToString();
        }
        protected override void initStrategy()
        { }
        protected override bool hasReqID(int reqId)
        {
            return false;   //to be implemented
        }
        //Interfaces
        public override bool calibrate(DateTime date, bool read_existing, bool save, bool overwrite_existing)
        {
            string paramsXml = "";
            bool written = false;
            if (read_existing)
                paramsXml = _db.getModelParameters(_model_id, date, true);
            bool has_existing = paramsXml != "";
            if (overwrite_existing || !has_existing)
            {
                //Calibrate model parameters xml here
                DateTime d = _calendar.advance(date, -1, TimeUnit.Days);
                double refLevel = 0;
                if (_useClose == true)
                {
                    int conId = getContractDetails(_con_id).Summary.ConId;
                    int barID = _db.getBarDataId(conId, "trades", 60, "IB");
                    double mid = 0; //This assumes UseClose=true
                    DataTable zonedt = _db.runQuery(@"select time_zone from contracts where id=" + conId.ToString());
                    TimeZoneInfo data_zone = mapDotNetTimeZone(zonedt.Rows[0][0].ToString());
                    while (mid == 0)
                    {
                        DateTime time = getExchangeTime(getLocalTime(d + _refClose, _zone), data_zone);
                        double[] bar = _db.getBar(barID, time);
                        if (bar.Count() > 0)
                            mid = (bar[1] + bar[2]) / 2.0;
                        else
                            d = d.AddDays(-1);
                    }
                    mid = mid + 0.005 - ((mid + 0.005) % 0.01);
                    refLevel = mid;
                }
                else
                { 
                    // get last settlement price
                    if (_socket is BacktestEngine)
                    {
                        int conId = getContractDetails(_con_id).Summary.ConId;
                        int barID = _db.getBarDataId(conId, "trades", 86400, "IB");
                        if (barID == 0)
                        {
                            _calibrationFailed = true;
                            _msg.logError(_socket.getCurrentLocalTime(), 0, "Fixed", getContractDetails(_con_id).Summary.ConId, id, "getBarDataId() failed to retrieve daily bar", "");
                        }
                        else
                        {
                            double[] bar = _db.getBar(barID, d);
                            refLevel = bar[3];
                        }
                    }
                    else
                    {
                        string con_symbol = getContractDetails(_con_id).Summary.LocalSymbol;
                        string mth_symbol = con_symbol.Substring(con_symbol.Length - 2);
                        Regex regExpr = new Regex("_.*_", RegexOptions.IgnoreCase);

                        String symbol_cmegroup = "BB" + mth_symbol; // G7 -> BBG7
                        _settlementXPathLast = regExpr.Replace(_settlementXPathLast, "_" + symbol_cmegroup + "_");
                        _settlementXPathDate = regExpr.Replace(_settlementXPathDate, "_" + symbol_cmegroup + "_");
                        bool ok_cmegroup = getRefPriceFromWeb_cmegroup(_settlementUrl, _settlementXPathLast, _settlementXPathDate, d, out refLevel);
                        if (ok_cmegroup == false)
                        {
                            Regex regExpr2 = new Regex("_.*\"", RegexOptions.IgnoreCase);
                            String symbol_barchart = "CB" + mth_symbol[0] + "1" + mth_symbol[1]; // G7 -> CBG17
                            _settlementXPathLast2 = regExpr2.Replace(_settlementXPathLast2, "_" + symbol_barchart + "\"");
                            bool ok_barchart = getRefPriceFromWeb_barchart(_settlementUrl2, _settlementXPathLast2, _settlementXPathDate2, d, out refLevel);
                            if (ok_barchart == false)
                                _calibrationFailed = true;
                        }
                    }
                    // sanity check on refLevel
                    if (refLevel <= 0 || refLevel > 200)
                    {
                        _calibrationFailed = true;
                        _msg.logError(_socket.getCurrentLocalTime(), 0, "Fixed", getContractDetails(_con_id).Summary.ConId, id, "refLevel is wrong: " + refLevel, "");
                        _msg.sendEmail("refLevel is wrong for Fixed Strategy " + id, "refLevel is wrong: " + refLevel);
                    }
                }

                //Write to xml
                StringBuilder sb = new StringBuilder();
                XmlWriterSettings settings = new XmlWriterSettings();
                settings.Encoding = Encoding.UTF8;
                settings.OmitXmlDeclaration = true;
                using (XmlWriter writer = XmlWriter.Create(sb, settings))
                {
                    //writer.WriteStartDocument();
                    writer.WriteStartElement("Parameters");
                    writer.WriteElementString("Asset", getContractDetails(_con_id).Summary.ConId.ToString());
                    writer.WriteElementString("EntryStartTime", _entryStartTime.ToString(@"HH\:mm\:ss"));
                    writer.WriteElementString("EntryEndTime", _entryEndTime.ToString(@"HH\:mm\:ss"));
                    writer.WriteElementString("ExitStartTime", _exitStartTime.ToString(@"HH\:mm\:ss"));
                    writer.WriteElementString("ExitEndTime", _exitEndTime.ToString(@"HH\:mm\:ss"));
                    writer.WriteElementString("TrendOrReversion", _trendOrReversion ? "Trend" : "Reversion");
                    writer.WriteElementString("Side", _side > 0 ? "Long" : _side < 0 ? "Short" : "Both");
                    writer.WriteElementString("RefLevel", refLevel.ToString());
                    writer.WriteElementString("EntryOffset", _entryOffset.ToString());
                    writer.WriteElementString("ProfitTarget", _profitTarget.ToString());
                    writer.WriteElementString("StopTarget", _stopTarget.ToString());
                    writer.WriteEndElement();
                    //writer.WriteEndDocument();
                }
                paramsXml = sb.ToString();
                if (save)
                {
                    _db.addModelParameters(_model_id, date, paramsXml);
                    written = true;
                }
            }
            //Apply model parameters xml here
            XmlDocument xmlm = new XmlDocument();
            xmlm.LoadXml(paramsXml);
            XmlNode xn = xmlm.SelectSingleNode("/Parameters");
            _profitTarget = Double.Parse(xn["ProfitTarget"].InnerText);
            _stopTarget = Double.Parse(xn["StopTarget"].InnerText);
            _refLevel = Double.Parse(xn["RefLevel"].InnerText);
            _entryOffset = Double.Parse(xn["EntryOffset"].InnerText);

            _longLevel = _trendOrReversion ? _refLevel + _entryOffset : _refLevel - _entryOffset;
            _shortLevel = _trendOrReversion ? _refLevel - _entryOffset : _refLevel + _entryOffset;
            return written;
        }

        private bool getRefPriceFromWeb_barchart(string settlementUrl, string settlementXPathLast, string settlementXPathDate, DateTime prevDate, out double refLevel)
        {
            refLevel = 0;
            bool success = true;
            try
            {
                HtmlWeb web = new HtmlWeb();
                HtmlDocument doc = web.Load(settlementUrl);
                string refLevelString = doc.DocumentNode.SelectNodes(settlementXPathLast)[0].InnerText;
                string publishDateString = doc.DocumentNode.SelectNodes(settlementXPathDate)[0].InnerText;
                refLevel = Convert.ToDouble(refLevelString);
                string[] dateStrings = publishDateString.Split(',');
                string publishMonthString = dateStrings[1].Trim().Split(null)[0];
                string publishDayString = dateStrings[1].Trim().Split(null)[1];
                publishDayString = publishDayString.Remove(publishDayString.Length - 2);
                int publishYear = Convert.ToInt32(dateStrings[2]);
                int publishMonth = DateTime.ParseExact(publishMonthString, "MMM", CultureInfo.InvariantCulture).Month;
                int publishDay = Convert.ToInt32(publishDayString);
                DateTime publishDate = new DateTime(publishYear, publishMonth, publishDay);
                Console.WriteLine("barchart.com refLevelString = {0} publishDate = {1} prevDate = {2}", refLevelString, publishDate, prevDate);
                if (publishDate.Date < prevDate.Date)
                {
                    success = false;
                    _msg.logError(_socket.getCurrentLocalTime(), 0, "Fixed", getContractDetails(_con_id).Summary.ConId, id, "Website publish date is not up-to-date: " + publishDate.Date.ToString(), "");
                    _msg.sendEmail("Web barchart.com failed for Fixed Strategy " + id, "Website publish date is not up-to-date: " + publishDate.Date.ToString());
                }
            }
            catch (Exception e)
            {
                success = false;
                _msg.logError(_socket.getCurrentLocalTime(), 1, "Fixed", getContractDetails(_con_id).Summary.ConId, id, e);
                _msg.sendEmail("Web barchart.com failed for Fixed Strategy " + id, e.ToString());
            }
            return success;
        }

        private bool getRefPriceFromWeb_cmegroup(string settlementUrl, string settlementXPathLast, string settlementXPathDate, DateTime prevDate, out double refLevel)
        {
            refLevel = 0;
            bool success = true;
            try
            {
                HtmlWeb web = new HtmlWeb();
                HtmlDocument doc = web.Load(settlementUrl);
                string refLevelString = doc.DocumentNode.SelectNodes(settlementXPathLast)[0].InnerText;
                string publishDateString = doc.DocumentNode.SelectNodes(settlementXPathDate)[0].InnerText;
                refLevel = Convert.ToDouble(refLevelString);
                DateTime publishDate = DateTime.ParseExact(publishDateString.Trim().ToUpper(), "dd MMM yyyy", CultureInfo.InvariantCulture);
                Console.WriteLine("cmegroup.com refLevelString = {0} publishDate = {1} prevDate = {2}", refLevelString, publishDate, prevDate);
                if (publishDate.Date < prevDate.Date)
                {
                    success = false;
                    _msg.logError(_socket.getCurrentLocalTime(), 0, "Fixed", getContractDetails(_con_id).Summary.ConId, id, "Website publish date is not up-to-date: " + publishDate.Date.ToString(), "");
                    _msg.sendEmail("Web cmegroup.com failed for Fixed Strategy " + id, "Website publish date is not up-to-date: " + publishDate.Date.ToString());
                }
            }
            catch (Exception e)
            {
                success = false;
                _msg.logError(_socket.getCurrentLocalTime(), 1, "Fixed", getContractDetails(_con_id).Summary.ConId, id, e);
                _msg.sendEmail("Web cmegroup.com failed for Fixed Strategy " + id, e.ToString());
            }
            return success;
        }

        protected override void onStartOfDay()
        {
            lock (_lock)
            {
                _entryLongOrd = null;
                _entryShortOrd = null;
                _profitLongOrd = null;
                _profitShortOrd = null;
                _expireOrd = null;
                _stopLongOrd = null;
                _stopShortOrd = null;
                _filledQty = 0;
                _profitQty = 0;
                _stopQty = 0;
                _expireQty = 0;
                _remainingFilledQty = 0;
                _remainingProfitQty = 0;
                _remainingStopQty = 0;
                _entryStartTime = _currDate + _entryStart;
                _entryEndTime = _currDate + _entryEnd;
                _exitStartTime = _currDate + _exitStart;
                _exitEndTime = _currDate + _exitEnd;

                calibrate(_currDate, true, true, false);
                if (!_calibrationFailed)
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
            //if (_entryOrd != null && _entryOrd.Filled() < _entryOrd.Count())
            //{
            //    _entryOrd.assignID(orderId);
            //    return true;
            //}
            //if (_exitOrd != null && _exitOrd.Filled() < _exitOrd.Count())
            //{
            //    _exitOrd.assignID(orderId);
            //    return true;
            //}
            return false;
        }
        public override void onOrderStatus(int orderId, string status, double filled, double remaining, double avgFillPrice, int permId, int parentId, double lastFillPrice, int clientId, String whyHeld) 
        {
            Console.WriteLine(orderId.ToString() + ":\t" + status);
            if(status == "Filled" && filled > 0)
            {
                lock (_lock)
                {
                    if (_entryLongOrd != null && orderId == _entryLongOrd.OrderId)
                    {
                        _expireTime = getExchangeTime(_socket.getCurrentLocalTime(), _zone) + _expiry;
                        _filledQty = filled;
                        _remainingFilledQty = remaining;
                    }
                    else if (_entryShortOrd != null && orderId == _entryShortOrd.OrderId)
                    {
                        _expireTime = getExchangeTime(_socket.getCurrentLocalTime(), _zone) + _expiry;
                        _filledQty = -filled;
                        _remainingFilledQty = remaining;
                    }
                    else if ((_profitLongOrd != null && orderId == _profitLongOrd.OrderId) || (_profitShortOrd != null && orderId == _profitShortOrd.OrderId))
                    {
                        _profitQty = filled;
                        _remainingProfitQty = remaining;
                    }
                    else if ((_stopLongOrd != null && orderId == _stopLongOrd.OrderId) || (_stopShortOrd != null && orderId == _stopShortOrd.OrderId))
                    {
                        _stopQty = filled;
                        _remainingStopQty = remaining;
                    }
                    else if (_expireOrd != null && orderId == _expireOrd.OrderId)
                    {
                        _expireQty = filled;
                    }
                    if (_filledQty != 0 && _profitQty + _stopQty + _expireQty == Math.Abs(_filledQty))
                    {
                        _filledQty = 0;
                        _profitQty = 0;
                        _stopQty = 0;
                        _expireQty = 0;
                        _remainingFilledQty = 0;
                        _remainingProfitQty = 0;
                        _remainingStopQty = 0;
                        _entryLongOrd = null;
                        _entryShortOrd = null;
                        _profitLongOrd = null;
                        _profitShortOrd = null;
                        _stopLongOrd = null;
                        _stopShortOrd = null;
                        _expireOrd = null;
                    }
                }
            }
        }

        protected override void onRealtimeBar(int reqId, DateTime exchangeTime, double open, double high, double low, double close, long volume, double wap, int count)
        {
            if (!_initialized)
                return;
            lock (_lock)
            {
                if (_filledQty != 0)
                {
                    if (_expireOrd == null && (exchangeTime >= _exitEndTime || exchangeTime >= _expireTime))
                    {
                        double qty = _remainingFilledQty;
                        if (_remainingProfitQty > 0)
                            _socket.cancelOrder(_filledQty > 0 ? _profitLongOrd.OrderId : _profitShortOrd.OrderId);
                        else if (_remainingStopQty > 0)
                            _socket.cancelOrder(_filledQty > 0 ? _stopLongOrd.OrderId : _stopShortOrd.OrderId);
                        Order ord = new Order();
                        ord.Action = _filledQty > 0 ? "SELL" : "BUY";
                        ord.TotalQuantity = qty;
                        ord.OrderType = "MKT";
                        _expireOrd = ord;
                        _om.placeOrder(id, getContractDetails(_con_id).Summary, _expireOrd, _con_id);
                    }
                }
                else
                {
                    if (exchangeTime > _entryEndTime)
                    {
                        if (_entryLongOrd != null)
                            _socket.cancelOrder(_entryLongOrd.OrderId);
                        if (_entryShortOrd != null)
                            _socket.cancelOrder(_entryShortOrd.OrderId);
                    }
                    if (_entryLongOrd == null && _entryShortOrd == null && exchangeTime >= _entryStartTime && exchangeTime <= _entryEndTime)
                    {
                        string ocaId = id.ToString() + "_E_" + exchangeTime.ToString("yyyyMMdd_hhmmss");
                        if (_side >= 0 && _entryLongOrd == null && ((_trendOrReversion && _longLevel > high) || (!_trendOrReversion && _longLevel < low))) //long side
                        {
                            Order ord = new Order();
                            ord.Action = "BUY";
                            ord.TotalQuantity = _qty;
                            if (_trendOrReversion)
                            {
                                ord.OrderType = "STP";
                                ord.AuxPrice = _longLevel;
                            }
                            else
                            {
                                ord.OrderType = "LMT";
                                ord.LmtPrice = _longLevel;
                            }
                            ord.OcaType = 1;    //reduce others qty with filled qty, overfill protection
                            ord.OcaGroup = ocaId;
                            ord.Transmit = true;
                            //_entryLongOrd = new BlockOrder(_socket, _zone, _con, ord, _entryEndTime, _unitSize, _maxBlockSize);
                            _entryLongOrd = ord;
                            List<Order> ordSet = OrderFactory.createBracketOrder(_entryLongOrd, _longLevel + _profitTarget, _longLevel - _stopTarget, DateTime.MinValue, null, _stopTemplate, null);
                            _profitLongOrd = ordSet[1];
                            _stopLongOrd = ordSet[2];
                            _om.placeBracketOrders(id, getContractDetails(_con_id).Summary, ordSet, _con_id);
                        }
                        if (_side <= 0 && _entryShortOrd == null && ((_trendOrReversion && _shortLevel < low) || (!_trendOrReversion && _shortLevel > high))) //short side
                        {
                            Order ord = new Order();
                            ord.Action = "SELL";
                            ord.TotalQuantity = _qty;
                            if (_trendOrReversion)
                            {
                                ord.OrderType = "STP";
                                ord.AuxPrice = _shortLevel;
                            }
                            else
                            {
                                ord.OrderType = "LMT";
                                ord.LmtPrice = _shortLevel;
                            }
                            ord.OcaType = 1;    //reduce others qty with filled qty, overfill protection
                            ord.OcaGroup = ocaId;
                            ord.Transmit = true;
                            //_entryShortOrd = new BlockOrder(_socket, _zone, _con, ord, _entryEndTime, _unitSize, _maxBlockSize);
                            _entryShortOrd = ord;
                            List<Order> ordSet = OrderFactory.createBracketOrder(_entryShortOrd, _shortLevel - _profitTarget, _shortLevel + _stopTarget, DateTime.MinValue, null, _stopTemplate, null);
                            _profitShortOrd = ordSet[1];
                            _stopShortOrd = ordSet[2];
                            _om.placeBracketOrders(id, getContractDetails(_con_id).Summary, ordSet, _con_id);
                        }
                    }
                }
            }
        }
    }
}
