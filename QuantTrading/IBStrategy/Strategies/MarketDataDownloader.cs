using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;
using DBAccess;
using IBApi;

namespace IBStrategy
{
    public class MarketDataDownloader : Strategy
    {
        int _bar_length;
        string _source;
        Dictionary<int, DateTime> _end_date_map;
        List<Tuple<int, DateTime, int>> _raw_jobs;  //(ConID, EndDate, Duration)
       
        bool _is_overwrite;
        bool _only_regular_hour;
        bool _use_calendar;
        int _retry_interval;
        int _retry_times;
        string _unit;
        int _max_units_per_try;
        string _date_fmt;
        string _interval_str;
        Dictionary<int, ContractDetails> _details;
        Dictionary<int, TimeZoneInfo> _zone_map;
        Dictionary<int, int> _nunits_map;
        List<Tuple<int,int,DateTime,int>> _failed_jobs;

        bool _pacing_violation;
        bool _is_sleeping;
        int _njobs;
        List<int> _qjobs; //jobs queue
        Dictionary<int, bool> _job_statuses;
        Queue<Bar> _bars;
        Dictionary<int, Tuple<int,int,DateTime,int>> _id_job_map;   //reqId to [DbBarId, job_id, endDate, count]
        Dictionary<int, int> _bar_con_map;   //DbBarId to contract
        public MarketDataDownloader(int strategy_id, string config, IBApi.EClientInterface socket)
            : base(strategy_id, config, socket)
        {
            _db = DBAccess.DBAccess.instance;
            _tg = TickerGenerator.instance;
            _msg = Messenger.instance;
            fromConfig(config);
        }
        public override void fromConfig(string config)
        {
            XmlDocument xml = new XmlDocument();
            xml.LoadXml(config);
            XmlNode xn1 = xml.SelectSingleNode("/Config");

            _bar_length = Int32.Parse(xn1["BarLength"].InnerText);
            _source = xn1["Source"].InnerText;
            _unit = xn1["DurationUnit"].InnerText;
            _is_overwrite = Boolean.Parse(xn1["Overwrite"].InnerText);
            _retry_interval = Int32.Parse(xn1["RetryInterval"].InnerText);
            _retry_times = Int32.Parse(xn1["RetryTimes"].InnerText);
            _max_units_per_try = Int32.Parse(xn1["MaxUnitsPerTry"].InnerText);
            _only_regular_hour = Boolean.Parse(xn1["OnlyRegularHour"].InnerText);
            _use_calendar = false;
            if(xn1["UseCalendar"] != null)
                _use_calendar = Boolean.Parse(xn1["UseCalendar"].InnerText);

            XmlNodeList jobNodes = xn1["Jobs"].GetElementsByTagName("Job");
            _raw_jobs = new List<Tuple<int, DateTime, int>>();
            foreach (XmlNode node in jobNodes)
            {
                int conId = int.Parse(node["ID"].InnerText);
                string dateStr = node["EndDate"].InnerText;
                int nunits = int.Parse(node["Duration"].InnerText);
                DateTime endDate;
                if (dateStr == "Today")
                {
                    if (_bar_length == 86400)
                        endDate = _socket.getCurrentLocalTime().Date;
                    else
                        endDate = _socket.getCurrentLocalTime();
                }
                else
                {
                    endDate = DateTime.ParseExact(dateStr, "yyyy-MM-dd", CultureInfo.InvariantCulture);
                }
                _raw_jobs.Add(new Tuple<int, DateTime, int>(conId, endDate, nunits));
            }

            List<Tuple<int, DateTime, int>> processed_jobs = new List<Tuple<int, DateTime, int>>();
            foreach (var job in _raw_jobs)
            {
                if (job.Item1 >= 0)
                    processed_jobs.Add(job);
                else
                {
                    DateTime startDate = addUnits(job.Item2, -job.Item3);
                    DataTable dt = _db.getNearbyContractSchedule(job.Item1, startDate, job.Item2);
                    foreach(DataRow row in dt.Rows)
                    {
                        DateTime endDate = DateTime.Parse(row["endDate"].ToString());
                        if (job.Item2 < endDate)
                            endDate = job.Item2;
                        int nunits = getUnits(endDate - startDate);
                        int conId = int.Parse(row["contract_id"].ToString());
                        processed_jobs.Add(new Tuple<int, DateTime, int>(conId, endDate, nunits));
                        startDate = endDate;
                    }
                }
            }

            _details = new Dictionary<int, ContractDetails>();
            _end_date_map = new Dictionary<int, DateTime>();
            _bar_con_map = new Dictionary<int, int>();
            _zone_map = new Dictionary<int, TimeZoneInfo>();
            _nunits_map = new Dictionary<int, int>();
            foreach (var job in processed_jobs)
            {
                Contract con = _db.getContract(job.Item1);
                ContractDetails d = _db.getContractDetails(con);
                _details.Add(con.ConId, d);
                int db_bar_id = _db.getBarDataId(con.ConId, _bar_type, _bar_length, _source);
                if (db_bar_id == 0)
                    db_bar_id = _db.addBarDataId(con.ConId, _bar_type, _bar_length, _source, "");
                _bar_con_map.Add(db_bar_id, con.ConId);
                _zone_map.Add(db_bar_id, mapDotNetTimeZone(d.TimeZoneId));
                _end_date_map.Add(db_bar_id, getExchangeTime(job.Item2, _zone_map[db_bar_id]));
                _nunits_map.Add(db_bar_id, job.Item3);
            }
        }
        public override string toConfig()
        {
            StringBuilder sb = new StringBuilder();
            XmlWriterSettings settings = new XmlWriterSettings();
            settings.Encoding = Encoding.UTF8;
            settings.OmitXmlDeclaration = true;
            using(XmlWriter writer = XmlWriter.Create(sb, settings))
            {
                //writer.WriteStartDocument();
                writer.WriteStartElement("Config");
                writeCommonConfig(writer);
                writer.WriteElementString("BarLength", _bar_length.ToString());
                writer.WriteElementString("Source", _source);
                writer.WriteElementString("DurationUnit", _unit);
                writer.WriteElementString("Overwrite", _is_overwrite.ToString());
                writer.WriteElementString("RetryInterval", _retry_interval.ToString());
                writer.WriteElementString("RetryTimes", _retry_times.ToString());
                writer.WriteElementString("MaxUnitsPerTry", _max_units_per_try.ToString());
                writer.WriteElementString("OnlyRegularHour", _only_regular_hour.ToString());
                writer.WriteStartElement("Jobs");
                DateTime today = _socket.getCurrentLocalTime().Date;
                foreach (var job in _raw_jobs)
                {
                    writer.WriteStartElement("Job");
                    writer.WriteElementString("ID", job.Item1.ToString());
                    string endDate = job.Item2.Date == today ? "Today" : job.Item2.Date.ToString("yyyy-MM-dd");
                    writer.WriteElementString("EndDate", endDate);
                    writer.WriteElementString("Duration", job.Item3.ToString());
                    writer.WriteEndElement();
                }
                writer.WriteEndElement();
                writer.WriteEndElement();
                //writer.WriteEndDocument();
            }
            return sb.ToString();
        }
        protected override void initStrategy()
        {
            lock (_lock)
            {
                _id_job_map = new Dictionary<int, Tuple<int, int, DateTime, int>>();
                _qjobs = new List<int>();
                _job_statuses = new Dictionary<int, bool>();
                _bars = new Queue<Bar>();
                _failed_jobs = new List<Tuple<int, int, DateTime, int>>();
                _pacing_violation = false;
                _is_sleeping = false;
                _date_fmt = _unit == "S" ? "yyyyMMdd HH:mm:ss" : "yyyyMMdd";
                if (_bar_length == 86400)
                    _interval_str = "1 day";
                else if (_bar_length == 3600)
                    _interval_str = "1 hour";
                else if (_bar_length > 60)
                    _interval_str = (_bar_length / 60).ToString() + " mins";
                else if (_bar_length == 60)
                    _interval_str = "1 min";
                else if (_bar_length == 1)
                    _interval_str = "1 sec";
                else
                    _interval_str = _bar_length.ToString() + " secs";
                int job_id = 0;
                foreach (KeyValuePair<int, int> pair in _bar_con_map)
                {
                    //int njobs = (int)((_nunits_map[pair.Key] - 1) / _max_units_per_try) + 1;
                    DateTime endDate = _end_date_map[pair.Key];
                    DateTime startDate = addUnits(endDate, -_nunits_map[pair.Key]);
                    while (endDate > startDate)
                    {
                        if (_use_calendar && _unit == "S")
                        {
                            if (endDate.TimeOfDay <= _start_time)
                                endDate = endDate.Date.AddDays(-1) + new TimeSpan(23, 59, 59);
                            while (!_calendar.isBusinessDay(endDate))
                                endDate = endDate.Date.AddDays(-1) + new TimeSpan(23, 59, 59);
                            if (endDate.TimeOfDay > _end_time)
                                endDate = endDate.Date + _end_time;
                            if (endDate <= startDate)
                                break;
                        }
                        int reqId = _tg.get();
                        _id_job_map.Add(reqId, new Tuple<int, int, DateTime, int>(pair.Key, job_id++, endDate, 0));
                        endDate = addUnits(endDate, -_max_units_per_try);
                        _qjobs.Add(reqId);
                        _job_statuses.Add(reqId, false);
                    }
                }
                _njobs = _qjobs.Count();
            }
            _msg.logMessage(_socket.getCurrentLocalTime(), 1, "Information", "Market Data", 0, id, "Market Data Download Started. In total " + _njobs.ToString() + " jobs.");

            Thread tb = new Thread(new ThreadStart(runBarsAgent));
            tb.CurrentCulture = CultureInfo.InvariantCulture;
            tb.CurrentUICulture = CultureInfo.InvariantCulture;
            tb.Start();
            runAllJobs();
        }
        public override bool calibrate(DateTime date, bool read_existing, bool save, bool overwrite_existing)
        {
            return false;
        }
        protected override bool hasReqID(int reqId)
        {
            return _id_job_map == null ? false : _id_job_map.ContainsKey(reqId);
        }
        //Event Handlers
        protected override void onError(int reqId, int errorCode, string errorMsg) 
        {
            //162 -- query returned no data
            //420 -- pacing violation
            var job = _id_job_map[reqId];
            string jobstr = "bar id=" + job.Item1.ToString() + "/job id=" + job.Item2.ToString() + "/end date=" + job.Item3.ToString();
            if (errorCode == 420 || errorMsg.Contains("pacing violation"))
            {
                if (job.Item4 < _retry_times)
                {
                    int newReqId = _tg.get();
                    lock (_lock)
                    {
                        job = new Tuple<int, int, DateTime, int>(job.Item1, job.Item2, job.Item3, job.Item4 + 1);
                        _id_job_map.Add(newReqId, job);
                        _qjobs.Add(newReqId);
                        _job_statuses.Add(newReqId, false);
                        _njobs++;
                        _pacing_violation = true;
                    }
                    _msg.logError(_socket.getCurrentLocalTime(), errorCode, "MktData", 0, id, "Reinitializing " + jobstr + ". " + errorMsg, "");
                }
                else
                {
                    _msg.logError(_socket.getCurrentLocalTime(), errorCode, "MktData", 0, id, "Failed to get bar data for " + jobstr + ". " + errorMsg, "");
                    lock (_lock)
                        _failed_jobs.Add(job);
                }
            }
            else if (errorCode == 162 || errorMsg.Contains("returned no data"))
            {
                _msg.logError(_socket.getCurrentLocalTime(), errorCode, "MktData", 0, id, "Error for " + jobstr + ": " + errorMsg, "");
                lock (_lock)
                {
                    _failed_jobs.Add(job);
                    _job_statuses[reqId] = true;
                }
            }
            else
            {
                _msg.logError(_socket.getCurrentLocalTime(), errorCode, "MktData", 0, id, "Error for " + jobstr + ": " + errorMsg, "");
                lock (_lock)
                    _failed_jobs.Add(job);
            }
        }
        protected override void onHistoricalData(int reqId, DateTime date, double open, double high, double low, double close, int volume, double WAP)
        {
            int id = _id_job_map[reqId].Item1;
            DateTime exchangeDate = _bar_length == 86400 ? date : getExchangeTime(date, _zone_map[id]);
            Bar bar = new Bar(id, exchangeDate, open, high, low, close, volume, WAP);
            lock (_lock)
            {
                _bars.Enqueue(bar);
            }
        }
        protected override void onHistoricalDataEnd(int reqId, DateTime startDate, DateTime endDate)
        {
            var job = _id_job_map[reqId];
            lock (_lock)
                _job_statuses[reqId] = true;
            int completed = _job_statuses.Values.Select(x => x ? 1 : 0).Sum();
            double perc = (double)completed / (double)_job_statuses.Count();
            TimeZoneInfo zone = _zone_map[job.Item1];
            string m = perc.ToString("0.##%") + @" Download Completed: " + job.Item1.ToString() + "/" + job.Item2.ToString() + "/" + job.Item4.ToString() + " from " + getExchangeTime(startDate, zone).ToString(_date_fmt) + " to " + getExchangeTime(endDate, zone).ToString(_date_fmt);
            _msg.logMessage(_socket.getCurrentLocalTime(), 1, "Information", "Market Data", _bar_con_map[job.Item1], id, m);
        }
        private DateTime addUnits(DateTime t, int nunits)
        {
            if (_unit == "S")
                return t.AddSeconds(nunits);
            if (_unit == "D")
                return t.AddDays(nunits);
            if (_unit == "W")
                return t.AddDays(nunits * 7);
            if (_unit == "M")
                return t.AddMonths(nunits);
            if (_unit == "Y")
                return t.AddYears(nunits);
            throw new Exception("Unexpected Duration Unit: " + _unit);
        }
        private int getUnits(TimeSpan t)
        {
            if (_unit == "S")
                return (int)(t.TotalSeconds);
            if (_unit == "D")
                return (int)(t.TotalDays);
            throw new Exception("Unexpected Duration Unit: " + _unit);
        }
        private void runJob(int reqId, Tuple<int,int,DateTime,int> job)
        {
            int dbBarId = job.Item1;
            ContractDetails d = _details[_bar_con_map[dbBarId]];
            Contract con = d.Summary;
            DateTime endDate = job.Item3;
            int nunits = _max_units_per_try;
            string units = nunits.ToString() + " " + _unit;
            _socket.reqHistoricalData(reqId, con, endDate.ToString("yyyyMMdd HH:mm:ss") + " " + d.TimeZoneId, units, _interval_str, _bar_type.ToUpper(), _only_regular_hour ? 1 : 0, 1, new List<TagValue>());
            int irun = job.Item2;
            int itry = job.Item4;
            string m = "Sent Data Downloading Job: " + reqId.ToString() + "/" + dbBarId.ToString() + "/" + irun.ToString() + "/" + itry.ToString() + " ending at " + endDate.ToString(_date_fmt);
            _msg.logMessage(_socket.getCurrentLocalTime(), 1, "Information", "Market Data", con.ConId, id, m);
        }
        private void runAllJobs()
        {
            int count = 0;
            int countDone = 0;
            bool done = false;
            while (!done)
            {
                for (; count < _njobs; ++count)
                {
                    if (count - countDone >= 50)
                        break;
                    int job_id = _qjobs[count];
                    var job = _id_job_map[job_id];
                    runJob(job_id, job);
                    Thread.Sleep(10500);
                }
                if (_pacing_violation)
                {
                    lock (_lock)
                    {
                        _is_sleeping = true;
                    }
                    Thread.Sleep(_retry_interval * 1000);
                    lock (_lock)
                    {
                        _is_sleeping = false;
                        _pacing_violation = false;
                    }
                }
                else
                    Thread.Sleep(1000);
                done = _bars.Count == 0;
                countDone = 0;
                lock (_lock)
                {
                    foreach (var pair in _job_statuses)
                    {
                        if (pair.Value == false)
                            done = false;
                        else
                            countDone++;
                    }
                }
            }
            if(_failed_jobs.Count == 0)
                _msg.logMessage(_socket.getCurrentLocalTime(), 1, "Information", "Market Data", 0, id, "Market Data Download Completed Successfully.");
            else
                _msg.logMessage(_socket.getCurrentLocalTime(), 1, "Warning", "Market Data", 0, id, "Market Data Download Completed with Errors.");
        }
        void runBarsAgent()
        {
            bool done = false;
            while (!done)
            {
                while (_bars.Count > 0)
                {
                    Bar b;
                    lock (_lock)
                    {
                        b = _bars.Dequeue();
                    }
                    _db.setBar(b.id, b.time, b.open, b.high, b.low, b.close, b.volume, b.wap, _is_overwrite);
                }
                done = _bars.Count == 0 && !_is_sleeping;
                if(done)
                {
                    lock (_lock)
                    {
                        foreach (var pair in _job_statuses)
                        {
                            if (pair.Value == false)
                            {
                                done = false;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}
