using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using IBApi;
using System.Text;
using Utils;

namespace DBAccess
{
    public class DBAccess
    {
        SqlConnection conn;
        HashSet<string> validErrs;
        private readonly Object _lock = new Object();
        public DBAccess()
        {
            validErrs = new HashSet<string>();
            validErrs.Add("Internal connection fatal error.");
            validErrs.Add("There is already an open DataReader associated with this Command which must be closed first.");
            validErrs.Add("Timeout expired.  The timeout period elapsed prior to completion of the operation or the server is not responding.");
        }
        public void connect(string server, string database)
        {
            conn = new SqlConnection();
            conn.ConnectionString = @"Server=" + server + "; Database=" + database + "; Integrated Security=True;";
            conn.Open();
        }
        public void runNonQuery(string query)
        {
            lock (_lock)
            {
                using (SqlCommand command = new SqlCommand(query, conn))
                {
                    while (true)
                    {
                        try
                        {
                            command.ExecuteNonQuery();
                            break;
                        }
                        catch (Exception ex)
                        {
                            if (!validErrs.Contains(ex.Message))
                            {
                                Exception newex = new Exception("Query: " + query + "\r\n" + ex.Message);
                                newex.Data["StackTrace"] = ex.StackTrace;
                                throw newex;
                            }
                            Thread.Sleep(100);
                        }
                    }
                }
            }
        }

        public DataTable runQuery(string query)
        {
            DataTable dt = new DataTable();
            lock (_lock)
            {
                using (SqlDataAdapter a = new SqlDataAdapter(query, conn))
                {
                    while (true)
                    {
                        try
                        {
                            a.Fill(dt);
                            break;
                        }
                        catch (Exception ex)
                        {
                            if (!validErrs.Contains(ex.Message))
                            {
                                Exception newex = new Exception("Query: " + query + "\r\n" + ex.Message);
                                newex.Data["StackTrace"] = ex.StackTrace;
                                throw newex;
                            }
                            Thread.Sleep(100);
                        }
                    }
                }
            }
            return dt;
        }
        public string getSqlString(string s)
        {
            if (s == null)
                return "null";
            return "'" + s.Replace("'", "''").Replace(@"\", @"\\") + "'";
        }
        public string getSqlLikeString(string s)
        {
            if (s == null)
                return "null";
            return "'" + s.Replace("'", "''").Replace(@"%", @"[%]").Replace("_", "[_]") + "'";
        }
        public string getSqlDouble(double v)
        {
            return Double.IsNaN(v) || Double.IsInfinity(v) || v > 1e300 || v < -1e300 ? "null" : v.ToString();
        }
        public string getSqlDateTime(DateTime dt)
        {
            return "'" + dt.ToString("yyyy-MM-dd HH:mm:ss:fff") + "'";
        }
        public string getSqlDate(DateTime dt)
        {
            return "'" + dt.ToString("yyyy-MM-dd") + "'";
        }
        public void createCalendars()
        {
            string command = @"
create table calendars
(
id int not null unique,
name varchar(16) not null,
primary key(id)
);";
            runNonQuery(command);
        }
        public void createHolidays()
        {
            string command = @"
create table holidays
(
id int not null,
date date not null,
comment varchar(64)
);";
            runNonQuery(command);
        }
        public void createExchanges()
        {
            string command = @"
create table exchanges
(
id int not null unique,
short_name varchar(16) not null,
name varchar(256),
open_time datetime,
close_time datetime,
time_zone varchar(64),
country varchar(64),
comment varchar(256)
primary key(id)
);
create index P1 on exchanges (id)
create index P2 on exchanges (short_name)";
            runNonQuery(command);
        }
        public void createContracts()
        {
            string command = @"
create table contracts
(
id int not null unique,
symbol varchar(16) not null,
exchange varchar(16),
calendar varchar(16),
sec_type varchar(8),
open_time varchar(16),
close_time varchar(16),
time_zone varchar(64),
lot_size int,
tick_size float,
is_combo bit,
expiry datetime,
strike float,
parity varchar(8),
primary key(id)
);
create index P1 on contracts (id)
create index P2 on contracts (symbol)";
            runNonQuery(command);
        }
        public void createContractDetails()
        {
            string command = @"
create table contract_details
(
id int not null unique,
long_name varchar(256),
currency varchar(8),
trading_class varchar(32),
industry varchar(64),
category varchar(64),
sub_category varchar(64),
sec_id_type varchar(32),
sec_id varchar(32)
);
create index P1 on contract_details (id)";
            runNonQuery(command);
        }
        public void createCombos()
        {
            string command = @"
create table combos
(
id int not null,
leg_id int not null,
weight int not null
);
create unique index P1 on combos (id, leg_id)";
            runNonQuery(command);
        }
        public void createBarDataList()
        {
            string command = @"
create table bar_data_list
(
id int not null unique,
contract_id int not null,
bar_type varchar(16),
bar_length int not null,
source varchar(16),
comment varchar(256)
);
create index P1 on bar_data_list (id)";
            runNonQuery(command);
        }
        public void createBarData()
        {
            string command = @"
create table bar_data
(
id int not null,
time datetime not null,
[open] float,
high float,
low float,
[close] float,
volume float,
wap float
);
create unique nonclustered index P1 on bar_data (id, time)";
            runNonQuery(command);
        }
        public void createTickDataList()
        {
            string command = @"
create table tick_data_list
(
id int not null unique,
contract_id int not null,
source varchar(16),
comment varchar(256)
);
create index P1 on tick_data_list(id)";
            runNonQuery(command);
        }
        public void createTickData()
        {
            string command = @"
create table tick_data
(
id int not null,
tick_type int not null,
time datetime not null,
value float not null
);";
            runNonQuery(command);
        }
        public void createModels()
        {
            string command = @"
create table models
(
id int not null unique,
contract_id int not null,
model_name varchar(32),
description varchar(256),
calibration_parameters xml not null
);
create index P1 on models (id)";
            runNonQuery(command);
        }
        public void createModelParameters()
        {
            string command = @"
create table model_parameters
(
model_id int not null,
calib_date date not null,
parameters xml not null
);";
            runNonQuery(command);
        }
        public void createStrategies()
        {
            string command = @"
create table strategies
(
id int not null unique,
name varchar(32) not null,
class varchar(32) not null,
description varchar(256),
config xml not null
);
create index P1 on strategies (id)
create index P2 on strategies (name)";
            runNonQuery(command);
        }
        public void createOrders()
        {
            string command = @"
create table orders
(
id int not null,
contract_id int not null,
timestamp datetime not null,
quantity int not null,
order_type varchar(16) not null,
limit_price float,
stop_price float,
time_in_force varchar(16),
strategy_id int,
parent_id int,
oca_group varchar(64),
oca_type int,
broker_id varchar(32),
comment varchar(256),
account varchar(16)
);";
            runNonQuery(command);
        }
        public void createOrderStatus()
        {
            string command = @"
create table order_status
(
order_id int not null,
timestamp datetime not null,
status varchar(32) not null,
filled_quantity int,
avg_fill_price float,
description varchar(256)
);";
            runNonQuery(command);
        }
        public void createExecutions()
        {
            string command = @"
create table executions
(
id varchar(32) not null unique,
order_id int not null,
contract_id int not null,
timestamp datetime not null,
filled_quantity int not null,
filled_price float not null,
exchange varchar(32),
account varchar(16)
);";
            runNonQuery(command);
        }
        public void createCommissions()
        {
            string command = @"
create table commissions
(
execution_id varchar(32) not null,
timestamp datetime not null,
commission float not null,
currency varchar(8),
realized_pnl float,
yield float,
comment varchar(256)
);";
            runNonQuery(command);
        }
        public void createAccounts()
        {
            string command = @"
create table accounts
(
account_id varchar(32) not null,
login_id varchar(16) not null,
broker varchar(32) not null,
account_type varchar(16) not null,
description varchar(256)
);";
            runNonQuery(command);
        }
        public void createPositions()
        {
            string command = @"
create table positions
(
contract_id int not null,
timestamp datetime not null,
quantity int not null,
avg_cost float not null,
strategy_id int,
account varchar(16)
);";
            runNonQuery(command);
        }
        public void createStrategyPositions()
        {
            string command = @"
create table strategy_positions
(
position_id int not null,
contract_id int not null,
strategy_id int not null,
timestamp datetime not null,
quantity int not null,
avg_cost float not null,
position_type varchar(2),
description varchar(256),
account varchar(16)
);";
            runNonQuery(command);
        }
        public void createErrors()
        {
            string command = @"
create table errors
(
timestamp datetime not null,
code int,
type varchar(32),
contract_id int,
strategy_id int,
message varchar(8000),
stack varchar(8000)
);";
            runNonQuery(command);
        }
        public void createSystemLog()
        {
            string command = @"
create table system_log
(
timestamp datetime not null,
code int,
level varchar(16),
type varchar(32),
contract_id int,
strategy_id int,
message varchar(1024)
);";
            runNonQuery(command);
        }
        public void createEvents()
        {
            string command = @"
create table events
(
name varchar(32) not null,
time datetime not null,
comment varchar(256)
);";
            runNonQuery(command);
        }

        public void createNearbyContracts()
        {
            string command = @"
create table nearby_contracts
(
id int not null check (id < 0),
startDate Date not null,
endDate Date not null,
contract_id int not null,
);
create unique index P1 on nearby_contracts (id, startDate, endDate)";
            runNonQuery(command);
        }

        public int getTickDataId(int contract_id, string source)
        {
            string query = @"select id from tick_data_list where contract_id = " + contract_id.ToString() + " and source = '" + source + "'";
            DataTable dt = runQuery(query);
            if (dt.Rows.Count == 0)
                return 0;
            return Int32.Parse(dt.Rows[0][0].ToString());
        }
        public int addTickDataId(int contract_id, string source, string comment)
        {
            string query = @"select max(id) from tick_data_list";
            string ret = runQuery(query).Rows[0][0].ToString();
            int id = ret == "" ? 1 : Int32.Parse(ret) + 1;
            string nonquery = "insert into tick_data_list values (" + id.ToString() + "," + contract_id.ToString() + ",'" + source + "'," + getSqlString(comment) + ")";
            runNonQuery(nonquery);
            return id;
        }
        public int getBarDataId(int contract_id, string bar_type, int bar_length, string source)
        {
            string query = @"select id from bar_data_list where contract_id = " + contract_id.ToString() + " and bar_type = '" + bar_type + "' and bar_length = " + bar_length.ToString() + " and source = '" + source + "'";
            DataTable dt = runQuery(query);
            if (dt.Rows.Count == 0)
                return 0;
            return Int32.Parse(dt.Rows[0][0].ToString());
        }
        public int addBarDataId(int contract_id, string bar_type, int bar_length, string source, string comment)
        {
            string query = contract_id > 0 ? @"select max(id) from bar_data_list where id > 0" : @"select min(id) from bar_data_list where id < 0";
            string ret = runQuery(query).Rows[0][0].ToString();
            int id = ret == "" ? (contract_id > 0 ? 1 : -1) : Int32.Parse(ret) + (contract_id > 0 ? 1 : -1);
            string nonquery = "insert into bar_data_list values (" + id.ToString() + "," + contract_id.ToString() + ",'" + bar_type + "'," + bar_length.ToString() + ",'" + source + "'," + getSqlString(comment) + ")";
            runNonQuery(nonquery);
            return id;
        }
        public Tuple<int, string, int, string> getBarDataInfo(int barID)
        {
            string query = @"select * from bar_data_list where id = " + barID.ToString();
            DataTable dt = runQuery(query);
            if (dt.Rows.Count == 0)
                return null;
            DataRow r = dt.Rows[0];
            return new Tuple<int, string, int, string>(Int32.Parse(r["contract_id"].ToString()), r["bar_type"].ToString(), Int32.Parse(r["bar_length"].ToString()), r["source"].ToString());
        }
        public DataTable getHistoricalData(int bar_data_id, DateTime startTime, DateTime endTime, String field = "all", bool includeEndTime = false)
        {
            if (bar_data_id < 0)
                return getNearbyContractHistoricalData(bar_data_id, startTime, endTime, field, includeEndTime);
            string query = " from bar_data where id=" + bar_data_id.ToString() + " and time>=" + getSqlDateTime(startTime) + " and time <" + getSqlDateTime(endTime) + " order by time";
            if (includeEndTime == true)
                query = " from bar_data where id=" + bar_data_id.ToString() + " and time>=" + getSqlDateTime(startTime) + " and time <=" + getSqlDateTime(endTime) + " order by time";
            if (field == "all")
                query = "select time, [open], high, low, [close], volume, wap" + query;
            else
                query = "select time, [" + field + "]" + query;
            return runQuery(query);
        }
        public double[] getBar(int bar_data_id, DateTime time, String field = "all")
        {
            string query = " from bar_data where id=" + bar_data_id.ToString() + " and time=" + getSqlDateTime(time);
            if (field == "all")
                query = "select [open], high, low, [close], volume, wap" + query;
            else
                query = "select [" + field + "]" + query;
            DataTable dt = runQuery(query);
            if(dt.Rows.Count == 0)
                return new double[0];
            return dt.Rows[0].ItemArray.Select(x=>x.ToString()).Select(x=>x==""?-1.0:Double.Parse(x)).ToArray();
        }
        public bool setBar(int bar_data_id, DateTime time, double open, double high, double low, double close, double volume, double wap, bool overwrite = false)
        {
            double[] bar = getBar(bar_data_id, time, "all");
            bool existing = bar.Count() != 0;
            if (existing && overwrite)
            {
                if(bar[0]!=open || bar[1] != high || bar[2] != low || bar[3] != close || bar[4] != volume || bar[5] != wap)
                    runNonQuery("update bar_data set [open] = " + open.ToString() + ", high = " + high.ToString() + ", low = " + low.ToString() + ", [close] = " + close.ToString() + ", volume = " + volume.ToString() + ", wap = " + wap.ToString()
                        + " where id=" + bar_data_id.ToString() + " and time=" + getSqlDateTime(time));
            }
            else if (!existing)
                runNonQuery("insert into bar_data values (" + bar_data_id.ToString() + "," + getSqlDateTime(time) + "," + open.ToString() + "," + high.ToString() + "," + low.ToString() + "," + close.ToString() + "," + volume.ToString() + "," + wap.ToString() + ")");
            return existing;
        }
        public string getModel(int model_id)
        {
            string query = @"select calibration_parameters from models where id = " + model_id.ToString();
            return runQuery(query).Rows[0][0].ToString();
        }
        public string getModelParameters(int model_id, DateTime date, bool exact = false)
        {
            string query = exact ? 
                @"select top 1 parameters from model_parameters where model_id = " + model_id.ToString() + " and calib_date = " + getSqlDate(date) + " order by calib_date desc" :
                @"select top 1 parameters from model_parameters where model_id = " + model_id.ToString() + " and calib_date <= " + getSqlDate(date) + " order by calib_date desc";
            DataTable ret = runQuery(query);
            if (ret.Rows.Count == 0)
                return "";
            return ret.Rows[0][0].ToString();
        }
        public List<string> getStrategyInfo(int strategy_id)
        {
            string query = @"select class, config from strategies where id = " + strategy_id.ToString();
            var results = runQuery(query).Rows[0];
            List<string> ret = new List<string>();
            ret.Add(results[0].ToString());
            ret.Add(results[1].ToString());
            return ret;
        }
        public int addModel(int conId, string name, string xml, string description)
        {
            string query = @"select max(id) from models";
            string ret = runQuery(query).Rows[0][0].ToString();
            int id = ret == "" ? 1 : Int32.Parse(ret) + 1;
            string nonquery = "insert into models values (" + id.ToString() + "," + conId.ToString() + ",'" + name + "'," + getSqlString(description) + "," + getSqlString(xml) + ")";
            runNonQuery(nonquery);
            return id;
        }
        public void addModelParameters(int model_id, DateTime calib_date, string xml)
        {
            string nonquery = "insert into model_parameters values (" + model_id.ToString() + "," + getSqlDate(calib_date) + "," + getSqlString(xml) + ")";
            runNonQuery(nonquery);
        }

        public int addNearbyContract(string symbol, string sec_type, string exchange, string description)
        {
            string query = @"select top(1) * from contracts where symbol=" + getSqlString(symbol) + " and sec_type=" + getSqlString(sec_type)
                + " and exchange=" + getSqlString(exchange) + " order by expiry desc";
            DataTable dt = runQuery(query);
            if (dt.Rows.Count == 0)
                return 0;
            DataRow row = dt.Rows[0];
            string queryd = @"select * from contract_details where id=" + row["id"].ToString();
            DataTable dtd = runQuery(queryd);
            if (dtd.Rows.Count == 0)
                return 0;
            string cal = row["calendar"].ToString();
            string open_time = row["open_time"].ToString();
            string close_time = row["close_time"].ToString();
            string time_zone = row["time_zone"].ToString();
            string lot_size = row["lot_size"].ToString();
            string tick_size = row["tick_size"].ToString();
            query = @"select min(id) from contracts";
            DataTable dtid = runQuery(query);
            int id = dtid.Rows.Count == 0 ? -1 : Int32.Parse(dtid.Rows[0][0].ToString()) - 1;
            if (id >= 0)
                id = -1;
            runNonQuery("insert into contracts values (" + id.ToString() + ", '" + symbol + "', '" + exchange + "', '" + cal + "', '" + sec_type + "', '" + open_time
                + "', '" + close_time + "', '" + time_zone + "', " + lot_size + ", " + tick_size
                + ", null, null, 0, null)");
            DataRow rowd = dtd.Rows[0];
            string ccy = rowd["currency"].ToString();
            string trading_class = rowd["trading_class"].ToString();
            string industry = rowd["industry"].ToString();
            string category = rowd["category"].ToString();
            string sub_category = rowd["sub_category"].ToString();
            string sec_id_type = rowd["sec_id_type"].ToString();
            string sec_id = rowd["sec_id"].ToString();
            runNonQuery("insert into contract_details values (" + id.ToString() + ", " + getSqlString(description) + ", '" + ccy + "', " + getSqlString(trading_class) + ", " + getSqlString(industry)
                + ", " + getSqlString(category) + ", " + getSqlString(sub_category) + ", " + getSqlString(sec_id_type) + ", " + getSqlString(sec_id) + ")");
            return id;
        }
        public DataTable getNearbyContractSchedule(int conId, DateTime? startDate = null, DateTime? endDate = null)
        {
            string query = @"select contract_id, startDate, endDate from nearby_contracts where id = " + conId.ToString();
            if (startDate != null)
                query += " and endDate >= " + getSqlDate(((DateTime)startDate).Date);
            if (endDate != null)
                query += " and startDate <= " + getSqlDate(((DateTime)endDate).Date);
            query += " order by startDate";
            return runQuery(query);
        }
        public void setNearbyContractSchedule(int conId, int nearbyNum, int indexOffset = 1, bool append_mode = false)
        {
            string preq = @"select symbol, sec_type, exchange, calendar from contracts where id=" + conId.ToString();
            DataRow prerow = runQuery(preq).Rows[0];
            string symbol = prerow["symbol"].ToString();
            string sec_type = prerow["sec_type"].ToString();
            string exchange = prerow["exchange"].ToString();
            string cal_str = prerow["calendar"].ToString();
            string query = @"select * from contracts where symbol=" + getSqlString(symbol) + " and sec_type=" + getSqlString(sec_type)
                + " and exchange=" + getSqlString(exchange) + " and id > 0 order by expiry asc";
            DataTable dt = runQuery(query);
            List<DateTime> startDates = new List<DateTime>();
            List<DateTime> endDates = new List<DateTime>();
            List<int> contractIds = new List<int>();

            DateTime startDate;
            DateTime endDate = DateTime.Today;
            DataRow row;

            Utils.Calendar calendar = mapCalendar(cal_str);

            int numRows = dt.Rows.Count;
            for (int i = 0; i<numRows; ++i)
            {
                if (i == nearbyNum-1)
                {
                    row = dt.Rows[i];
                    endDate = DateTime.Parse(dt.Rows[i]["expiry"].ToString() );
                    endDate = calendar.advance(endDate, -indexOffset, TimeUnit.Days);
                    startDate = endDate.AddMonths(-1);

                    startDates.Add(startDate);
                    endDates.Add(endDate);
                    contractIds.Add(Int32.Parse(dt.Rows[i]["id"].ToString() ) );
                }
                else if ( i >= nearbyNum)
                {
                    startDate = calendar.advance(endDate, 1, TimeUnit.Days);
                    //endDate = DateTime.ParseExact(dt.Rows[i]["expiry"].ToString(), "yyyy-MM-dd  HH:mm:ss", CultureInfo.InvariantCulture);
                    endDate = DateTime.Parse(dt.Rows[i]["expiry"].ToString());
                    endDate = calendar.advance(endDate, -indexOffset, TimeUnit.Days);

                    startDates.Add(startDate);
                    endDates.Add(endDate);
                    contractIds.Add(Int32.Parse(dt.Rows[i]["id"].ToString()));
                }                
            }
            if(!append_mode)
            {
                string clearquery = @"delete from nearby_contracts where id = " + conId.ToString();
                runNonQuery(clearquery);
            }
            for (int i = 0; i < startDates.Count; ++i)
            {
                string nonquery = "insert into nearby_contracts values (" + conId.ToString() + "," + getSqlDate(startDates[i]) + "," + getSqlDate(endDates[i]) + "," + contractIds[i].ToString() + ")";
                runNonQuery(nonquery);
            }
        }
        public int addStrategy(string cls, string name, string xml, string description)
        {
            string query = @"select max(id) from strategies";
            string ret = runQuery(query).Rows[0][0].ToString();
            int id = ret == "" ? 1 : Int32.Parse(ret) + 1;
            string nonquery = "insert into strategies values (" + id.ToString() + ",'" + name + "','" + cls + "','" + description + "'," + getSqlString(xml) + ")";
            runNonQuery(nonquery);
            return id;
        }
        public ContractDetails getContractDetails(Contract con)
        {

            DataTable dt = runQuery("select c.time_zone as TimeZoneId, c.tick_size as MinTick, cd.long_name as LongName, cd.industry as Industry, cd.category as Category, cd.sub_category as Subcategory from contracts c inner join contract_details cd on c.id = cd.id where c.id = " + con.ConId.ToString());
            if (dt.Rows.Count == 0)
                return null;
            ContractDetails d = new ContractDetails();
            d.Summary = con;
            d.TimeZoneId = dt.Rows[0]["TimeZoneId"].ToString();
            d.MinTick = Double.Parse(dt.Rows[0]["MinTick"].ToString());
            d.LongName = dt.Rows[0]["LongName"] as String;
            d.Industry = dt.Rows[0]["Industry"] as String;
            d.Category = dt.Rows[0]["Category"] as String;
            d.Subcategory = dt.Rows[0]["Subcategory"] as String;
            return d;
        }
        public Contract getContractByDate(int contract_id, DateTime date)
        {
            if(contract_id < 0) //Continuous contract
            {
                string dateStr = getSqlDate(date);
                string query = @"select top(1) contract_id from nearby_contracts where id=" + contract_id.ToString() + " and startDate<=" + dateStr + " and endDate>=" + dateStr;
                DataTable dt = runQuery(query);
                if (dt.Rows.Count == 0)
                    return null;
                contract_id = int.Parse(dt.Rows[0][0].ToString());
            }
            return getContract(contract_id);
        }
        public Contract getContract(int contract_id)
        {
            string query = @"select c.symbol as symbol, c.exchange as exchange, cd.currency as currency, c.sec_type as sec_type, c.expiry as expiry, c.lot_size as lot_size, c.strike as strike, c.parity as parity, cd.trading_class as trading_class, cd.sec_id as sec_id, cd.sec_id_type as sec_id_type from contracts c, contract_details cd where c.id = cd.id and c.id = " + contract_id.ToString();
            DataTable dt = runQuery(query);
            if (dt.Rows.Count == 0)
                return null;
            Contract c = new Contract();
            c.ConId = contract_id;
            c.Symbol = dt.Rows[0]["symbol"].ToString();
            c.LocalSymbol = c.Symbol;
            c.SecType = dt.Rows[0]["sec_type"].ToString();
            c.Currency = dt.Rows[0]["currency"].ToString();
            c.Exchange = dt.Rows[0]["exchange"].ToString();
            c.Multiplier = dt.Rows[0]["lot_size"] as String;
            c.Strike = Double.Parse(dt.Rows[0]["strike"].ToString());
            c.Right = dt.Rows[0]["parity"] as String;
            c.TradingClass = dt.Rows[0]["trading_class"] as String;
            c.SecId = dt.Rows[0]["sec_id"] as String;
            c.SecIdType = dt.Rows[0]["sec_id_type"] as String;
            if (c.SecType == "FUT")
                c.LastTradeDateOrContractMonth = DateTime.Parse(dt.Rows[0]["expiry"].ToString()).ToString("yyyyMM");
            return c;
        }

        public bool setContract(ContractDetails details, string cal, TimeSpan open_time, TimeSpan close_time, bool overwrite = false)
        {
            Contract con = details.Summary;
            Contract existingCon = getContract(con.ConId);
            bool existing = existingCon != null;
            int lot_size = con.Multiplier == null ? 1 : Int32.Parse(con.Multiplier);
            if (existing && overwrite)
            {
                runNonQuery("update contracts set id = " + con.ConId.ToString() + ", symbol = '" + con.Symbol + "', exchange = '" + con.Exchange + "', calendar = '" + cal + "', sec_type = '" + con.SecType + "', open_time = '" + open_time.ToString(@"hh\:mm\:ss") 
                    + "', close_time = '" + close_time.ToString(@"hh\:mm\:ss") + "', time_zone = '" + details.TimeZoneId + "', lot_size = " + lot_size.ToString() + ", tick_size = " + details.MinTick.ToString() 
                    + ", is_combo = null, expiry = " + getSqlString(con.LastTradeDateOrContractMonth) + ", strike = " + getSqlDouble(con.Strike) + ", parity = " + getSqlString(con.Right)
                    + " where id=" + con.ConId.ToString());
                runNonQuery("update contract_details set id = " + con.ConId.ToString() + ", long_name = " + getSqlString(details.LongName) + ", currency = '" + con.Currency + "', trading_class = " + getSqlString(con.TradingClass) + ", industry = " + getSqlString(details.Industry)
                    + ", category = " + getSqlString(details.Category) + ", sub_category = " + getSqlString(details.Subcategory) + ", sec_id_type = " + getSqlString(con.SecIdType) + ", sec_id = " + getSqlString(con.SecId)
                    + " where id=" + con.ConId.ToString());
                return true;
            }
            if (!existing)
            {
                runNonQuery("insert into contracts values (" + con.ConId.ToString() + ", '" + con.Symbol + "', '" + con.Exchange + "', '" + cal + "', '" + con.SecType + "', '" + open_time.ToString(@"hh\:mm\:ss")
                    + "', '" + close_time.ToString(@"hh\:mm\:ss") + "', '" + details.TimeZoneId + "', " + lot_size + ", " + details.MinTick.ToString()
                    + ", null, " + getSqlString(con.LastTradeDateOrContractMonth) + ", " + getSqlDouble(con.Strike) + ", " + getSqlString(con.Right) + ")");
                runNonQuery("insert into contract_details values (" + con.ConId.ToString() + ", " + getSqlString(details.LongName) + ", '" + con.Currency + "', " + getSqlString(con.TradingClass) + ", " + getSqlString(details.Industry)
                    + ", " + getSqlString(details.Category) + ", " + getSqlString(details.Subcategory) + ", " + getSqlString(con.SecIdType) + ", " + getSqlString(con.SecId) + ")");
                return true;
            }
            return false;
        }

        public Contract getFutureContract(string symbol, DateTime currDate, int numNearby)
        {
            string query = @"select c.expiry from contracts c where c.symbol = '" + symbol + "' order by expiry asc";
            DataTable dt = runQuery(query);
            if (dt.Rows.Count == 0)
                return null;

            List<DateTime> expiries = new List<DateTime>();

            for (int i=0; i<dt.Rows.Count; ++i)
            {
                expiries.Add( DateTime.Parse( dt.Rows[i]["expiry"].ToString() ) );
            }

            int contractInd = expiries.Count - 1;
            bool hasFound = false;

            if (expiries[contractInd - numNearby + 1] <= currDate)
                throw new Exception("need to update database contracts table, before querying for nearby contract");

           
            while (!hasFound)   
            {
                if (expiries[contractInd - numNearby + 1] > currDate)   //we now assumes nearby indexoffset convention is 1 for all assets, since we mostly work on brent.
                    --contractInd;
                else
                {
                    hasFound = true;
                    ++contractInd;
                }
                if (contractInd < 0)
                    throw new Exception("Need more historical contracts");
            }

            string query2 = @"select * from contracts c, contract_details cd where c.symbol = '" + symbol + "' and c.expiry = '" + expiries[contractInd].ToString("yyyy-MM-dd") + "' and c.id = cd.id";
            //string query2 = @"select * from contracts c, contract_details cd where c.symbol = 'COIL' and c.expiry = '2016-10-31' and c.id = cd.id";
            DataTable dt2 = runQuery(query2);

            Contract c = new Contract();
            c.ConId = Int32.Parse(dt2.Rows[0]["id"].ToString() );
            c.Symbol = dt2.Rows[0]["symbol"].ToString();
            c.LocalSymbol = dt2.Rows[0]["LocalSymbol"].ToString();
            c.SecType = dt2.Rows[0]["sec_type"].ToString();
            c.Currency = dt2.Rows[0]["currency"].ToString();
            c.Exchange = dt2.Rows[0]["exchange"].ToString();
            
            return c;
        }

        //saves nearbyContract historical data, by stating id, startDate and endDate (both inclusive) to a file
        public DataTable getNearbyContractHistoricalData(int barID, DateTime startDate, DateTime endDate, String field = "all", bool includeEndTime = false)
        {
            if (barID >= 0 || endDate <= startDate)
                return null;
            var barInfo = getBarDataInfo(barID);
            if (barInfo == null)
                return null;
            int conID = barInfo.Item1;
            DataTable dt = getNearbyContractSchedule(conID, startDate, endDate);
            DataTable results = null;
            foreach (DataRow row in dt.Rows)
            {
                int subConID = Int32.Parse(row["contract_id"].ToString());
                int subBarID = getBarDataId(subConID, barInfo.Item2, barInfo.Item3, barInfo.Item4);
                DateTime subStartDate = DateTime.Parse(row["startDate"].ToString());
                if (startDate > subStartDate)
                    subStartDate = startDate;
                DateTime subEndDate = DateTime.Parse(row["endDate"].ToString()) + new TimeSpan(23,59,59);
                DateTime scheduleEndDate = subEndDate;
                if (endDate < subEndDate)
                    subEndDate = endDate;
                DataTable ret = getHistoricalData(subBarID, subStartDate, subEndDate, field, endDate <= scheduleEndDate ? includeEndTime : true);
                if (results == null)
                    results = ret;
                else
                    results.Merge(ret);
            }
            return results;
        }
        public Dictionary<int, int> getCombos(int combo_con_id)
        {
            string query = @"select leg_id, weight from combos where id = " + combo_con_id.ToString();
            DataTable dt = runQuery(query);
            Dictionary<int, int> ret = new Dictionary<int, int>();
            foreach( DataRow row in dt.Rows)
                ret.Add(Int32.Parse(row[0].ToString()), Int32.Parse(row[1].ToString()));
            return ret;
        }
        //public void addPosition(DateTime timestamp, string account, int strategy_id, int contract_id, int position, double avg_cost)
        //{
        //    string nonquery = "insert into positions values (" + contract_id.ToString() + "," + getSqlDateTime(timestamp) + "," + position.ToString() + "," + avg_cost.ToString() + "," + strategy_id.ToString() + "," + getSqlString(account) + ")";
        //    runNonQuery(nonquery);
        //}
        //public Tuple<int, double> getPosition(string account, int strategy_id, int contract_id)
        //{
        //    string query = @"select top(1) quantity, avg_cost from positions where account=" + getSqlString(account) + " and strategy_id=" + strategy_id.ToString() + " and contract_id=" + contract_id.ToString() + " order by timestamp desc";
        //    DataTable dt = runQuery(query);
        //    if (dt.Rows.Count == 0)
        //        return new Tuple<int, double>(0,0);
        //    return new Tuple<int, double>(int.Parse(dt.Rows[0][0].ToString()), double.Parse(dt.Rows[0][1].ToString()));
        //}
        //public void addStrategyPosition(DateTime timestamp, string account, int strategy_id, int contract_id, int position_id, int position, double avg_cost, string type, string description)
        //{
        //    string nonquery = "insert into strategy_positions values (" + position_id.ToString() + "," + contract_id.ToString() + "," + strategy_id.ToString() + "," + getSqlDateTime(timestamp) + "," + position.ToString() + "," + avg_cost.ToString() + ",'" + type + "'," + getSqlString(description) + "," + getSqlString(account) + ")";
        //    runNonQuery(nonquery);
        //}
        //public List<Tuple<int, int, double, string>> getOpenStrategyPositions(string account, int strategy_id, int contract_id, DateTime timestamp)
        //{
        //    string query1 = "select position_id, description, (sum(avg_cost * quantity) / sum(quantity)) as avg_cost from strategy_positions where account=" + getSqlString(account) + " and strategy_id=" + strategy_id.ToString() + " and contract_id=" + contract_id.ToString() + " and timestamp<=" + getSqlDateTime(timestamp) + " and position_type='E' group by position_id, description";
        //    string query2 = "select position_id, sum(quantity) as quantity from strategy_positions where account=" + getSqlString(account) + " and strategy_id=" + strategy_id.ToString() + " and contract_id=" + contract_id.ToString() + " and timestamp<=" + getSqlDateTime(timestamp) + " group by position_id";
        //    string query = "select t1.position_id as position_id, t2.quantity as quantity, t1.avg_cost as avg_cost, t1.description as description from (" + query1 + ") as t1, (" + query2 + ") as t2 where t1.position_id = t2.position_id and t2.quantity <> 0";
        //    DataTable dt = runQuery(query);
        //    List<Tuple<int, int, double, string>> ret = new List<Tuple<int, int, double, string>>();
        //    foreach (DataRow row in dt.Rows)
        //        ret.Add(new Tuple<int, int, double, string>(int.Parse(row[0].ToString()), int.Parse(row[1].ToString()), int.Parse(row[2].ToString()), row[3].ToString()));
        //    return ret;
        //}
        public void addExecution(int contract_id, Execution exec)
        {
            int qty = (int)(exec.Shares);
            if (exec.Side == "SLD")
                qty = -qty;
            DateTime time = DateTime.ParseExact(exec.Time, "yyyyMMdd  HH:mm:ss", CultureInfo.InvariantCulture);
            string nonquery = "insert into executions values ('" + exec.ExecId + "'," + exec.OrderId.ToString() + "," + contract_id.ToString() + "," + getSqlDateTime(time) + "," + qty.ToString() + "," + exec.Price.ToString() + "," + getSqlString(exec.Exchange) + "," + getSqlString(exec.AcctNumber) + ")";
            runNonQuery(nonquery);
        }
        public List<Tuple<string, int>> getOpenStrategyPositions(string account, int strategy_id, int contract_id, DateTime timestamp)
        {
            string query = @"select o.comment as comment, sum(e.filled_quantity) as quantity from executions e inner join orders o on e.order_id=o.id and e.contract_id=o.contract_id";
            query += @" where e.account=" + getSqlString(account) + " and e.contract_id=" + contract_id.ToString() + " and o.strategy_id=" + strategy_id.ToString() + " and e.timestamp<=" + getSqlDateTime(timestamp) + " group by o.comment";
            DataTable dt = runQuery(query);
            List<Tuple<string, int>> ret = new List<Tuple<string, int>>();
            foreach (DataRow row in dt.Rows)
                ret.Add(new Tuple<string, int>(row[0].ToString(), int.Parse(row[1].ToString())));
            return ret;
        }
        public int getContractIdFromOrderId(int order_id)
        {
            string query = @"select contract_id from orders where id = " + order_id.ToString();
            DataTable dt = runQuery(query);
            if (dt.Rows.Count == 0)
                return 0;
            return Int32.Parse(dt.Rows[0][0].ToString());
        }
        public int getMaxOrderId()
        {
            string query = @"select max(id) from orders";
            DataTable dt = runQuery(query);
            return dt.Rows.Count == 0 || dt.Rows[0][0].ToString() == "" ? 0 : Int32.Parse(dt.Rows[0][0].ToString());
        }
        public Tuple<int, int, Order> getOrder(int orderId)
        {
            string query = @"select * from orders where id=" + orderId.ToString();
            DataTable dt = runQuery(query);
            if (dt.Rows.Count == 0)
                return null;
            DataRow row = dt.Rows[0];
            int conId = int.Parse(row["contract_id"].ToString());
            int stgId = int.Parse(row["strategy_id"].ToString());
            Order o = new Order();
            o.OrderId = int.Parse(row["id"].ToString());
            int qty = int.Parse(row["quantity"].ToString());
            o.TotalQuantity = Math.Abs(qty);
            o.Action = qty > 0 ? "BUY" : "SELL";
            o.OrderType = row["order_type"].ToString();
            if(o.OrderType == "LMT")
                o.LmtPrice = double.Parse(row["limit_price"].ToString());
            else if (o.OrderType == "STP")
                o.AuxPrice = double.Parse(row["stop_price"].ToString());
            string ocaType = row["oca_type"].ToString();
            if (ocaType != "")
            {
                o.OcaGroup = row["oca_group"].ToString();
                o.OcaType = int.Parse(ocaType);
            }
            o.OrderRef = row["comment"].ToString();
            o.Account = row["account"].ToString();
            o.ParentId = int.Parse(row["parent_id"].ToString());
            o.Tif = row["time_in_force"].ToString();
            o.PermId = int.Parse(row["broker_id"].ToString());
            return new Tuple<int, int, Order>(stgId, conId, o);
        }
        public DateTime getNextEventTime(string eventName, DateTime t)
        {
            string query = @"select top(1) time from events where time>=" + getSqlDateTime(t) + " order by time";
            DataTable dt = runQuery(query);
            if (dt.Rows.Count == 0)
                return default(DateTime);
            return DateTime.Parse(dt.Rows[0][0].ToString());
        }
        public void exportBarsToFile(int bar_data_id, DateTime startTime, DateTime endTime, string filename)
        {
            DataTable data = getHistoricalData(bar_data_id, startTime, endTime);
            List<string> lines = new List<string>();
            lines.Add("time,open,high,low,close,volume,wap");
            foreach (DataRow row in data.Rows)
                lines.Add(((DateTime)(row["time"])).ToString("yyyy-MM-dd HH:mm:ss") + "," + row["open"].ToString() + "," + row["high"].ToString() + "," + row["low"].ToString() + "," + row["close"].ToString() + "," + row["volume"].ToString() + "," + row["wap"].ToString());

            if (filename.EndsWith(".gz"))
                filename = filename.Substring(0, filename.Length - 3) + ".csv.gz";
            else if (filename.EndsWith(".csv"))
                filename += ".gz";
            else
                filename += ".csv.gz";
            string tmpFile = Path.GetTempFileName();
            File.WriteAllLines(tmpFile, lines.ToArray());
            byte[] b;
            using (FileStream f = new FileStream(tmpFile, FileMode.Open))
            {
                b = new byte[f.Length];
                f.Read(b, 0, (int)f.Length);
            }
            using (FileStream f2 = new FileStream(filename, FileMode.Create))
            using (GZipStream gz = new GZipStream(f2, CompressionMode.Compress, false))
            {
                gz.Write(b, 0, b.Length);
            }
            File.Delete(tmpFile);
        }
        public void convertAndExportMinBarsToFile(int bar_data_id, DateTime startTime, DateTime endTime, string filename, int minuteStep, int startingMinute)
        {
            DataTable data = getHistoricalData(bar_data_id, startTime, endTime);

            string[] colNames = new string[data.Columns.Count];

            for (int i = 0; i < data.Columns.Count; i++)
            {
                colNames[i] = data.Columns[i].ColumnName.ToString();
            }

            List<string> lines = new List<string>();
            lines.Add("time,open,high,low,close,volume,wap");
            DataTableReader reader = data.CreateDataReader();
            reader.Read();

            DateTime startT;
            startT = (DateTime)(reader[0]);

            DateTime currentT = startT;

            int currentM = currentT.Minute;
            int myStartingMinute = startingMinute;

            while (currentM > (myStartingMinute + minuteStep))
            {
                myStartingMinute = (myStartingMinute + minuteStep) % 60;
            }

            startT = new DateTime(currentT.Year, currentT.Month, currentT.Day, currentT.Hour, myStartingMinute, 0);
            DateTime endT = startT.AddMinutes(minuteStep);


            bool isAggregating = true;
            bool hasResults = true;

            double open, high, low, close, wap;
            int vol;

            open = (double)reader[1];
            high = (double)reader[2];
            low = (double)reader[3];
            close = (double)reader[4];
            vol = Convert.ToInt32(reader[5]);
            wap = (double)reader[6];

            double priceTotal = vol * wap;
            int currentVol = 0;
            double currentWap = 0;

            while (reader.Read())
            {
                currentT = (DateTime)reader[0];

                if (currentT >= endT)
                {
                    if (hasResults)
                    {
                        wap = priceTotal / vol;
                        lines.Add(startT.ToString("yyyy-MM-dd HH:mm:ss") + "," + open.ToString() + "," + high.ToString() + "," + low.ToString() + "," + close.ToString() + "," + vol.ToString() + "," + wap.ToString());
                        isAggregating = false;
                        hasResults = false;
                    }

                    startT = endT;
                    endT = startT.AddMinutes(minuteStep);
                    while (currentT >= endT)
                    {
                        startT = endT;
                        endT = startT.AddMinutes(minuteStep);
                    }
                }

                if (isAggregating)
                {
                    if (!hasResults)
                        throw new Exception("Something went wrong when aggregating");

                    high = Math.Max(high, (double)(reader[2]));
                    low = Math.Min(low, (double)(reader[3]));
                    close = (double)(reader[4]);
                    currentVol = Convert.ToInt32(reader[5]);
                    vol += currentVol;
                    currentWap = (double)(reader[6]);
                    priceTotal += currentWap * currentVol;
                }
                else
                {
                    open = (double)reader[1];
                    high = (double)reader[2];
                    low = (double)reader[3];
                    close = (double)reader[4];
                    vol = Convert.ToInt32(reader[5]);
                    wap = (double)reader[6];

                    priceTotal = wap * vol;

                    isAggregating = true;
                    hasResults = true;
                }
            }
            if (hasResults)
                lines.Add(startT.ToString("yyyy-MM-dd HH:mm:ss") + "," + open.ToString() + "," + high.ToString() + "," + low.ToString() + "," + close.ToString() + "," + vol.ToString() + "," + wap.ToString());


            if (filename.EndsWith(".gz"))
                filename = filename.Substring(0, filename.Length - 3) + ".csv.gz";
            else if (filename.EndsWith(".csv"))
                filename += ".gz";
            else
                filename += ".csv.gz";
            string tmpFile = Path.GetTempFileName();
            File.WriteAllLines(tmpFile, lines.ToArray());
            byte[] b;
            using (FileStream f = new FileStream(tmpFile, FileMode.Open))
            {
                b = new byte[f.Length];
                f.Read(b, 0, (int)f.Length);
            }
            using (FileStream f2 = new FileStream(filename, FileMode.Create))
            using (GZipStream gz = new GZipStream(f2, CompressionMode.Compress, false))
            {
                gz.Write(b, 0, b.Length);
            }
            File.Delete(tmpFile);
        }
        public void importBarsFromFile(int bar_data_id, string filename, bool overwrite = false)
        {
            string tmpFile = Path.GetTempFileName();
            using (FileStream gzStream = File.OpenRead(filename))
            {
                using (FileStream tmpStream = File.Create(tmpFile))
                {
                    using (GZipStream decompressStream = new GZipStream(gzStream, CompressionMode.Decompress))
                    {
                        decompressStream.CopyTo(tmpStream);
                    }
                }
            }
            string[] lines = File.ReadAllLines(tmpFile);
            foreach(string line in lines)
            {
                if (line.StartsWith("time"))
                    continue;
                string[] infos = line.Split(',');
                DateTime time = DateTime.ParseExact(infos[0], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                double open = double.Parse(infos[1]);
                double high = double.Parse(infos[2]);
                double low = double.Parse(infos[3]);
                double close = double.Parse(infos[4]);
                double volume = double.Parse(infos[5]);
                double wap = double.Parse(infos[6]);
                setBar(bar_data_id, time, open, high, low, close, volume, wap, overwrite);
            }
            File.Delete(tmpFile);
        }
        public static Utils.Calendar mapCalendar(string calendar_name)
        {
            switch (calendar_name)
            {
                case "ICE": return new UnitedKingdom(UnitedKingdom.Market.ICE);
                case "SGX": return new Singapore(Singapore.Market.SGX);
                case "ICE_SGX": return new JointCalendar(new UnitedKingdom(UnitedKingdom.Market.ICE), new Singapore(Singapore.Market.SGX));
                case "NYSE": return new UnitedStates(UnitedStates.Market.NYSE);
                case "CME": return new UnitedStates(UnitedStates.Market.CME);
                default: return new WeekendsOnly();
            }
        }
        //Singleton interface
        private static DBAccess _instance;
        public static DBAccess instance
        {
            get
            {
                if (_instance == null)
                    _instance = new DBAccess();
                return _instance;
            }
        }
    }
}
