using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace DBAccess
{
    public class Report
    {
        DBAccess _db = DBAccess.instance;
        private string _account;
        private DateTime _startDate;
        private DateTime _endDate;
        private DataTable _executions;
        public Report(string account)
        {
            _account = account;
            string query = getQueryWithAccount();
            query += @" order by e.timestamp, o.id";
            DataTable dt = _db.runQuery(query);
            _startDate = DateTime.MinValue;
            _endDate = DateTime.MaxValue;
            processRawData(dt);
        }
        public Report(string account, DateTime startDate)
        {
            _account = account;
            _startDate = startDate;
            string query = getQueryWithAccount();
            query += @" and e.timestamp >= " + _db.getSqlDateTime(startDate);
            query += @" order by e.timestamp, o.id";
            DataTable dt = _db.runQuery(query);
            _endDate = DateTime.MaxValue;
            processRawData(dt);
        }
        public Report(string account, DateTime startDate, DateTime endDate)
        {
            _account = account;
            _startDate = startDate;
            _endDate = endDate;
            string query = getQueryWithAccount();
            query += @" and e.timestamp >= " + _db.getSqlDateTime(startDate);
            query += @" and e.timestamp < " + _db.getSqlDateTime(endDate);
            query += @" order by e.timestamp, o.id";
            DataTable dt = _db.runQuery(query);
            processRawData(dt);
        }
        public string reportTradeDetails()
        {
            StringBuilder sb = new StringBuilder();
            IEnumerable<string> columnNames = _executions.Columns.Cast<DataColumn>().
                                              Select(column => column.ColumnName);
            sb.AppendLine(string.Join(",", columnNames));
            foreach (DataRow row in _executions.Rows)
            {
                IEnumerable<string> fields = row.ItemArray.Select(field => field.ToString());
                sb.AppendLine(string.Join(",", fields));
            }
            return sb.ToString();
        }
        public string reportSummary()
        {
            int nbwin = 0;
            int nblose = 0;
            int nswin = 0;
            int nslose = 0;
            double Srb = 0;
            double Srs = 0;
            double Sr = 0;
            double Sr2 = 0;
            double maxDown = 0;
            double dailyR = 0;
            int ndays = 0;
            for (DateTime dt = _startDate; dt < _endDate; dt = dt.AddDays(1))
                if (dt.DayOfWeek != DayOfWeek.Saturday && dt.DayOfWeek != DayOfWeek.Sunday)
                    ndays++;
            //(int)((_endDate - _startDate).TotalDays);
            DateTime currd = _startDate;
            double down = 0;
            double prevpeak = 0;
            foreach (DataRow row in _executions.Rows)
            {
                DateTime dt = DateTime.Parse(row["EntryTime"].ToString());
                if (dt.Date != currd)
                {
                    Sr += dailyR;
                    Sr2 += dailyR * dailyR;
                    dailyR = 0;
                    currd = dt.Date;
                    if (Sr > prevpeak)
                    {
                        if (down > maxDown)
                            maxDown = down;
                        down = 0;
                        prevpeak = Sr;
                    }
                    else
                    {
                        if (prevpeak - Sr > down)
                            down = prevpeak - Sr;
                    }
                }
                double pnl = double.Parse(row["PnL"].ToString());
                double qty = double.Parse(row["FilledQty"].ToString());
                double price = double.Parse(row["EntryPrice"].ToString());
                int lotSize = int.Parse(row["LotSize"].ToString());
                double r = pnl / Math.Abs(qty * lotSize) / price;
                dailyR += r;
                if (qty > 0)
                {
                    if (pnl > 0)
                        nbwin++;
                    else
                        nblose++;
                    Srb += r;
                }
                else
                {
                    if (pnl > 0)
                        nswin++;
                    else
                        nslose++;
                    Srs += r;
                }
            }
            if (dailyR != 0)
            {
                Sr += dailyR;
                Sr2 += dailyR * dailyR;
                if (Sr > prevpeak)
                {
                    if (down > maxDown)
                        maxDown = down;
                    down = 0;
                    prevpeak = Sr;
                }
                else
                {
                    if (prevpeak - Sr > down)
                        down = prevpeak - Sr;
                }
            }
            string summary = "";
            int ntrades = nbwin + nblose + nswin + nslose;
            double vol = Math.Sqrt((Sr2 - Sr * Sr / (double)(ndays)) / (double)(ndays - 1));
            //Total PnL
            summary += "Total Return:\t" + Sr.ToString("0.##%") + "\r\n";
            //N Trading Days
            summary += "N Trading Days:\t" + ndays.ToString() + " days\r\n";
            //Annualized Return
            summary += "Annual Return:\t" + (Sr / (double)ndays * 252).ToString("0.##%") + "\r\n";
            //Daily Volatility
            summary += "Daily Volatility:\t" + vol.ToString("0.####%") + "\r\n";
            //Sharpe Ratio
            summary += "Sharpe Ratio:\t" + (Sr / (double)ndays / vol * Math.Sqrt(252.0)).ToString("0.####") + "\r\n";
            //WinningRatio
            summary += "Winning Ratio:\t" + ((double)(nbwin + nswin) / (double)ntrades).ToString("0.##%") + "\r\n";
            //PerTradeReturn
            summary += "Per Trade Return:\t" + (Sr / (double)ntrades).ToString("0.####%") + "\r\n";
            //NTradesPerDay
            summary += "N Trades Per Day:\t" + ((double)ntrades / (double)ndays).ToString() + "\r\n";
            //MaxDrawdown
            summary += "Max Drawdown:\t" + maxDown.ToString("0.##%") + "\r\n";
            //LongRatio
            summary += "Long Trades Ratio:\t" + ((double)(nbwin + nblose) / (double)ntrades).ToString("0.##%") + "\r\n";
            //LongWinningRatio
            summary += "Long Trades Winning Ratio:\t" + ((double)nbwin / (double)(nbwin + nblose)).ToString("0.##%") + "\r\n";
            //LongAnnualizedReturn
            summary += "Long Trades Annual Return:\t" + (Srb / (double)ndays * 252).ToString("0.##%") + "\r\n";
            //ShortRatio
            summary += "Short Trades Ratio:\t" + ((double)(nswin + nslose) / (double)ntrades).ToString("0.##%") + "\r\n";
            //ShortWinningRatio
            summary += "Short Trades Winning Ratio:\t" + ((double)nswin / (double)(nswin + nslose)).ToString("0.##%") + "\r\n";
            //ShortAnnualizedReturn
            summary += "Short Trades Annual Return:\t" + (Srs / (double)ndays * 252).ToString("0.##%") + "\r\n";
            return summary;
        }
        private string getQueryWithAccount()
        {
            return @"select e.id as execution_id, o.id as order_id, o.contract_id as contract_id, o.strategy_id as strategy_id, e.timestamp as timestamp, e.filled_quantity as filled_qty, 
e.filled_price as filled_price, o.order_type as order_type, o.limit_price as limit_price, o.stop_price as stop_price, o.parent_id as parent_id, c.lot_size as lot_size
from executions e inner join orders o on e.order_id = o.id inner join contracts c on o.contract_id = c.id where o.account = " + _db.getSqlString(_account);
        }
        private void processRawData(DataTable dt)
        {
            Dictionary<int, List<DataRow>> es = new Dictionary<int, List<DataRow>>();
            _executions = new DataTable();
            _executions.Columns.Add("Account", typeof(string));
            _executions.Columns.Add("StrategyID", typeof(int));
            _executions.Columns.Add("EntryTime", typeof(DateTime));
            _executions.Columns.Add("EntryPrice", typeof(double));
            _executions.Columns.Add("ExitTime", typeof(DateTime));
            _executions.Columns.Add("ExitPrice", typeof(double));
            _executions.Columns.Add("FilledQty", typeof(double));
            _executions.Columns.Add("LotSize", typeof(int));
            _executions.Columns.Add("PnL", typeof(double));
            _executions.Columns.Add("TotalPnL", typeof(double));

            _executions.Columns.Add("EntryOrderID", typeof(int));
            _executions.Columns.Add("EntryExecutionID", typeof(string));
            _executions.Columns.Add("EntryOrderType", typeof(string));
            _executions.Columns.Add("EntryLimitPrice", typeof(double));
            _executions.Columns.Add("EntryStopPrice", typeof(double));
            _executions.Columns.Add("ExitOrderID", typeof(int));
            _executions.Columns.Add("ExitExecutionID", typeof(string));
            _executions.Columns.Add("ExitOrderType", typeof(string));
            _executions.Columns.Add("ExitLimitPrice", typeof(double));
            _executions.Columns.Add("ExitStopPrice", typeof(double));
            double totalPnL = 0;
            foreach (DataRow row in dt.Rows)
            {
                if (row["parent_id"].ToString() == "0")
                {
                    int order_id = int.Parse(row["order_id"].ToString());
                    if (!es.ContainsKey(order_id))
                        es.Add(order_id, new List<DataRow>());
                    es[order_id].Add(row);
                }
                else
                {
                    int qty = int.Parse(row["filled_qty"].ToString());
                    int parent_id = int.Parse(row["parent_id"].ToString());
                    int lot_size = int.Parse(row["lot_size"].ToString());
                    List<DataRow> parents = es[parent_id];
                    int nparents = parents.Count;
                    for (int i = 0; i < nparents; ++i)
                    {
                        DataRow parent_row = parents[0];
                        int parent_qty = int.Parse(parent_row["filled_qty"].ToString());
                        int exe_qty = Math.Min(Math.Abs(qty), Math.Abs(parent_qty)) * Math.Sign(parent_qty);
                        double pnl = lot_size * exe_qty * (double.Parse(row["filled_price"].ToString()) - double.Parse(parent_row["filled_price"].ToString()));
                        totalPnL += pnl;
                        _executions.Rows.Add(
                            _account,
                            row["strategy_id"],
                            parent_row["timestamp"],
                            parent_row["filled_price"],
                            row["timestamp"],
                            row["filled_price"],
                            exe_qty,
                            row["lot_size"],
                            pnl,
                            totalPnL,
                            parent_row["order_id"],
                            parent_row["execution_id"],
                            parent_row["order_type"],
                            parent_row["limit_price"],
                            parent_row["stop_price"],
                            row["order_id"],
                            row["execution_id"],
                            row["order_type"],
                            row["limit_price"],
                            row["stop_price"]
                        );
                        if (Math.Abs(parent_qty) > Math.Abs(exe_qty))
                            parent_row["filled_qty"] = parent_qty + exe_qty;
                        else
                            parents.RemoveAt(0);
                        if (Math.Abs(qty) <= Math.Abs(exe_qty))
                            break;
                    }
                }
            }
            if (_startDate == DateTime.MinValue)
                _startDate = DateTime.Parse(_executions.Rows[0]["EntryTime"].ToString()).Date;
            if (_endDate == DateTime.MaxValue)
                _endDate = DateTime.Parse(_executions.Rows[_executions.Rows.Count - 1]["ExitTime"].ToString()).Date.AddDays(1);
        }
    }
}
