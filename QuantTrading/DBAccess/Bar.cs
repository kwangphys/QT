using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace DBAccess
{
    public struct Bar
    {
        public int id;
        public DateTime time;
        public double open;
        public double high;
        public double low;
        public double close;
        public double volume;
        public double wap;
        public Bar(int _id, DataRow row)
        {
            id = _id;
            time = DateTime.Parse(row["time"].ToString());
            open = Double.Parse(row["open"].ToString());
            high = Double.Parse(row["high"].ToString());
            low = Double.Parse(row["low"].ToString());
            close = Double.Parse(row["close"].ToString());
            volume = Double.Parse(row["volume"].ToString());
            wap = Double.Parse(row["wap"].ToString());
        }
        public Bar(int i, DateTime t, double o, double h, double l, double c, double v, double w)
        {
            id = i;
            time = t;
            open = o;
            high = h;
            low = l;
            close = c;
            volume = v;
            wap = w;
        }
    }
}
