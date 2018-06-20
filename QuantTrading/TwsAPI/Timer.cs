using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;

namespace IBApi
{
    public delegate void onTime();

    public class Timer
    {
        private readonly Object _lock = new Object();
        SortedDictionary<int, List<onTime>> _runners;
        public Timer()
        {
            _runners = new SortedDictionary<int, List<onTime>>();
            Thread t = new Thread(new ThreadStart(runAllJobs));
            t.CurrentCulture = CultureInfo.InvariantCulture;
            t.CurrentUICulture = CultureInfo.InvariantCulture;
            t.Start();
        }
        public void subscribe(TimeSpan t, onTime func)
        {
            int nt = (int)(t.TotalMinutes);
            lock (_lock)
            {
                if (!_runners.ContainsKey(nt))
                    _runners.Add(nt, new List<onTime>());
                _runners[nt].Add(func);
            }
        }
        void runAllJobs()
        {
            DateTime dt = DateTime.Now;
            int nt = (int)(dt.TimeOfDay.TotalMinutes) + 1;
            DateTime currd = dt.Date;
            while (true)
            {
                if (nt >= 1440)
                {
                    nt = 0;
                    currd = currd.AddDays(1);
                }
                DateTime nextTime = currd.AddMinutes(nt);
                int nsleep = (int)((nextTime - DateTime.Now).TotalMilliseconds);
                if (nsleep > 0)
                    Thread.Sleep(nsleep);
                if (_runners.ContainsKey(nt))
                {
                    foreach (onTime runner in _runners[nt])
                    {
                        Thread t = new Thread(new ThreadStart(runner));
                        t.CurrentCulture = CultureInfo.InvariantCulture;
                        t.CurrentUICulture = CultureInfo.InvariantCulture;
                        t.Start();
                    }
                }
                nt++;
            }
        }
        //Singleton interface
        private static Timer _instance;
        public static Timer instance
        {
            get
            {
                if (_instance == null)
                    _instance = new Timer();
                return _instance;
            }
        }

    }
}
