using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
namespace IBStrategy
{
    public class ThreadManager
    {
        private readonly Object _lock = new Object();
        int _currEventId;
        Dictionary<int, ManualResetEvent> _events;

        public Dictionary<int, ManualResetEvent> Events
        {
            get { return _events; }
        }
        public ThreadManager()
        {
            _currEventId = 0;
            _events = new Dictionary<int, ManualResetEvent>();
        }
        public ManualResetEvent createEvent()
        {
            ManualResetEvent e = new ManualResetEvent(true);
            lock (_lock)
                _events.Add(_currEventId++, e);
            return e;
        }
        //Singleton interface
        private static ThreadManager _instance;
        public static ThreadManager instance
        {
            get
            {
                if (_instance == null)
                    _instance = new ThreadManager();
                return _instance;
            }
        }
    }

}
