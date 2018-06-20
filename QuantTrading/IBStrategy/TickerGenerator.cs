using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace IBStrategy
{
    public class TickerGenerator
    {
        private readonly Object _lock = new Object();
        private int _currTicker;
        public void reset()
        {
            lock(_lock)
            {
                _currTicker = 0;
            }
        }
        public int get()
        {
            lock(_lock)
            {
                ++_currTicker;
            }
            return _currTicker;
        }
        public TickerGenerator()
        {
            reset();
        }
        //Singleton interface
        private static TickerGenerator _instance;
        public static TickerGenerator instance
        {
            get
            {
                if (_instance == null)
                    _instance = new TickerGenerator();
                return _instance;
            }
        }
    }
}
