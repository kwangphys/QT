using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Utils
{
    public class WeekendsOnly : Calendar
    {
        public WeekendsOnly() : base(Impl.Singleton) { }

        class Impl : Calendar.WesternImpl
        {
            public static readonly Impl Singleton = new Impl();
            private Impl() { }

            public override string name() { return "weekends only"; }
            public override bool isBusinessDay(DateTime date) { return !isWeekend(date.DayOfWeek); }

        }
    }
}
