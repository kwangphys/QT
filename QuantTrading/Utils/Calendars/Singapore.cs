using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Utils
{
    //! Singapore calendars
    /*! Holidays for the Singapore exchange
        (data from
         <http://www.sgx.com/wps/portal/sgxweb/home/trading/securities/trading_hours_calendar>):
        <ul>
        <li>Saturdays</li>
        <li>Sundays</li>
        <li>New Year's day, January 1st</li>
        <li>Good Friday</li>
        <li>Labour Day, May 1st</li>
        <li>National Day, August 9th</li>
        <li>Christmas, December 25th </li>
        </ul>

        Other holidays for which no rule is given
        (data available for 2004-2010, 2012-2014 only:)
        <ul>
        <li>Chinese New Year</li>
        <li>Hari Raya Haji</li>
        <li>Vesak Poya Day</li>
        <li>Deepavali</li>
        <li>Diwali</li>
        <li>Hari Raya Puasa</li>
        </ul>

        \ingroup calendars
    */

    public class Singapore : Calendar
    {
        public enum Market { SGX }

        public Singapore() : this(Market.SGX) { }
        public Singapore(Market m) : base()
        {
            calendar_ = SGX.Singleton;
        }

        private class SGX : Calendar.WesternImpl
        {
            public static readonly SGX Singleton = new SGX();
            private SGX() { }

            public override string name() { return "Singapore exchange"; }
            public override bool isBusinessDay(DateTime date)
            {
                DayOfWeek w = date.DayOfWeek;
                int d = date.Day, dd = date.DayOfYear;
                Month m = (Month)date.Month;
                int y = date.Year;
                int em = easterMonday(y);

                if (isWeekend(w)
                    // New Year's Day
                    || ((d == 1 || (d == 2 && w == DayOfWeek.Monday)) && m == Month.January)
                    // Good Friday
                    || (dd == em - 3)
                    // Labor Day
                    || ((d == 1 || (d == 2 && w == DayOfWeek.Monday)) && m == Month.May)
                    // National Day
                    || ((d == 9 || (d == 10 && w == DayOfWeek.Monday)) && m == Month.August)
                    // Christmas Day
                    || ((d == 25 || (d == 26 && w == DayOfWeek.Monday)) && m == Month.December)

                    // Chinese New Year
                    || ((d == 22 || d == 23) && m == Month.January && y == 2004)
                    || ((d == 9 || d == 10) && m == Month.February && y == 2005)
                    || ((d == 30 || d == 31) && m == Month.January && y == 2006)
                    || ((d == 19 || d == 20) && m == Month.February && y == 2007)
                    || ((d == 7 || d == 8) && m == Month.February && y == 2008)
                    || ((d == 26 || d == 27) && m == Month.January && y == 2009)
                    || ((d == 15 || d == 16) && m == Month.January && y == 2010)
                    || ((d == 23 || d == 24) && m == Month.January && y == 2012)
                    || ((d == 11 || d == 12) && m == Month.February && y == 2013)
                    || (d == 31 && m == Month.January && y == 2014)
                    || (d == 1 && m == Month.February && y == 2014)
                    || ((d == 19 || d == 20) && m == Month.February && y == 2015)
                    || ((d == 8 || d == 9) && m == Month.February && y == 2016)
                    || ((d == 28 || d == 29 || d == 30) && m == Month.January && y == 2017)

                    // Vesak Day
                    || (d == 2 && m == Month.June && y == 2004)
                    || (d == 22 && m == Month.May && y == 2005)
                    || (d == 12 && m == Month.May && y == 2006)
                    || (d == 31 && m == Month.May && y == 2007)
                    || (d == 18 && m == Month.May && y == 2008)
                    || (d == 9 && m == Month.May && y == 2009)
                    || (d == 28 && m == Month.May && y == 2010)
                    || (d == 5 && m == Month.May && y == 2012)
                    || (d == 24 && m == Month.May && y == 2013)
                    || (d == 13 && m == Month.May && y == 2014)
                    || (d == 1 && m == Month.June && y == 2015)
                    || (d == 21 && m == Month.May && y == 2016)
                    || (d == 10 && m == Month.May && y == 2017)

                    // Hari Raya Puasa
                    || ((d == 14 || d == 15) && m == Month.November && y == 2004)
                    || (d == 3 && m == Month.November && y == 2005)
                    || (d == 24 && m == Month.October && y == 2006)
                    || (d == 13 && m == Month.October && y == 2007)
                    || (d == 1 && m == Month.October && y == 2008)
                    || (d == 21 && m == Month.September && y == 2009)
                    || (d == 10 && m == Month.September && y == 2010)
                    || (d == 20 && m == Month.August && y == 2012)
                    || (d == 8 && m == Month.August && y == 2013)
                    || (d == 28 && m == Month.July && y == 2014)
                    || (d == 17 && m == Month.July && y == 2015)
                    || (d == 6 && m == Month.July && y == 2016)
                    || ((d == 25 || d == 26) && m == Month.June && y == 2017)

                    // Hari Raya Haji
                    || ((d == 1 || d == 2) && m == Month.February && y == 2004)
                    || (d == 21 && m == Month.January && y == 2005)
                    || (d == 10 && m == Month.January && y == 2006)
                    || (d == 2 && m == Month.January && y == 2007)
                    || (d == 20 && m == Month.December && y == 2007)
                    || (d == 8 && m == Month.December && y == 2008)
                    || (d == 27 && m == Month.November && y == 2009)
                    || (d == 17 && m == Month.November && y == 2010)
                    || (d == 26 && m == Month.October && y == 2012)
                    || (d == 15 && m == Month.October && y == 2013)
                    || (d == 6 && m == Month.October && y == 2014)
                    || (d == 24 && m == Month.September && y == 2015)
                    || (d == 12 && m == Month.September && y == 2016)
                    || (d == 1 && m == Month.September && y == 2017)

                    // Deepavali
                    || (d == 11 && m == Month.November && y == 2004)
                    || (d == 1 && m == Month.November && y == 2005)
                    || (d == 8 && m == Month.November && y == 2007)
                    || (d == 28 && m == Month.October && y == 2008)
                    || (d == 16 && m == Month.November && y == 2009)
                    || (d == 5 && m == Month.November && y == 2010)
                    || (d == 13 && m == Month.November && y == 2012)
                    || (d == 4 && m == Month.November && y == 2013)
                    || (d == 23 && m == Month.October && y == 2014)
                    || (d == 10 && m == Month.November && y == 2015)
                    || (d == 29 && m == Month.October && y == 2016)
                    || (d == 18 && m == Month.October && y == 2017)

                    // other events
                    // SG50
                    || (d == 7 && m == Month.August && y == 2015)
                    // Polling Day
                    || (d == 11 && m == Month.September && y == 2015)
                    )
                    return false;
                return true;
            }
        }
    }
}
