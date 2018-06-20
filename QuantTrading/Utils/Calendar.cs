using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Utils
{
    public enum Month
    {
        January = 1,
        February = 2,
        March = 3,
        April = 4,
        May = 5,
        June = 6,
        July = 7,
        August = 8,
        September = 9,
        October = 10,
        November = 11,
        December = 12,
        Jan = 1,
        Feb = 2,
        Mar = 3,
        Apr = 4,
        Jun = 6,
        Jul = 7,
        Aug = 8,
        Sep = 9,
        Oct = 10,
        Nov = 11,
        Dec = 12
    };

    public enum BusinessDayConvention
    {
        // ISDA
        Following,          /*!< Choose the first business day after
                                 the given holiday. */
        ModifiedFollowing,  /*!< Choose the first business day after
                                 the given holiday unless it belongs
                                 to a different month, in which case
                                 choose the first business day before
                                 the holiday. */
        Preceding,          /*!< Choose the first business day before
                                 the given holiday. */
        // NON ISDA
        ModifiedPreceding,  /*!< Choose the first business day before
                                 the given holiday unless it belongs
                                 to a different month, in which case
                                 choose the first business day after
                                 the holiday. */
        Unadjusted,          /*!< Do not adjust. */
        HalfMonthModifiedFollowing,   /*!< Choose the first business day after
                                          the given holiday unless that day
                                          crosses the mid-month (15th) or the
                                          end of month, in which case choose
                                          the first business day before the
                                          holiday. */
        Nearest                      /*!< Choose the nearest business day 
                                          to the given holiday. If both the
                                          preceding and following business
                                          days are equally far away, default
                                          to following business day. */
    };

    //! Units used to describe time periods
    public enum TimeUnit
    {
        Days,
        Weeks,
        Months,
        Years
    };

    /// <summary>
    /// This class provides methods for determining whether a date is a
    /// business day or a holiday for a given market, and for
    /// incrementing/decrementing a date of a given number of business days.
    /// 
    /// A calendar should be defined for specific exchange holiday schedule
    /// or for general country holiday schedule. Legacy city holiday schedule
    /// calendars will be moved to the exchange/country convention.
    /// </summary>
    public class Calendar
    {
        protected Calendar calendar_;
        public List<DateTime> addedHolidays = new List<DateTime>();
        public List<DateTime> removedHolidays = new List<DateTime>();

        public Calendar calendar
        {
            get { return calendar_; }
            set { calendar_ = value; }
        }

        // constructors
        /*! The default constructor returns a calendar with a null 
            implementation, which is therefore unusable except as a
            placeholder. */
        public Calendar() { }
        public Calendar(Calendar c) { calendar_ = c; }

        //! \name Wrappers for interface
        //@{
        /// <summary>
        /// This method is used for output and comparison between
        /// calendars. It is <b>not</b> meant to be used for writing
        /// switch-on-type code.
        /// </summary>
        /// <returns>
        /// The name of the calendar.
        /// </returns>
        public virtual string name() { return calendar.name(); }
        /// <param name="d">Date</param>
        /// <returns>Returns <tt>true</tt> iff the date is a business day for the
        /// given market.</returns>
        public virtual bool isBusinessDay(DateTime d)
        {
            if (calendar.addedHolidays.Contains(d))
                return false;
            if (calendar.removedHolidays.Contains(d))
                return true;
            return calendar.isBusinessDay(d);
        }
        ///<summary>
        /// Returns <tt>true</tt> iff the weekday is part of the
        /// weekend for the given market.
        ///</summary>
        public virtual bool isWeekend(DayOfWeek w) { return calendar.isWeekend(w); }
        //@}

        // other functions
        /// <summary>
        /// Returns whether or not the calendar is initialized
        /// </summary>
        public bool empty() { return (object)calendar == null; }				//!  Returns whether or not the calendar is initialized
        /// <summary>
        /// Returns <tt>true</tt> iff the date is a holiday for the given
        /// market.
        /// </summary>
        public bool isHoliday(DateTime d) { return !isBusinessDay(d); }
        /// <summary>
        /// Returns <tt>true</tt> iff the date is last business day for the
        /// month in given market.
        /// </summary>
        public bool isEndOfMonth(DateTime d) { return (d.Month != adjust(d.AddDays(1)).Month); }
        /// <summary>
        /// last business day of the month to which the given date belongs
        /// </summary>
        public DateTime endOfMonth(DateTime d) { return adjust(d.AddDays(-d.Day + DateTime.DaysInMonth(d.Year, d.Month)), BusinessDayConvention.Preceding); }

        /// <summary>
        /// Adjusts a non-business day to the appropriate near business day  with respect 
        /// to the given convention.  
        /// </summary>
        public DateTime adjust(DateTime d, BusinessDayConvention c = BusinessDayConvention.Following)
        {
            if (d == null) throw new ArgumentException("null date");
            if (c == BusinessDayConvention.Unadjusted) return d;

            DateTime d1 = d;
            if (c == BusinessDayConvention.Following || c == BusinessDayConvention.ModifiedFollowing ||
                c == BusinessDayConvention.HalfMonthModifiedFollowing)
            {
                while (isHoliday(d1)) d1 = d1.AddDays(1);
                if (c == BusinessDayConvention.ModifiedFollowing || c == BusinessDayConvention.HalfMonthModifiedFollowing)
                {
                    if (d1.Month != d.Month)
                        return adjust(d, BusinessDayConvention.Preceding);
                    if (c == BusinessDayConvention.HalfMonthModifiedFollowing)
                    {
                        if (d.Day <= 15 && d1.Day > 15)
                        {
                            return adjust(d, BusinessDayConvention.Preceding);
                        }
                    }
                }
            }
            else if (c == BusinessDayConvention.Preceding || c == BusinessDayConvention.ModifiedPreceding)
            {
                while (isHoliday(d1))
                    d1 = d1.AddDays(-1);
                if (c == BusinessDayConvention.ModifiedPreceding && d1.Month != d.Month)
                    return adjust(d, BusinessDayConvention.Following);
            }
            else if (c == BusinessDayConvention.Nearest)
            {
                DateTime d2 = d;
                while (isHoliday(d1) && isHoliday(d2))
                {
                    d1 = d1.AddDays(1);
                    d2 = d2.AddDays(-1);
                }
                if (isHoliday(d1))
                    return d2;
                else
                    return d1;
            }
            else throw new ArgumentException("Unknown business-day convention: " + c);
            return d1;
        }

        /// <summary>
        /// Advances the given date of the given number of business days and
        /// returns the result.
        /// </summary>
        /// <remarks>The input date is not modified</remarks>
        public DateTime advance(DateTime d, int n, TimeUnit unit, BusinessDayConvention c = BusinessDayConvention.Following, bool endOfMonth = false)
        {
            if (d == null) throw new ArgumentException("null date");
            if (n == 0)
                return adjust(d, c);
            else if (unit == TimeUnit.Days)
            {
                DateTime d1 = d;
                if (n > 0)
                {
                    while (n > 0)
                    {
                        d1 = d1.AddDays(1);
                        while (isHoliday(d1))
                            d1 = d1.AddDays(1);
                        n--;
                    }
                }
                else
                {
                    while (n < 0)
                    {
                        d1 = d1.AddDays(-1);
                        while (isHoliday(d1))
                            d1 = d1.AddDays(-1);
                        n++;
                    }
                }
                return d1;
            }
            else if (unit == TimeUnit.Weeks)
            {
                DateTime d1 = d.AddDays(n * 7);
                return adjust(d1, c);
            }
            else if (unit == TimeUnit.Months)
            {
                DateTime d1 = d.AddMonths(n);
                if (endOfMonth && unit == TimeUnit.Months && isEndOfMonth(d))
                    return this.endOfMonth(d1);
                return adjust(d1, c);
            }
            else // Years
            {
                DateTime d1 = d.AddYears(n);
                if (endOfMonth && unit == TimeUnit.Years && isEndOfMonth(d))
                    return this.endOfMonth(d1);
                return adjust(d1, c);
            }
        }

        /// <summary>
        /// Calculates the number of business days between two given
        /// dates and returns the result.
        /// </summary>
        public int businessDaysBetween(DateTime from, DateTime to, bool includeFirst = true, bool includeLast = false)
        {
            int wd = 0;
            if (from != to)
            {
                if (from < to)
                {
                    // the last one is treated separately to avoid incrementing Date::maxDate()
                    for (DateTime d = from; d.Date < to.Date; d = d.AddDays(1))
                    {
                        if (isBusinessDay(d))
                            ++wd;
                    }
                    if (isBusinessDay(to))
                        ++wd;
                }
                else if (from > to)
                {
                    for (DateTime d = to; d.Date < from.Date; d = d.AddDays(1))
                    {
                        if (isBusinessDay(d))
                            ++wd;
                    }
                    if (isBusinessDay(from))
                        ++wd;
                }

                if (isBusinessDay(from) && !includeFirst)
                    wd--;
                if (isBusinessDay(to) && !includeLast)
                    wd--;

                if (from > to)
                    wd = -wd;
            }
            return wd;
        }

        /// <summary>
        /// Adds a date to the set of holidays for the given calendar.
        /// </summary>
        public void addHoliday(DateTime d)
        {
            // if d was a genuine holiday previously removed, revert the change
            calendar.removedHolidays.Remove(d);
            // if it's already a holiday, leave the calendar alone.
            // Otherwise, add it.
            if (isBusinessDay(d))
                calendar.addedHolidays.Add(d);
        }
        /// <summary>
        /// Removes a date from the set of holidays for the given calendar.
        /// </summary>
        public void removeHoliday(DateTime d)
        {
            // if d was an artificially-added holiday, revert the change
            calendar.addedHolidays.Remove(d);
            // if it's already a business day, leave the calendar alone.
            // Otherwise, add it.
            if (!isBusinessDay(d))
                calendar.removedHolidays.Add(d);
        }
        /// <summary>
        /// Returns the holidays between two dates
        /// </summary>
        public static List<DateTime> holidayList(Calendar calendar, DateTime from, DateTime to, bool includeWeekEnds = false)
        {
            if (to.Date <= from.Date)
            {
                throw new Exception("'from' date (" + from.Date + ") must be earlier than 'to' date (" + to.Date + ")");
            }

            List<DateTime> result = new List<DateTime>();

            for (DateTime d = from; d.Date <= to.Date; d = d.AddDays(1))
            {
                if (calendar.isHoliday(d)
                    && (includeWeekEnds || !calendar.isWeekend(d.DayOfWeek)))
                    result.Add(d);
            }
            return result;
        }

        /// <summary>
        /// This class provides the means of determining the Easter
        /// Monday for a given year, as well as specifying Saturdays
        /// and Sundays as weekend days.
        /// </summary>
        public class WesternImpl : Calendar
        {		// Western calendars
            public WesternImpl() { }
            public WesternImpl(Calendar c) : base(c) { }

            int[] EasterMonday = {
                          98,  90, 103,  95, 114, 106,  91, 111, 102,   // 1901-1909
		             87, 107,  99,  83, 103,  95, 115,  99,  91, 111,   // 1910-1919
		             96,  87, 107,  92, 112, 103,  95, 108, 100,  91,   // 1920-1929
		            111,  96,  88, 107,  92, 112, 104,  88, 108, 100,   // 1930-1939
		             85, 104,  96, 116, 101,  92, 112,  97,  89, 108,   // 1940-1949
		            100,  85, 105,  96, 109, 101,  93, 112,  97,  89,   // 1950-1959
		            109,  93, 113, 105,  90, 109, 101,  86, 106,  97,   // 1960-1969
		             89, 102,  94, 113, 105,  90, 110, 101,  86, 106,   // 1970-1979
		             98, 110, 102,  94, 114,  98,  90, 110,  95,  86,   // 1980-1989
		            106,  91, 111, 102,  94, 107,  99,  90, 103,  95,   // 1990-1999
		            115, 106,  91, 111, 103,  87, 107,  99,  84, 103,   // 2000-2009
		             95, 115, 100,  91, 111,  96,  88, 107,  92, 112,   // 2010-2019
		            104,  95, 108, 100,  92, 111,  96,  88, 108,  92,   // 2020-2029
		            112, 104,  89, 108, 100,  85, 105,  96, 116, 101,   // 2030-2039
		             93, 112,  97,  89, 109, 100,  85, 105,  97, 109,   // 2040-2049
		            101,  93, 113,  97,  89, 109,  94, 113, 105,  90,   // 2050-2059
		            110, 101,  86, 106,  98,  89, 102,  94, 114, 105,   // 2060-2069
		             90, 110, 102,  86, 106,  98, 111, 102,  94, 114,   // 2070-2079
		             99,  90, 110,  95,  87, 106,  91, 111, 103,  94,   // 2080-2089
		            107,  99,  91, 103,  95, 115, 107,  91, 111, 103,   // 2090-2099
		             88, 108, 100,  85, 105,  96, 109, 101,  93, 112,   // 2100-2109
		             97,  89, 109,  93, 113, 105,  90, 109, 101,  86,   // 2110-2119
		            106,  97,  89, 102,  94, 113, 105,  90, 110, 101,   // 2120-2129
		             86, 106,  98, 110, 102,  94, 114,  98,  90, 110,   // 2130-2139
		             95,  86, 106,  91, 111, 102,  94, 107,  99,  90,   // 2140-2149
		            103,  95, 115, 106,  91, 111, 103,  87, 107,  99,   // 2150-2159
		             84, 103,  95, 115, 100,  91, 111,  96,  88, 107,   // 2160-2169
		             92, 112, 104,  95, 108, 100,  92, 111,  96,  88,   // 2170-2179
		            108,  92, 112, 104,  89, 108, 100,  85, 105,  96,   // 2180-2189
		            116, 101,  93, 112,  97,  89, 109, 100,  85, 105    // 2190-2199
		        };

            public override bool isWeekend(DayOfWeek w) { return w == DayOfWeek.Saturday || w == DayOfWeek.Sunday; }
            /// <summary>
            /// Expressed relative to first day of year
            /// </summary>
            /// <param name="y"></param>
            /// <returns></returns>
            public int easterMonday(int y)
            {
                return EasterMonday[y - 1901];
            }
        }

        //! \name Operators
        //@{
        public static bool operator ==(Calendar c1, Calendar c2)
        {
            // If both are null, or both are same instance, return true.
            if (System.Object.ReferenceEquals(c1, c2))
            {
                return true;
            }

            // If one is null, but not both, return false.
            if (((object)c1 == null) || ((object)c2 == null))
            {
                return false;
            }

            return (c1.empty() && c2.empty())
            || (!c1.empty() && !c2.empty() && c1.name() == c2.name());
        }

        public static bool operator !=(Calendar c1, Calendar c2)
        {
            return !(c1 == c2);
        }
        public override bool Equals(object o) { return (this == (Calendar)o); }
        public override int GetHashCode() { return 0; }
        //@}
    }
}
