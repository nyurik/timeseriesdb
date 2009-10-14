using System;
using System.Runtime.InteropServices;

namespace NYurik.FastBinTimeseries.CommonCode
{
    /// <summary>
    /// This is a replacement of the <see cref="DateTime"/> object that ensures type safety
    /// when dealing with the Coordinated Univeral Time (UTC). Internally, the value is
    /// stored as a long tick count.
    /// </summary>
    /// <remarks>
    /// Use this struct instead of the <see cref="DateTime"/> to store 
    /// the date as a UTC value in a 1-byte-packed structures.
    /// DateTime may not be used in serialization due to different packing on
    /// 32bit and 64bit architectures.
    /// </remarks>
    [Serializable]
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct UtcDateTime : IFormattable, IEquatable<UtcDateTime>, IComparable<UtcDateTime>
    {
        /// <summary>
        /// Same as <see cref="DateTime.MaxValue"/> except as UTC kind
        /// </summary>
        public static readonly UtcDateTime MaxValue =
            (UtcDateTime)new DateTime(DateTime.MaxValue.Ticks, DateTimeKind.Utc);

        /// <summary>
        /// Same as <see cref="DateTime.MinValue"/> except as UTC kind
        /// </summary>
        public static readonly UtcDateTime MinValue =
            (UtcDateTime)new DateTime(DateTime.MinValue.Ticks, DateTimeKind.Utc);

        private readonly long _value;

        public UtcDateTime(long ticks)
        {
            _value = ticks;
        }

        public UtcDateTime(int year, int month, int day)
            : this(new DateTime(year, month, day, 0, 0, 0, DateTimeKind.Utc))
        {
        }

        public UtcDateTime(DateTime value)
        {
            if (value.Kind == DateTimeKind.Utc)
                _value = value.Ticks;
            else
                throw new ArgumentOutOfRangeException(
                    "value", value,
                    "DateTime must be in UTC\n" +
                    "You may either use value.ToUniversalTime() or DateTime.SpecifyKind(value, DateTimeKind.Utc)to convert.");
        }

        public static UtcDateTime Now
        {
            get { return (UtcDateTime)DateTime.UtcNow; }
        }

        public static UtcDateTime Today
        {
            get { return (UtcDateTime)DateTime.UtcNow.Date; }
        }

        /// <summary>
        /// If <paramref name="date"/> is a date-only value, treat it as UTC, otherwise throws an error
        /// </summary>
        public static UtcDateTime DateOnly(DateTime date)
        {
            if (date.Date != date)
                throw new ArgumentOutOfRangeException(
                    "date", date, "The value must be a date-only value");

            return (UtcDateTime)DateTime.SpecifyKind(date, DateTimeKind.Utc);
        }

        #region Properties

        public long Ticks
        {
            get { return _value; }
        }

        public int Day
        {
            get { return ((DateTime)this).Day; }
        }

        public int DayOfYear
        {
            get { return ((DateTime)this).DayOfYear; }
        }

        public int Hour
        {
            get { return ((DateTime)this).Hour; }
        }

        public int Millisecond
        {
            get { return ((DateTime)this).Millisecond; }
        }

        public int Minute
        {
            get { return ((DateTime)this).Minute; }
        }

        public int Month
        {
            get { return ((DateTime)this).Month; }
        }

        public int Second
        {
            get { return ((DateTime)this).Second; }
        }

        public TimeSpan TimeOfDay
        {
            get { return ((DateTime)this).TimeOfDay; }
        }

        public int Year
        {
            get { return ((DateTime)this).Year; }
        }

        public UtcDateTime Date
        {
            get { return (UtcDateTime)((DateTime)this).Date; }
        }

        public DayOfWeek DayOfWeek
        {
            get { return ((DateTime)this).DayOfWeek; }
        }

        #endregion

        #region Equality

        #region IComparable<UtcDateTime> Members

        public int CompareTo(UtcDateTime other)
        {
            return _value.CompareTo(other._value);
        }

        #endregion

        #region IEquatable<UtcDateTime> Members

        public bool Equals(UtcDateTime other)
        {
            return other._value == _value;
        }

        #endregion

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof(UtcDateTime)) return false;
            return Equals((UtcDateTime)obj);
        }

        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        #endregion

        #region Operators

        public static implicit operator DateTime(UtcDateTime value)
        {
            return new DateTime(value._value, DateTimeKind.Utc);
        }

        public static explicit operator UtcDateTime(DateTime value)
        {
            return new UtcDateTime(value);
        }

        public static TimeSpan operator -(UtcDateTime d1, UtcDateTime d2)
        {
            return d1 - (DateTime)d2;
        }

        public static UtcDateTime operator -(UtcDateTime d, TimeSpan t)
        {
            return (UtcDateTime)((DateTime)d - t);
        }

        public static UtcDateTime operator +(UtcDateTime d, TimeSpan t)
        {
            return (UtcDateTime)((DateTime)d + t);
        }

        public static bool operator ==(UtcDateTime left, UtcDateTime right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(UtcDateTime left, UtcDateTime right)
        {
            return !left.Equals(right);
        }

        public static bool operator <(UtcDateTime t1, UtcDateTime t2)
        {
            return t1._value < t2._value;
        }

        public static bool operator <=(UtcDateTime t1, UtcDateTime t2)
        {
            return t1._value <= t2._value;
        }

        public static bool operator >(UtcDateTime t1, UtcDateTime t2)
        {
            return t1._value > t2._value;
        }

        public static bool operator >=(UtcDateTime t1, UtcDateTime t2)
        {
            return t1._value >= t2._value;
        }

        #endregion

        #region Methods

        public UtcDateTime Add(TimeSpan value)
        {
            return (UtcDateTime)((DateTime)this).Add(value);
        }

        public UtcDateTime AddDays(double value)
        {
            return (UtcDateTime)((DateTime)this).AddDays(value);
        }

        public UtcDateTime AddHours(double value)
        {
            return (UtcDateTime)((DateTime)this).AddHours(value);
        }

        public UtcDateTime AddMilliseconds(double value)
        {
            return (UtcDateTime)((DateTime)this).AddMilliseconds(value);
        }

        public UtcDateTime AddMinutes(double value)
        {
            return (UtcDateTime)((DateTime)this).AddMinutes(value);
        }

        public UtcDateTime AddMonths(int months)
        {
            return (UtcDateTime)((DateTime)this).AddMonths(months);
        }

        public UtcDateTime AddSeconds(double value)
        {
            return (UtcDateTime)((DateTime)this).AddSeconds(value);
        }

        public UtcDateTime AddTicks(long value)
        {
            return (UtcDateTime)((DateTime)this).AddTicks(value);
        }

        public UtcDateTime AddYears(int value)
        {
            return (UtcDateTime)((DateTime)this).AddYears(value);
        }

        #endregion

        #region Formatting

        public override string ToString()
        {
            return ((DateTime)this).ToString();
        }

        public string ToString(string format)
        {
            return ((DateTime)this).ToString(format);
        }

        public string ToString(IFormatProvider provider)
        {
            return ((DateTime)this).ToString(provider);
        }

        public string ToString(string format, IFormatProvider formatProvider)
        {
            return ((DateTime)this).ToString(format, formatProvider);
        }

        public DateTime ToLocalTime()
        {
            return ((DateTime)this).ToLocalTime();
        }

        public string ToShortDateString()
        {
            return ((DateTime)this).ToShortDateString();
        }

        public string ToShortTimeString()
        {
            return ((DateTime)this).ToShortTimeString();
        }

        public string ToLongDateString()
        {
            return ((DateTime)this).ToLongDateString();
        }

        public string ToLongTimeString()
        {
            return ((DateTime)this).ToLongTimeString();
        }

        #endregion
    }
}