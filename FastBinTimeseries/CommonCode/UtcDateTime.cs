using System;
using System.Globalization;
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
    [Serializable, StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct UtcDateTime : IComparable, IFormattable, IConvertible, IComparable<UtcDateTime>,
                                IEquatable<UtcDateTime>
    {
        internal const string FormatDateOnly = "yyyy-MM-dd";
        internal const string FormatDateTimeMin = "yyyy-MM-dd HH:mm";
        internal const string FormatDateTimeSec = "yyyy-MM-dd HH:mm:ss";
        internal const string FormatDateTimeMs = "yyyy-MM-dd HH:mm:ss.ffff";

        /// <summary>
        /// Same as <see cref="DateTime.MaxValue"/> except as UTC kind
        /// </summary>
        public static readonly UtcDateTime MaxValue =
            (UtcDateTime) DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc);

        /// <summary>
        /// Same as <see cref="DateTime.MinValue"/> except as UTC kind
        /// </summary>
        public static readonly UtcDateTime MinValue =
            (UtcDateTime) DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc);

        private readonly long _value;

        public UtcDateTime(long ticks)
        {
            _value = ticks;
        }

        public UtcDateTime(int year, int month, int day)
            : this(year, month, day, 0, 0, 0, 0)
        {
        }

        public UtcDateTime(int year, int month, int day, int hour, int minute, int second)
            : this(year, month, day, hour, minute, second, 0)
        {
        }

        public UtcDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond)
            : this(new DateTime(year, month, day, hour, minute, second, millisecond,  DateTimeKind.Utc))
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
            get { return (UtcDateTime) DateTime.UtcNow; }
        }

        public static UtcDateTime Today
        {
            get { return (UtcDateTime) DateTime.UtcNow.Date; }
        }

        #region Properties

        public int Day
        {
            get { return ((DateTime) this).Day; }
        }

        public int DayOfYear
        {
            get { return ((DateTime) this).DayOfYear; }
        }

        public int Hour
        {
            get { return ((DateTime) this).Hour; }
        }

        public DateTimeKind Kind
        {
            get { return DateTimeKind.Utc; }
        }

        public int Millisecond
        {
            get { return ((DateTime) this).Millisecond; }
        }

        public int Minute
        {
            get { return ((DateTime) this).Minute; }
        }

        public int Month
        {
            get { return ((DateTime) this).Month; }
        }

        public int Second
        {
            get { return ((DateTime) this).Second; }
        }

        public long Ticks
        {
            get { return _value; }
        }

        public TimeSpan TimeOfDay
        {
            get { return ((DateTime) this).TimeOfDay; }
        }

        public int Year
        {
            get { return ((DateTime) this).Year; }
        }

        public UtcDateTime Date
        {
            get { return (UtcDateTime) ((DateTime) this).Date; }
        }

        public DayOfWeek DayOfWeek
        {
            get { return ((DateTime) this).DayOfWeek; }
        }

        #endregion

        #region IComparable Members

        public int CompareTo(object value)
        {
            if (value is UtcDateTime)
                return CompareTo((UtcDateTime) value);
            throw new ArgumentException("UtcDateTime is not comparable with " + value.GetType());
        }

        #endregion

        #region IComparable<UtcDateTime> Members

        public int CompareTo(UtcDateTime other)
        {
            return _value.CompareTo(other._value);
        }

        #endregion

        #region IConvertible Members

        bool IConvertible.ToBoolean(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToBoolean(provider);
        }

        char IConvertible.ToChar(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToChar(provider);
        }

        sbyte IConvertible.ToSByte(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToSByte(provider);
        }

        byte IConvertible.ToByte(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToByte(provider);
        }

        short IConvertible.ToInt16(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToInt16(provider);
        }

        ushort IConvertible.ToUInt16(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToUInt16(provider);
        }

        int IConvertible.ToInt32(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToInt32(provider);
        }

        uint IConvertible.ToUInt32(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToUInt32(provider);
        }

        long IConvertible.ToInt64(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToInt64(provider);
        }

        ulong IConvertible.ToUInt64(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToUInt64(provider);
        }

        float IConvertible.ToSingle(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToSingle(provider);
        }

        double IConvertible.ToDouble(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToDouble(provider);
        }

        decimal IConvertible.ToDecimal(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToDecimal(provider);
        }

        DateTime IConvertible.ToDateTime(IFormatProvider provider)
        {
            return ((IConvertible) ((DateTime) this)).ToDateTime(provider);
        }

        object IConvertible.ToType(Type conversionType, IFormatProvider provider)
        {
            if (conversionType == typeof(UtcDateTime))
                return this;
            if (conversionType == typeof(DateTime))
                return (DateTime) this;
            if (conversionType == typeof(string))
                return ToString(provider);
            throw new InvalidCastException("Cannot convert to " + conversionType);
        }

        public TypeCode GetTypeCode()
        {
            return ((DateTime) this).GetTypeCode();
        }

        public string ToString(IFormatProvider provider)
        {
            return ToString(null, provider);
        }

        #endregion

        #region IFormattable Members

        /// <summary>
        /// Custom formats: "G" or null - show just the significant (non-zero) part of the datetime. "L" - same but in local time
        /// </summary>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            var value = (DateTime) this;

            switch (format)
            {
                case "L":
                    
                    if (value != value.Date)
                    {
                        value = ToLocalTime();
                        goto case null;
                    }
                    formatProvider = CultureInfo.InvariantCulture;
                    format = FormatDateOnly;
                    break;

                case null:
                case "G":

                    formatProvider = CultureInfo.InvariantCulture;
                    TimeSpan timeOfDay = value.TimeOfDay;

                    if (timeOfDay == TimeSpan.Zero)
                        format = FormatDateOnly;
                    else if (timeOfDay.Milliseconds == 0)
                        format = timeOfDay.Seconds == 0 ? FormatDateTimeMin : FormatDateTimeSec;
                    else
                        format = FormatDateTimeMs;
                    break;
            }

            return value.ToString(format, formatProvider);
        }

        #endregion

        #region DateTime Arithmetic

        public UtcDateTime Add(TimeSpan value)
        {
            return AddTicks(value.Ticks);
        }

        public UtcDateTime AddDays(double value)
        {
            return (UtcDateTime) ((DateTime) this).AddDays(value);
        }

        public UtcDateTime AddHours(double value)
        {
            return (UtcDateTime) ((DateTime) this).AddHours(value);
        }

        public UtcDateTime AddMilliseconds(double value)
        {
            return (UtcDateTime) ((DateTime) this).AddMilliseconds(value);
        }

        public UtcDateTime AddMinutes(double value)
        {
            return (UtcDateTime) ((DateTime) this).AddMinutes(value);
        }

        public UtcDateTime AddMonths(int months)
        {
            return (UtcDateTime) ((DateTime) this).AddMonths(months);
        }

        public UtcDateTime AddSeconds(double value)
        {
            return (UtcDateTime) ((DateTime) this).AddSeconds(value);
        }

        public UtcDateTime AddTicks(long value)
        {
            if (value < -_value || value > (MaxValue._value - _value))
                throw new ArgumentOutOfRangeException("value", "Arithmetic overflow");

            return new UtcDateTime(_value + value);
        }

        public UtcDateTime AddYears(int value)
        {
            return (UtcDateTime) ((DateTime) this).AddYears(value);
        }

        public TimeSpan Subtract(UtcDateTime value)
        {
            return ((DateTime) this).Subtract((DateTime) value);
        }

        public UtcDateTime Subtract(TimeSpan value)
        {
            return AddTicks(-value.Ticks);
        }

        public DateTime ToLocalTime()
        {
            return ((DateTime) this).ToLocalTime();
        }

        #endregion

        #region Equality

        public int CompareTo(DateTime value)
        {
            return ((DateTime) this).CompareTo(value);
        }

        public bool Equals(UtcDateTime other)
        {
            return _value == other._value;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (UtcDateTime)) return false;
            return Equals((UtcDateTime) obj);
        }

        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        #endregion

        #region Formatting

        public string[] GetDateTimeFormats()
        {
            return ((DateTime) this).GetDateTimeFormats();
        }

        public string[] GetDateTimeFormats(IFormatProvider provider)
        {
            return ((DateTime) this).GetDateTimeFormats(provider);
        }

        public string[] GetDateTimeFormats(char format)
        {
            return ((DateTime) this).GetDateTimeFormats(format);
        }

        public string[] GetDateTimeFormats(char format, IFormatProvider provider)
        {
            return ((DateTime) this).GetDateTimeFormats(format, provider);
        }

        public string ToLongDateString()
        {
            return ((DateTime) this).ToLongDateString();
        }

        public string ToLongTimeString()
        {
            return ((DateTime) this).ToLongTimeString();
        }

        public string ToShortDateString()
        {
            return ((DateTime) this).ToShortDateString();
        }

        public string ToShortTimeString()
        {
            return ((DateTime) this).ToShortTimeString();
        }

        public override string ToString()
        {
            return ToString(null, null);
        }

        public string ToString(string format)
        {
            return ToString(format, null);
        }

        #endregion

        #region Operators

        public static UtcDateTime operator +(UtcDateTime d, TimeSpan t)
        {
            return d.AddTicks(t.Ticks);
        }

        public static bool operator ==(UtcDateTime left, UtcDateTime right)
        {
            return left._value == right._value;
        }

        public static explicit operator UtcDateTime(DateTime value)
        {
            return new UtcDateTime(value);
        }

        public static bool operator >(UtcDateTime t1, UtcDateTime t2)
        {
            return t1._value > t2._value;
        }

        public static bool operator >=(UtcDateTime t1, UtcDateTime t2)
        {
            return t1._value >= t2._value;
        }

        public static explicit operator DateTime(UtcDateTime value)
        {
            return new DateTime(value._value, DateTimeKind.Utc);
        }

        public static bool operator !=(UtcDateTime left, UtcDateTime right)
        {
            return left._value != right._value;
        }

        public static bool operator <(UtcDateTime t1, UtcDateTime t2)
        {
            return t1._value < t2._value;
        }

        public static bool operator <=(UtcDateTime t1, UtcDateTime t2)
        {
            return t1._value <= t2._value;
        }

        public static TimeSpan operator -(UtcDateTime d1, UtcDateTime d2)
        {
            return new TimeSpan(d1._value - d2._value);
        }

        public static UtcDateTime operator -(UtcDateTime d, TimeSpan t)
        {
            return d.Subtract(t);
        }

        #endregion
    }
}