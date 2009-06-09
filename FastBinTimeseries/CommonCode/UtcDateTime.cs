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
    [Serializable, StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct UtcDateTime : IComparable, IFormattable, IConvertible, IComparable<UtcDateTime>,
                                IEquatable<UtcDateTime>
    {
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
            return ((IConvertible) ((DateTime) this)).ToType(conversionType, provider);
        }

        public TypeCode GetTypeCode()
        {
            return ((DateTime) this).GetTypeCode();
        }

        public string ToString(IFormatProvider provider)
        {
            return ((DateTime) this).ToString(provider);
        }

        #endregion

        #region IFormattable Members

        public string ToString(string format, IFormatProvider formatProvider)
        {
            return ((DateTime) this).ToString(format, formatProvider);
        }

        #endregion

        #region DateTime Arithmetic

        public UtcDateTime Add(TimeSpan value)
        {
            return (UtcDateTime) ((DateTime) this).Add(value);
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
            return (UtcDateTime) ((DateTime) this).AddTicks(value);
        }

        public UtcDateTime AddYears(int value)
        {
            return (UtcDateTime) ((DateTime) this).AddYears(value);
        }

        public TimeSpan Subtract(UtcDateTime value)
        {
            return ((DateTime) this).Subtract(value);
        }

        public UtcDateTime Subtract(TimeSpan value)
        {
            return (UtcDateTime) ((DateTime) this).Subtract(value);
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
            return other._value.Equals(_value);
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
            return this.ToStringAuto();
        }

        public string ToString(string format)
        {
            return ((DateTime) this).ToString(format);
        }

        #endregion

        #region Operators

        public static UtcDateTime operator +(UtcDateTime d, TimeSpan t)
        {
            return (UtcDateTime) ((DateTime) d + t);
        }

        public static bool operator ==(UtcDateTime left, UtcDateTime right)
        {
            return left.Equals(right);
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

        public static implicit operator DateTime(UtcDateTime value)
        {
            return new DateTime(value._value, DateTimeKind.Utc);
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

        public static TimeSpan operator -(UtcDateTime d1, UtcDateTime d2)
        {
            return d1 - (DateTime) d2;
        }

        public static UtcDateTime operator -(UtcDateTime d, TimeSpan t)
        {
            return (UtcDateTime) ((DateTime) d - t);
        }

        #endregion
    }
}