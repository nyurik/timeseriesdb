#region COPYRIGHT
/*
 *     Copyright 2009-2011 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of FastBinTimeseries library
 * 
 *  FastBinTimeseries is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  FastBinTimeseries is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with FastBinTimeseries.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
#endregion

using System;
using System.Diagnostics.Contracts;
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
        public const long MinTicks = 0; // UtcDateTime.MinValue.Ticks;
        public const long MaxTicks = 3155378975999999999; // UtcDateTime.MaxValue.Ticks;

        internal const string FormatDateOnly = "yyyy-MM-dd";
        internal const string FormatDateTimeMin = "yyyy-MM-dd HH:mm";
        internal const string FormatDateTimeSec = "yyyy-MM-dd HH:mm:ss";
        internal const string FormatDateTimeMs = "yyyy-MM-dd HH:mm:ss.fff";
        internal const string FormatDateTimeComplete = "yyyy-MM-dd HH:mm:ss.fffffff";

        /// <summary>
        /// Same as <see cref="DateTime.MaxValue"/> except as UTC kind
        /// </summary>
        public static readonly UtcDateTime MaxValue = new UtcDateTime(MaxTicks);

        /// <summary>
        /// Same as <see cref="DateTime.MinValue"/> except as UTC kind
        /// </summary>
        public static readonly UtcDateTime MinValue = new UtcDateTime(MinTicks);

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
            : this(new DateTime(year, month, day, hour, minute, second, millisecond, DateTimeKind.Utc))
        {
        }

        public UtcDateTime(DateTime value)
        {
            if (value.Kind != DateTimeKind.Utc)
                throw new ArgumentOutOfRangeException(
                    "value", value,
                    "DateTime must be in UTC\n" +
                    "You may either use value.ToUniversalTime() or DateTime.SpecifyKind(value, DateTimeKind.Utc)to convert.");
            _value = value.Ticks;
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
            get { return (int) (_value/TimeSpan.TicksPerHour%24); }
        }

        public int Millisecond
        {
            get { return (int) (_value/TimeSpan.TicksPerMillisecond%1000); }
        }

        public int Minute
        {
            get { return (int) (_value/TimeSpan.TicksPerMinute%60); }
        }

        public int Month
        {
            get { return ((DateTime) this).Month; }
        }

        public int Second
        {
            get { return (int) (_value/TimeSpan.TicksPerSecond%60); }
        }

        public long Ticks
        {
            get { return _value; }
        }

        public TimeSpan TimeOfDay
        {
            get { return new TimeSpan(_value%TimeSpan.TicksPerDay); }
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
            get { return (DayOfWeek) ((_value/TimeSpan.TicksPerDay + 1)%7); }
        }

        #endregion

        #region IComparable Members

        [Pure]
        public int CompareTo(object value)
        {
            if (value is UtcDateTime)
                return CompareTo((UtcDateTime) value);
            throw new ArgumentException("UtcDateTime is not comparable with " + value.GetType());
        }

        #endregion

        #region IComparable<UtcDateTime> Members

        [Pure]
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
            if (conversionType == null)
                throw new ArgumentNullException("conversionType");
            if (conversionType == typeof (UtcDateTime))
                return this;
            if (conversionType == typeof (DateTime))
                return (DateTime) this;
            if (conversionType == typeof (string))
                return ToString(provider);
            if (conversionType == typeof (object))
                return this;
            throw new InvalidCastException("Cannot convert to " + conversionType);
        }

        [Pure]
        public TypeCode GetTypeCode()
        {
            return ((DateTime) this).GetTypeCode();
        }

        [Pure]
        public string ToString(IFormatProvider provider)
        {
            return ToString(null, provider);
        }

        #endregion

        #region IFormattable Members

        /// <summary>
        /// Custom formats: "G" or null - show just the significant (non-zero) part of the datetime. "L" - same but in local time
        /// </summary>
        [Pure]
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

        private UtcDateTime AddScaled(double value, int scale)
        {
            const long minMillisecond = -315537897600000L;
            const long maxMillisecond = 315537897600000L;

            var val = (long) (value*scale + (value >= 0.0 ? 0.5 : -0.5));
            if (val <= minMillisecond || val >= maxMillisecond)
                throw new ArgumentOutOfRangeException("value", "Arithmetic overflow");

            return AddTicks(val*TimeSpan.TicksPerMillisecond);
        }

        [Pure]
        public UtcDateTime Add(TimeSpan value)
        {
            return AddTicks(value.Ticks);
        }

        [Pure]
        public UtcDateTime AddDays(double value)
        {
            return AddScaled(value, (int) (TimeSpan.TicksPerDay/TimeSpan.TicksPerMillisecond));
        }

        [Pure]
        public UtcDateTime AddHours(double value)
        {
            return AddScaled(value, (int) (TimeSpan.TicksPerHour/TimeSpan.TicksPerMillisecond));
        }

        [Pure]
        public UtcDateTime AddMilliseconds(double value)
        {
            return AddScaled(value, 1);
        }

        [Pure]
        public UtcDateTime AddMinutes(double value)
        {
            return AddScaled(value, (int) (TimeSpan.TicksPerMinute/TimeSpan.TicksPerMillisecond));
        }

        [Pure]
        public UtcDateTime AddMonths(int months)
        {
            return (UtcDateTime) ((DateTime) this).AddMonths(months);
        }

        [Pure]
        public UtcDateTime AddSeconds(double value)
        {
            return AddScaled(value, (int) (TimeSpan.TicksPerSecond/TimeSpan.TicksPerMillisecond));
        }

        [Pure]
        public UtcDateTime AddTicks(long value)
        {
            if (value < -_value || value > (MaxValue._value - _value))
                throw new ArgumentOutOfRangeException("value", "Arithmetic overflow");

            return new UtcDateTime(_value + value);
        }

        [Pure]
        public UtcDateTime AddYears(int value)
        {
            return (UtcDateTime) ((DateTime) this).AddYears(value);
        }

        [Pure]
        public TimeSpan Subtract(UtcDateTime value)
        {
            return new TimeSpan(_value - value._value);
        }

        [Pure]
        public UtcDateTime Subtract(TimeSpan value)
        {
            return AddTicks(-value.Ticks);
        }

        [Pure]
        public DateTime ToLocalTime()
        {
            return ((DateTime) this).ToLocalTime();
        }

        #endregion

        #region Equality

        [Pure]
        public int CompareTo(DateTime value)
        {
            return ((DateTime) this).CompareTo(value);
        }

        [Pure]
        public bool Equals(UtcDateTime other)
        {
            return _value == other._value;
        }

        [Pure]
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (UtcDateTime)) return false;
            return Equals((UtcDateTime) obj);
        }

        [Pure]
        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        #endregion

        #region Formatting

        [Pure]
        public string[] GetDateTimeFormats()
        {
            return ((DateTime) this).GetDateTimeFormats();
        }

        [Pure]
        public string[] GetDateTimeFormats(IFormatProvider provider)
        {
            return ((DateTime) this).GetDateTimeFormats(provider);
        }

        [Pure]
        public string[] GetDateTimeFormats(char format)
        {
            return ((DateTime) this).GetDateTimeFormats(format);
        }

        [Pure]
        public string[] GetDateTimeFormats(char format, IFormatProvider provider)
        {
            return ((DateTime) this).GetDateTimeFormats(format, provider);
        }

        [Pure]
        public string ToLongDateString()
        {
            return ToString("D", null);
        }

        [Pure]
        public string ToLongTimeString()
        {
            return ToString("T", null);
        }

        [Pure]
        public string ToShortDateString()
        {
            return ToString("d", null);
        }

        [Pure]
        public string ToShortTimeString()
        {
            return ToString("t", null);
        }

        [Pure]
        public override string ToString()
        {
            return ToString(null, null);
        }

        [Pure]
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