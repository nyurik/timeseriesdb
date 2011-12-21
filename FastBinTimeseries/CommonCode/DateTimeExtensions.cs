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

namespace NYurik.FastBinTimeseries.CommonCode
{
    public static class DateTimeExtensions
    {
        private const string FormatDateOnly = "yyyy-MM-dd";
        private const string FormatDateTime = "yyyy-MM-dd HH:mm:ss";
        private const string FormatDateTimeMs = "yyyy-MM-dd HH:mm:ss.ffff";

        /// <summary>
        /// Convert from unspecified kind as stored in database to UTC, assuming that original value was UTC
        /// </summary>
        public static UtcDateTime DbToUtc(this DateTime dateTime)
        {
            if (dateTime.Kind != DateTimeKind.Unspecified)
                throw new ArgumentOutOfRangeException(
                    "dateTime", dateTime,
                    "Unexpected - database always stores DateTimes as Unspecified");
            return (UtcDateTime) DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
        }

        /// <summary>
        /// Convert from unspecified kind as stored in database to UTC, assuming that original value was local
        /// </summary>
        public static UtcDateTime DbLocalToUtc(this DateTime dateTime)
        {
            if (dateTime.Kind != DateTimeKind.Unspecified)
                throw new ArgumentOutOfRangeException(
                    "dateTime", dateTime,
                    "Unexpected - database always stores DateTimes as Unspecified");
            return DateTime.SpecifyKind(dateTime, DateTimeKind.Local).ToUtc();
        }

        /// <summary>
        /// Calls <see cref="DateTime.ToUniversalTime"/> on the value, and converts
        /// to <see cref="UtcDateTime"/>
        /// </summary>
        public static UtcDateTime ToUtc(this DateTime dateTime)
        {
            return (UtcDateTime) dateTime.ToUniversalTime();
        }

        /// <summary>
        /// Calculates the time span between two <see cref="UtcDateTime"/> values,
        /// regardless of which one is less.
        /// </summary>
        public static TimeSpan AbsDifference(this UtcDateTime first, UtcDateTime second)
        {
            return new TimeSpan(Math.Abs(first.Ticks - second.Ticks));
        }

        public static TimeSpan Multiply(this TimeSpan timeSpan, long count)
        {
            return TimeSpan.FromTicks(timeSpan.Ticks*count);
        }

        /// <summary>
        /// Calculate the number of <paramref name="divisor"/>s 
        /// </summary>
        /// <param name="dividend">Value to divide</param>
        /// <param name="divisor">Value by which to divide</param>
        /// <returns>Number of times the <paramref name="divisor"/> would fit in the <paramref name="dividend"/>.</returns>
        public static long DivideEven(this TimeSpan dividend, TimeSpan divisor)
        {
            if (dividend.Ticks%divisor.Ticks != 0)
                throw new ArgumentException(
                    String.Format("Dividend '{0}' may not be evenly divided by the divisor '{1}'", dividend, divisor));
            return dividend.Ticks/divisor.Ticks;
        }

        /// <summary>
        /// Converts DateTime to string
        /// Includes time if the value is not at midnight, includes milliseconds if not 0
        /// </summary>
        public static string ToStringAuto(this DateTime dateTime)
        {
            if (dateTime == dateTime.Date)
                return dateTime.ToString(FormatDateOnly);
            if (dateTime.Millisecond == 0)
                return dateTime.ToString(FormatDateTime);
            return dateTime.ToString(FormatDateTimeMs);
        }

        /// <summary>
        /// Converts UtcDateTime to string
        /// Includes time if the value is not at midnight, includes milliseconds if not 0
        /// </summary>
        public static string ToStringAuto(this UtcDateTime dateTime)
        {
            if (dateTime == dateTime.Date)
                return dateTime.ToString(FormatDateOnly);
            if (dateTime.Millisecond == 0)
                return dateTime.ToString(FormatDateTime);
            return dateTime.ToString(FormatDateTimeMs);
        }

        /// <summary>
        /// Converts UtcDateTime to local string
        /// Includes time if the value is not at midnight, includes milliseconds if not 0
        /// </summary>
        public static string ToStringAutoLocal(this UtcDateTime dateTime)
        {
            return dateTime == dateTime.Date
                       ? dateTime.ToString(FormatDateOnly)
                       : dateTime.ToLocalTime().ToStringAuto();
        }
    }
}