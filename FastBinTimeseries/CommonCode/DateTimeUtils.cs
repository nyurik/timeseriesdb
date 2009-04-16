using System;

namespace NYurik.FastBinTimeseries.CommonCode
{
    public static class DateTimeExtensions
    {
        /// <summary>
        /// Convert from unspecified kind as stored in database to UTC, assuming that original value was UTC
        /// </summary>
        public static UtcDateTime DbToUtc(this DateTime dateTime)
        {
            if (dateTime.Kind != DateTimeKind.Unspecified)
                throw new ArgumentOutOfRangeException("dateTime", dateTime,
                                                      "Unexpected - database always stores DateTimes as Unspecified");
            return (UtcDateTime) DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
        }

        /// <summary>
        /// Convert from unspecified kind as stored in database to UTC, assuming that original value was local
        /// </summary>
        public static UtcDateTime DbLocalToUtc(this DateTime dateTime)
        {
            if (dateTime.Kind != DateTimeKind.Unspecified)
                throw new ArgumentOutOfRangeException("dateTime", dateTime,
                                                      "Unexpected - database always stores DateTimes as Unspecified");
            return DateTime.SpecifyKind(dateTime, DateTimeKind.Local).ToUtc();
        }

        /// <summary>
        /// Calls <see cref="DateTime.ToUniversalTime"/> on the value, and converts
        /// to <see cref="UtcDateTime"/>
        /// </summary>
        public static UtcDateTime ToUtc(this DateTime date)
        {
            return (UtcDateTime) date.Date.ToUniversalTime();
        }

        /// <summary>
        /// Calculates the time span between two <see cref="UtcDateTime"/> values,
        /// regardless of which one is less.
        /// </summary>
        public static TimeSpan AbsDifference(this UtcDateTime first, UtcDateTime second)
        {
            return new TimeSpan(Math.Abs(first.Ticks - second.Ticks));
        }

        public static TimeSpan Multiple(this TimeSpan timeSpan, long count)
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
    }
}