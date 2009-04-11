using System;

namespace NYurik.FastBinTimeseries.CommonCode
{
    public static class DateTimeUtils
    {
        /// <summary>
        /// Same as <see cref="DateTime.MaxValue"/> except as UTC kind
        /// </summary>
        public static readonly DateTime UtcMaxValue = new DateTime(DateTime.MaxValue.Ticks, DateTimeKind.Utc);

        /// <summary>
        /// Same as <see cref="DateTime.MinValue"/> except as UTC kind
        /// </summary>
        public static readonly DateTime UtcMinValue = new DateTime(DateTime.MinValue.Ticks, DateTimeKind.Utc);

        /// <summary>
        /// Throws an exception if this DateTime is not UTC kind. Returns input.
        /// </summary>
        public static DateTime EnsureUtc(this DateTime dateTime)
        {
            if (dateTime.Kind != DateTimeKind.Utc)
                throw new ArgumentOutOfRangeException("dateTime", dateTime,
                                                      "DateTime must be in UTC form");
            return dateTime;
        }

        /// <summary>
        /// Throws an exception if this DateTime? is not UTC kind. Returns input.
        /// </summary>
        public static DateTime? EnsureUtc(this DateTime? dateTime)
        {
            return dateTime.HasValue ? dateTime.Value.EnsureUtc() : (DateTime?) null;
        }
    }
}