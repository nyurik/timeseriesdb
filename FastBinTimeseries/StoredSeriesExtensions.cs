using System;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public static class StoredSeriesExtensions
    {
        public static UtcDateTime ValidateIndex(this IStoredUniformTimeseries timeseries, UtcDateTime timestamp)
        {
            if (timestamp.Ticks%timeseries.ItemTimeSpan.Ticks != 0)
                throw new ArgumentException(
                    String.Format(
                        "The timestamp {0} must be aligned by the time slice {1}", timestamp,
                        timeseries.ItemTimeSpan));
            return timestamp;
        }

        public static long IndexToLong(this IStoredUniformTimeseries series, UtcDateTime timestamp)
        {
            return (series.ValidateIndex(timestamp).Ticks - series.FirstTimestamp.Ticks)/series.ItemTimeSpan.Ticks;
        }

        public static int ToIntCountChecked(this long value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException("value", value, "<0");
            if (value > Int32.MaxValue)
                throw new ArgumentException(
                    String.Format(
                        "Attempted to process {0} items at once, which is over the maximum of {1}.",
                        value, Int32.MaxValue));
            return (int) value;
        }

        /// <summary>
        /// Convert item index to the timestamp
        /// </summary>
        public static UtcDateTime IndexToTimestamp(this IStoredUniformTimeseries series, long index)
        {
            return series.FirstTimestamp + series.ItemTimeSpan.Multiply(index);
        }
    }
}