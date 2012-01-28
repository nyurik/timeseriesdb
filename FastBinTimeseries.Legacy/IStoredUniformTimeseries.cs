using System;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public interface IStoredUniformTimeseries : IStoredSeries
    {
        /// <summary>
        /// The timestamp of the first item in the file.
        /// </summary>
        UtcDateTime FirstTimestamp { get; }

        /// <summary>
        /// Represents the timestamp of the first value beyond the end of the existing data.
        /// (<see cref="IStoredSeries.GetItemCount"/> as a timestamp)
        /// </summary>
        UtcDateTime FirstUnavailableTimestamp { get; }

        /// <summary>
        /// Span of time each item represents.
        /// </summary>
        TimeSpan ItemTimeSpan { get; }

        /// <summary>
        /// Generic version of <see cref="BinUniformTimeseriesFile{T}.ReadData(UtcDateTime,UtcDateTime)"/>.
        /// Read data starting at <paramref name="fromInclusive"/>, up to, but not including <paramref name="toExclusive"/>,
        /// and return an <see cref="Array"/> object. 
        /// </summary>
        Array GenericReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive);

        /// <summary>
        /// Read <paramref name="count"/> items starting at <paramref name="fromInclusive"/>.
        /// and return an <see cref="Array"/> object. 
        /// </summary>
        /// <param name="fromInclusive">Index of the item to start from.</param>
        /// <param name="count">The number of items to be read.</param>
        Array GenericReadData(UtcDateTime fromInclusive, int count);
    }
}