using System;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public interface IStoredSeries : IDisposable
    {
        /// <summary> Total number of items in the file </summary>
        long Count { get; }

        /// <summary> Type of the items stored in this file </summary>
        Type ItemType { get; }

        /// <summary> User string stored in the header </summary>
        string Tag { get; }

        /// <summary> True when the file has no data </summary>
        bool IsEmpty { get; }

        /// <summary> True when the object has been disposed. No further operations are allowed. </summary>
        bool IsDisposed { get; }

        /// <summary>
        /// Read up to <paramref name="count"/> items beging at <paramref name="firstItemIdx"/>, and return an <see cref="Array"/> object. 
        /// </summary>
        /// <param name="firstItemIdx">Index of the item to start from.</param>
        /// <param name="count">The maximum number of items to read.</param>
        Array GenericReadData(long firstItemIdx, int count);
    }

    public interface IStoredTimeSeries : IStoredSeries
    {
        long BinarySearch(UtcDateTime timestamp);

        /// <summary>
        /// Create a timeseries representing data within the indexed range
        /// </summary>
        ITimeSeries GetTimeSeries(long firstItemIdx, int count);
    }

    public interface IStoredTimeSeries<T> : IStoredTimeSeries
    {
        /// <summary>
        /// Create a timeseries representing data within the indexed range
        /// </summary>
        new ITimeSeries<T> GetTimeSeries(long firstItemIdx, int count);

        /// <summary>
        /// Read as many items as in the <paramref name="buffer"/> or end of file,
        /// starting at <paramref name="fromInclusive"/>.
        /// </summary>
        /// <param name="fromInclusive">Index of the item to start from.</param>
        /// <param name="buffer">Array of values to be read into from a file.</param>
        /// <returns>Number of items read</returns>
        int ReadData(UtcDateTime fromInclusive, ArraySegment<T> buffer);
    }

    public interface IStoredUniformTimeseries<T> : IStoredTimeSeries<T>, IStoredUniformTimeseries
    {
    }

    public interface IStoredUniformTimeseries : IStoredSeries
    {
        /// <summary>
        /// The timestamp of the first item in the file.
        /// </summary>
        UtcDateTime FirstTimestamp { get; }

        /// <summary>
        /// Represents the timestamp of the first value beyond the end of the existing data.
        /// (<see cref="IStoredSeries.Count"/> as a timestamp)
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