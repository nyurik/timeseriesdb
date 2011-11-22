using System;
using System.Collections.Generic;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public interface IEnumerableFeed : IGenericInvoker, IDisposable
    {
        /// <summary> Type of the items stored in this file </summary>
        Type ItemType { get; }

        /// <summary> User string stored in the header </summary>
        string Tag { get; }
    }

    public interface IEnumerableFeed<TInd, TVal> : IEnumerableFeed
        where TInd : struct, IComparable<TInd>
    {
        /// <summary>
        /// Returns function that can extract TInd index from a given value T
        /// </summary>
        Func<TVal, TInd> IndexAccessor { get; }

        /// <summary>
        /// Enumerate all items one block at a time using an internal buffer.
        /// </summary>
        /// <param name="from">The index of the first element to read. Inclusive if going forward, exclusive when going backwards</param>
        /// <param name="inReverse">Set to true if you want to enumerate backwards, from last to first</param>
        /// <param name="bufferSize">Size of the read buffer. If 0, the buffer will start small and grow with time</param>
        IEnumerable<ArraySegment<TVal>> StreamSegments(TInd from, bool inReverse = false, int bufferSize = 0);
    }

    //[Obsolete("Use IEnumerableFeed<TInd, TVal> instead")]
    public interface IEnumerableFeed<T> : IEnumerableFeed<UtcDateTime, T>
    {
//        /// <summary>
//        /// Returns function that can extract timestamp from a given value T
//        /// </summary>
//        Func<T, UtcDateTime> TimestampAccessor { get; }
//
//
//        /// <summary>
//        /// Enumerate all items one block at a time using an internal buffer.
//        /// </summary>
//        /// <param name="from">The index of the first element to read. Inclusive if going forward, exclusive when going backwards</param>
//        /// <param name="inReverse">Set to true if you want to enumerate backwards, from last to first</param>
//        /// <param name="bufferSize">Size of the read buffer. If 0, the buffer will start small and grow with time</param>
//        IEnumerable<ArraySegment<T>> StreamSegments(UtcDateTime from, bool inReverse = false, int bufferSize = 0);
    }

    public interface IStoredSeries : IDisposable
    {
        /// <summary> Type of the items stored in this file </summary>
        Type ItemType { get; }

        /// <summary> User string stored in the header </summary>
        string Tag { get; }

        /// <summary> True when the file has no data </summary>
        bool IsEmpty { get; }

        /// <summary> True when the object has been disposed. No further operations are allowed. </summary>
        bool IsDisposed { get; }

        /// <summary> Total number of items in the file </summary>
        long GetItemCount();

        /// <summary>
        /// Read up to <paramref name="count"/> items beging at <paramref name="firstItemIdx"/>, and return an <see cref="Array"/> object. 
        /// </summary>
        /// <param name="firstItemIdx">Index of the item to start from.</param>
        /// <param name="count">The maximum number of items to read.</param>
        Array GenericReadData(long firstItemIdx, int count);
    }

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