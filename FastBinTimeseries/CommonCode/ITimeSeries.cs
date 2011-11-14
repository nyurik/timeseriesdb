using System;

namespace NYurik.FastBinTimeseries.CommonCode
{
    public interface ISeries
    {
        int Count { get; }
        Type GetElementType();
        object GetValueSlow(int index);
    }

    public interface ISeries<T> : ISeries
    {
        T this[int index] { get; }
    }

    public interface ITimeSeries : ISeries
    {
        /// <summary>
        /// Performs search for specified timestamp. In the worst case it'll be a binary search.
        /// </summary>
        /// <param name="timestamp">The timestamp.</param>
        /// <returns>The zero-based index of item, if item is found; otherwise, a negative number that is the bitwise complement of the index of the next element that is larger than item or, if there is no larger element, the bitwise complement of <see cref="ISeries.Count"/>.</returns>
        int BinarySearch(UtcDateTime timestamp);

        UtcDateTime GetTimestamp(int index);
    }

    public interface ITimeSeries<T> : ITimeSeries, ISeries<T>
    {
    }
}