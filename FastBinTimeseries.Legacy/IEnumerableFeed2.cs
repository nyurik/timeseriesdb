using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
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
}