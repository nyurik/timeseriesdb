using System;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public interface IBinUniformTimeseriesFile : IBinaryFile
    {
        /// <summary>
        /// The timestamp of the first item in the file.
        /// May only be set before the <see cref="BinaryFile.InitializeNewFile"/> is called.
        /// </summary>
        UtcDateTime FirstFileTS { get; }

        /// <summary>
        /// Represents the timestamp of the first value beyond the end of the existing data.
        /// (<see cref="BinaryFile{T}.Count"/> as a timestamp)
        /// </summary>
        UtcDateTime FirstUnavailableTS { get; }

        /// <summary>
        /// Span of time each item represents.
        /// May only be set before the <see cref="BinaryFile.InitializeNewFile"/> is called.
        /// </summary>
        TimeSpan ItemTimeSpan { get; }

        /// <summary>
        /// Adjusts the date range to the ones that exist in the file
        /// </summary>
        /// <param name="fromInclusive">Will get rounded up to the next closest existing item.
        /// If no data beyond this point, date will only be adjusted to the next valid timestamp, and 0 is returned</param>
        /// <param name="toExclusive">Will get rounded up to the next closest existing item or first unavailable.
        /// If no data beyond this point, date will only be adjusted to the next valid timestamp, and 0 is returned</param>
        /// <returns>Number of items that exist between new range</returns>
        int AdjustRangeToExistingData(ref UtcDateTime fromInclusive, ref UtcDateTime toExclusive);

        /// <summary>
        /// Generic version of <see cref="BinUniformTimeseriesFile{T}.ReadData(UtcDateTime,UtcDateTime)"/>.
        /// Read data starting at <paramref name="fromInclusive"/>, up to, but not including <paramref name="toExclusive"/>,
        /// and return an <see cref="Array"/> object. 
        /// </summary>
        Array GenericReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive);

        /// <summary>
        /// Generic version of <see cref="BinUniformTimeseriesFile{T}.ReadData(UtcDateTime,int)"/>.
        /// Read <paramref name="count"/> items starting at <paramref name="fromInclusive"/>.
        /// and return an <see cref="Array"/> object. 
        /// </summary>
        /// <param name="fromInclusive">Index of the item to start from.</param>
        /// <param name="count">The number of items to be read.</param>
        Array GenericReadData(UtcDateTime fromInclusive, int count);

        /// <summary>
        /// Generic version of <see cref="BinUniformTimeseriesFile{T}.WriteData"/>.
        /// Write an array of items to the file.
        /// </summary>
        /// <param name="firstItemIndex">The index of the first value in the <paramref name="buffer"/> array.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        /// <param name="offset">The zero-based index of the first element in the range.</param>
        /// <param name="count">The number of elements in the range.</param>
        void GenericWriteData(UtcDateTime firstItemIndex, Array buffer, int offset, int count);

        /// <summary>
        /// Truncate existing file to the new date.
        /// </summary>
        /// <param name="newFirstUnavailableTimestamp">Must be less then or equal to <see cref="FirstUnavailableTS"/></param>
        void TruncateFile(UtcDateTime newFirstUnavailableTimestamp);
    }
}