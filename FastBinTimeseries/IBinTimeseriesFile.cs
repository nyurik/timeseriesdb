using System.Reflection;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public interface IBinTimeseriesFile : IBinaryFile
    {
        /// <summary>
        /// Specifies which field in the item type to use as a timestamp.
        /// May only be set before the <see cref="BinaryFile.InitializeNewFile"/> is called.
        /// </summary>
        FieldInfo TimestampFieldInfo { get; set; }

        /// <summary> Returns null for empty file, or the timestamp of the first element </summary>
        UtcDateTime? FirstFileTS { get; }

        /// <summary> Returns null for empty file, or the timestamp of the last element </summary>
        UtcDateTime? LastFileTS { get; }

        /// <summary>
        /// Force the uniqueness of the timestamps in the file.
        /// May only be set before the <see cref="BinaryFile.InitializeNewFile"/> is called.
        /// </summary>
        bool UniqueTimestamps { get; set; }

        /// <summary>
        /// Performs binary search for the given timestamp.
        /// An exception will be thrown if the file allows non-unique timestamps.
        /// </summary>
        /// <param name="timestamp">timestamp to find</param>
        /// <returns>Index of the unique item with the given timestamp if found.
        /// If value is not found and value is less than one or more elements in the file, 
        /// a negative number which is the bitwise complement of the index of the first element that is larger than value. 
        /// If value is not found and value is greater than any of the elements in array, 
        /// a negative number which is the bitwise complement of (the index of the last element plus 1).</returns>
        /// <summary>
        long BinarySearch(UtcDateTime timestamp);

        /// <summary>
        /// Performs binary search for the given timestamp.
        /// In case of files where timestamps are not unique, returns the position of the first/last of the same values.
        /// </summary>
        /// <param name="timestamp">timestamp to find</param>
        /// <param name="findFirst">If true, finds the first in a series of duplicates, otherwise returns last</param>
        /// <returns>Index of the unique, first, or last item with the given timestamp if found.
        /// If value is not found and value is less than one or more elements in the file, 
        /// a negative number which is the bitwise complement of the index of the first element that is larger than value. 
        /// If value is not found and value is greater than any of the elements in array, 
        /// a negative number which is the bitwise complement of (the index of the last element plus 1).</returns>
        long BinarySearch(UtcDateTime timestamp, bool findFirst);

        /// <summary> Truncate existing file to the new date. </summary>
        /// <param name="lastTimestampToPreserve">After truncation, the file will have up to and including this timestamp</param>
        void TruncateFile(UtcDateTime lastTimestampToPreserve);

        /// <summary> Truncate existing file to the new item count. </summary>
        void TruncateFile(long newCount);
    }
}