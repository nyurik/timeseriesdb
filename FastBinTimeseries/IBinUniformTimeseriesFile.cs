using System;

namespace NYurik.FastBinTimeseries
{
    public interface IBinUniformTimeseriesFile : IBinaryFile
    {
        /// <summary>The timestamp of the first item in the file</summary>
        DateTime FirstFileTS { get; }

        /// <summary>Represents the timestamp of the first value beyond the end of the existing data.
        /// (<see cref="BinaryFile.Count"/> as a timestamp)</summary>
        DateTime FirstUnavailableTimestamp { get; }

        /// <summary>Span of time each item represents</summary>
        TimeSpan ItemTimeSpan { get; }

        /// <summary>
        /// Adjusts the date range to the ones that exist in the file
        /// </summary>
        /// <param name="fromInclusive">Will get rounded up to the next closest existing item.
        /// If no data beyond this point, date will only be adjusted to the next valid timestamp, and 0 is returned</param>
        /// <param name="toExclusive">Will get rounded up to the next closest existing item or first unavailable.
        /// If no data beyond this point, date will only be adjusted to the next valid timestamp, and 0 is returned</param>
        /// <returns>Number of items that exist between new range</returns>
        int AdjustRangeToExistingData(ref DateTime fromInclusive, ref DateTime toExclusive);
    }
}