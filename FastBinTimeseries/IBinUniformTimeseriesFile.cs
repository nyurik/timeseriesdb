using System;

namespace NYurik.FastBinTimeseries
{
    public interface IBinUniformTimeseriesFile : IBinaryFile
    {
        /// <summary>The timestamp of the first item in the file</summary>
        DateTime FirstFileTS { get; }

        /// <summary>Represents the timestamp of the first value beyond the end of the existing data.
        /// (<see cref="BinaryFile{T}.Count"/> as a timestamp)</summary>
        DateTime FirstUnavailableTimestamp { get; }

        /// <summary>Span of time each item represents</summary>
        TimeSpan ItemTimeSpan { get; }

        /// <summary>
        /// Returns adjusted index that would correspond to the valid timestamp in this file.
        /// The FirstFileTS has no affect, and index will always be valid.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        DateTime RoundDownToTimeSpanStart(DateTime index);
    }
}