#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of FastBinTimeseries library
 * 
 *  FastBinTimeseries is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  FastBinTimeseries is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with FastBinTimeseries.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.IO;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Helper non-generic class aids in creating a new instance of <see cref="BinUniformTimeseriesFile{T}"/>.
    /// </summary>
    [Obsolete("Uniform data files are no longer supported")]
    public static class BinUniformTimeseriesFile
    {
        /// <summary>
        /// Uses reflection to create an instance of <see cref="BinUniformTimeseriesFile{T}"/>.
        /// </summary>
        public static IBinUniformTimeseriesFile GenericNew(Type itemType, string fileName, TimeSpan itemTimeSpan,
                                                           UtcDateTime firstTimestamp)
        {
            return (IBinUniformTimeseriesFile)
                   Activator.CreateInstance(
                       typeof (BinUniformTimeseriesFile<>).MakeGenericType(itemType),
                       fileName, itemTimeSpan, firstTimestamp);
        }
    }

    /// <summary>
    /// Object representing a binary-serialized timeseries file with each item with uniform distribution of items
    /// one <see cref="ItemTimeSpan"/> from each other.
    /// </summary>
    [Obsolete("Uniform data files are no longer supported")]
    public class BinUniformTimeseriesFile<T> : BinaryFile<T>, IBinaryFile<T>, IBinUniformTimeseriesFile
    {
        #region Constructors

        /// <summary>
        /// Allow Activator non-public instantiation
        /// </summary>
        protected BinUniformTimeseriesFile()
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        /// <param name="itemTimeSpan">Unit of time each value represents.
        ///   If less than a day, the day must be evenly divisible by this value</param>
        /// <param name="firstTimestamp"></param>
        public BinUniformTimeseriesFile(string fileName, TimeSpan itemTimeSpan, UtcDateTime firstTimestamp)
            : base(fileName)
        {
            ItemTimeSpan = itemTimeSpan;
            FirstTimestamp = firstTimestamp;
        }

        #endregion

        #region Fields

        private UtcDateTime _firstTimestamp;
        private TimeSpan _itemTimeSpan;

        /// <summary>
        /// The timestamp of the first item in the file.
        /// May only be set before the <see cref="BinaryFile.InitializeNewFile"/> is called.
        /// </summary>
        public UtcDateTime FirstTimestamp
        {
            get { return _firstTimestamp; }
            set
            {
                ThrowOnInitialized();
                _firstTimestamp = ValidateIndex(value);
            }
        }

        public UtcDateTime FirstUnavailableTimestamp
        {
            get { return FirstTimestamp + Multiply(ItemTimeSpan, Count); }
        }

        /// <summary>
        /// Span of time each item represents.
        /// May only be set before the <see cref="BinaryFile.InitializeNewFile"/> is called.
        /// </summary>
        public TimeSpan ItemTimeSpan
        {
            get { return _itemTimeSpan; }
            set
            {
                ThrowOnInitialized();
                if (value > TimeSpan.FromDays(1))
                    throw new BinaryFileException("Time slice {0} is > 1 day", value);
                if (TimeSpan.TicksPerDay%value.Ticks != 0)
                    throw new BinaryFileException(
                        "TimeSpan.TicksPerDay must be divisible by time slice {0} ", value);
                _itemTimeSpan = value;
            }
        }

        public int AdjustRangeToExistingData(ref UtcDateTime fromInclusive, ref UtcDateTime toExclusive)
        {
            if (fromInclusive < FirstTimestamp)
                fromInclusive = FirstTimestamp;
            else
                fromInclusive = new UtcDateTime(
                    FastBinFileUtils.RoundUpToMultiple(fromInclusive.Ticks, ItemTimeSpan.Ticks));

            if (toExclusive > FirstUnavailableTimestamp)
                toExclusive = FirstUnavailableTimestamp;
            else
                toExclusive = new UtcDateTime(
                    FastBinFileUtils.RoundUpToMultiple(toExclusive.Ticks, ItemTimeSpan.Ticks));

            if (fromInclusive >= FirstUnavailableTimestamp)
                fromInclusive = toExclusive = FirstUnavailableTimestamp;
            if (toExclusive <= FirstTimestamp)
                fromInclusive = toExclusive = FirstTimestamp;

            long len = IndexToLong(toExclusive) - IndexToLong(fromInclusive);
            if (len > int.MaxValue)
                return 0;
            return (int) len;
        }

        [Obsolete("Use streaming methods instead")]
        Array IStoredUniformTimeseries.GenericReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive)
        {
            return ReadData(fromInclusive, toExclusive);
        }

        [Obsolete("Use streaming methods instead")]
        Array IStoredUniformTimeseries.GenericReadData(UtcDateTime fromInclusive, int count)
        {
            long firstItemIdx = IndexToLong(fromInclusive);
            int bufCount = Math.Min(ToIntCountChecked(Count - firstItemIdx), count);
            var buffer = new ArraySegment<T>(new T[bufCount], 0, bufCount);

            PerformFileAccess(firstItemIdx, buffer, false);

            return buffer.Array;
        }

        [Obsolete("Use streaming methods instead")]
        void IBinUniformTimeseriesFile.GenericWriteData(UtcDateTime firstItemIndex, Array buffer, int offset, int count)
        {
            WriteData(firstItemIndex, new ArraySegment<T>((T[]) buffer, offset, count));
        }

        public void TruncateFile(UtcDateTime newFirstUnavailableTimestamp)
        {
            PerformTruncateFile(IndexToLong(newFirstUnavailableTimestamp));
        }

        #endregion

        // ReSharper disable StaticFieldInGenericType
        private static readonly Version Version10 = new Version(1, 0);
        private static readonly Version Version11 = new Version(1, 1);
        // ReSharper restore StaticFieldInGenericType

        #region IBinaryFile<T> Members

        [Obsolete("Use streaming methods instead")]
        public void ReadData(long firstItemIdx, ArraySegment<T> buffer)
        {
            PerformFileAccess(firstItemIdx, buffer, false);
        }

        #endregion

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, but not including <paramref name="toExclusive"/>.
        /// </summary>
        [Obsolete("Use streaming methods instead")]
        public T[] ReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive)
        {
            Tuple<long, int> rng = CalcNeededBuffer(fromInclusive, toExclusive);
            var buffer = new T[rng.Item2];

            PerformFileAccess(rng.Item1, new ArraySegment<T>(buffer), false);

            return buffer;
        }

        /// <summary>
        /// Write an array of items to the file.
        /// </summary>
        /// <param name="firstItemIndex">The index of the first value in the <paramref name="buffer"/> array.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        [Obsolete("Use streaming methods instead")]
        public void WriteData(UtcDateTime firstItemIndex, ArraySegment<T> buffer)
        {
            if (buffer.Array == null)
                throw new ArgumentException("buffer");

            if (!CanWrite)
                throw new InvalidOperationException("The file was opened as readonly");

            if (firstItemIndex < FirstTimestamp)
                throw new ArgumentOutOfRangeException(
                    "firstItemIndex", firstItemIndex,
                    "Must be >= FirstTimestamp (" + FirstTimestamp + ")");
            if (firstItemIndex > FirstUnavailableTimestamp)
                throw new ArgumentOutOfRangeException(
                    "firstItemIndex", firstItemIndex,
                    "Must be <= FirstUnavailableTimestamp (" +
                    FirstUnavailableTimestamp + ")");

            long itemLong = IndexToLong(firstItemIndex);

            PerformFileAccess(itemLong, buffer, true);
        }

        protected override Version Init(BinaryReader reader, Func<string, Type> typeResolver)
        {
            Version ver = reader.ReadVersion();
            if (ver != Version11 && ver != Version10)
                throw new IncompatibleVersionException(GetType(), ver);

            ItemTimeSpan = TimeSpan.FromTicks(reader.ReadInt64());

            // in 1.0, DateTime was serialized as binary instead of UtcDateTime.Ticks
            FirstTimestamp =
                ver == Version11
                    ? new UtcDateTime(reader.ReadInt64())
                    : new UtcDateTime(DateTime.FromBinary(reader.ReadInt64()));

            return ver;
        }

        protected override Version WriteCustomHeader(BinaryWriter writer)
        {
            writer.WriteVersion(Version11);
            writer.Write(ItemTimeSpan.Ticks);
            writer.Write(FirstTimestamp.Ticks);

            return Version11;
        }

        /// <summary>
        /// Returns the first index and the length of the data available in this file for the given range of dates
        /// </summary>
        protected Tuple<long, int> CalcNeededBuffer(UtcDateTime fromInclusive, UtcDateTime toExclusive)
        {
            if (fromInclusive.CompareTo(toExclusive) > 0)
                throw new ArgumentOutOfRangeException("fromInclusive", "'from' must be <= 'to'");

            long firstIndexIncl = IndexToLong(fromInclusive);
            return Tuple.Create(firstIndexIncl, ToIntCountChecked(IndexToLong(toExclusive) - firstIndexIncl));
        }

        public override string ToString()
        {
            return string.Format("{0}, firstTS={1}, slice={2}", base.ToString(), FirstTimestamp, ItemTimeSpan);
        }

        private UtcDateTime ValidateIndex(UtcDateTime timestamp)
        {
            if (timestamp.Ticks%ItemTimeSpan.Ticks != 0)
                throw new ArgumentException(
                    String.Format(
                        "The timestamp {0} must be aligned by the time slice {1}", timestamp,
                        ItemTimeSpan));
            return timestamp;
        }

        private long IndexToLong(UtcDateTime timestamp)
        {
            return (ValidateIndex(timestamp).Ticks - FirstTimestamp.Ticks)/ItemTimeSpan.Ticks;
        }

        private int ToIntCountChecked(long value)
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

        private TimeSpan Multiply(TimeSpan timeSpan, long count)
        {
            return TimeSpan.FromTicks(timeSpan.Ticks*count);
        }
    }
}