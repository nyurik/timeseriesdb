using System;
using System.Collections.Generic;
using System.IO;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Object representing a binary-serialized timeseries file.
    /// </summary>
    public class BinUniformTimeseriesFile<T> : BinaryFile<T>, IBinUniformTimeseriesFile
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
        public BinUniformTimeseriesFile(string fileName, TimeSpan itemTimeSpan, DateTime firstTimestamp)
            : base(fileName)
        {
            ItemTimeSpan = itemTimeSpan;
            FirstFileTS = ValidateIndex(firstTimestamp);
        }

        #endregion

        #region Fields

        /// <summary>Span of time each item represents</summary>
        private TimeSpan m_itemTimeSpan;

        /// <summary>The timestamp of the first item in the file</summary>
        public DateTime FirstFileTS { get; private set; }

        /// <summary>Represents the timestamp of the first value beyond the end of the existing data.
        /// (<see cref="BinaryFile{T}.Count"/> as a timestamp)</summary>
        public DateTime FirstUnavailableTimestamp
        {
            get { return new DateTime(FirstFileTS.Ticks + ItemTimeSpan.Ticks*Count, FirstFileTS.Kind); }
        }

        /// <summary>Span of time each item represents</summary>
        public TimeSpan ItemTimeSpan
        {
            get { return m_itemTimeSpan; }

            private set
            {
                if (value > TimeSpan.FromDays(1))
                    throw new IOException(String.Format("Time slice {0} is > 1 day", value));
                if (TimeSpan.TicksPerDay%value.Ticks != 0)
                    throw new IOException(String.Format("TimeSpan.TicksPerDay must be divisible by time slice {0} ",
                                                        value));
                m_itemTimeSpan = value;
            }
        }

        public int AdjustRangeToExistingData(ref DateTime fromInclusive, ref DateTime toExclusive)
        {
            if (fromInclusive.EnsureUtc() < FirstFileTS)
                fromInclusive = FirstFileTS;
            else
                fromInclusive = new DateTime(
                    FastBinFileUtils.RoundUpToMultiple(fromInclusive.Ticks, ItemTimeSpan.Ticks),
                    DateTimeKind.Utc);

            if (toExclusive.EnsureUtc() > FirstUnavailableTimestamp)
                toExclusive = FirstUnavailableTimestamp;
            else
                toExclusive = new DateTime(
                    FastBinFileUtils.RoundUpToMultiple(toExclusive.Ticks, ItemTimeSpan.Ticks),
                    DateTimeKind.Utc);

            if (fromInclusive >= FirstUnavailableTimestamp || toExclusive <= FirstFileTS)
                return 0;

            long len = IndexToLong(toExclusive) - IndexToLong(fromInclusive);
            if (len > int.MaxValue)
                return 0;
            return (int) len;
        }

        #endregion

        private static readonly Version CurrentVersion = new Version(1, 0);

        protected override void ReadCustomHeader(BinaryReader stream, Version version, IDictionary<string, Type> typeMap)
        {
            if (version == CurrentVersion)
            {
                ItemTimeSpan = TimeSpan.FromTicks(stream.ReadInt64());
                FirstFileTS = ValidateIndex(DateTime.FromBinary(stream.ReadInt64())).EnsureUtc();
            }
            else
                FastBinFileUtils.ThrowUnknownVersion(version, GetType());
        }

        protected override Version WriteCustomHeader(BinaryWriter stream)
        {
            stream.Write(ItemTimeSpan.Ticks);
            stream.Write(FirstFileTS.ToBinary());

            return CurrentVersion;
        }

        public override string ToString()
        {
            return string.Format("{0}, firstTS={1}, slice={2}", base.ToString(), FirstFileTS, ItemTimeSpan);
        }

        private DateTime ValidateIndex(DateTime timestamp)
        {
            if (timestamp.EnsureUtc().TimeOfDay.Ticks%ItemTimeSpan.Ticks != 0)
                throw new IOException(
                    String.Format("The timestamp {0} must be aligned by the time slice {1}", timestamp, ItemTimeSpan));
            return timestamp;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, but not including <paramref name="toExclusive"/>.
        /// </summary>
        /// <returns>The total number of items read.</returns>
        public int ReadData(DateTime fromInclusive, DateTime toExclusive, ArraySegment<T> buffer)
        {
            if (buffer.Array == null) throw new ArgumentNullException("buffer");
            var rng = CalcNeededBuffer(fromInclusive, toExclusive);
            PerformRead(rng.First, new ArraySegment<T>(buffer.Array, buffer.Offset, Math.Min(buffer.Count, rng.Second)));
            return rng.Second;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, but not including <paramref name="toExclusive"/>.
        /// </summary>
        public T[] ReadData(DateTime fromInclusive, DateTime toExclusive)
        {
            var rng = CalcNeededBuffer(fromInclusive, toExclusive);
            var buffer = new T[rng.Second];

            PerformRead(rng.First, new ArraySegment<T>(buffer));

            return buffer;
        }

        /// <summary>
        /// Read items starting at <paramref name="fromInclusive"/>. Read backwards if count is negative.
        /// </summary>
        /// <param name="fromInclusive">Index of the item to start from.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        public void ReadData(DateTime fromInclusive, ArraySegment<T> buffer)
        {
            PerformRead(IndexToLong(fromInclusive), buffer);
        }

        /// <summary>
        /// Read <paramref name="count"/> items starting at <paramref name="fromInclusive"/>. Read backwards if count is negative.
        /// </summary>
        /// <param name="fromInclusive">Index of the item to start from.</param>
        /// <param name="count">The number of items to be read.</param>
        /// <returns>New array of elements</returns>
        public T[] ReadData(DateTime fromInclusive, int count)
        {
            var buffer = new T[count];
            PerformRead(IndexToLong(fromInclusive), new ArraySegment<T>(buffer));
            return buffer;
        }

        /// <summary>
        /// Write an array of items to the file.
        /// </summary>
        /// <param name="firstItemIndex">The index of the first value in the <paramref name="buffer"/> array.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        public void WriteData(DateTime firstItemIndex, ArraySegment<T> buffer)
        {
            if(buffer.Array == null) throw new ArgumentException("buffer");

            if (!CanWrite) throw new InvalidOperationException("The file was opened as readonly");

            if (firstItemIndex < FirstFileTS)
                throw new ArgumentOutOfRangeException("firstItemIndex", firstItemIndex,
                                                      "Must be >= FirstFileTS (" + FirstFileTS + ")");
            if (firstItemIndex > FirstUnavailableTimestamp)
                throw new ArgumentOutOfRangeException("firstItemIndex", firstItemIndex,
                                                      "Must be <= FirstUnavailableTimestamp (" +
                                                      FirstUnavailableTimestamp + ")");

            long itemLong = IndexToLong(firstItemIndex);

            PerformWrite(itemLong, buffer);
        }

        /// <summary>
        /// Returns the first index and the length of the data available in this file for the given range of dates
        /// </summary>
        protected Tuple<long, int> CalcNeededBuffer(DateTime fromInclusive, DateTime toExclusive)
        {
            if (fromInclusive.CompareTo(toExclusive) > 0)
                throw new ArgumentOutOfRangeException("fromInclusive", "'from' must be <= 'to'");

            long firstIndexIncl = IndexToLong(fromInclusive);
            return Tuple.Create(firstIndexIncl, (IndexToLong(toExclusive) - firstIndexIncl).ToInt32Checked());
        }

        protected long IndexToLong(DateTime timestamp)
        {
            return (ValidateIndex(timestamp).Ticks - FirstFileTS.Ticks)/ItemTimeSpan.Ticks;
        }
    }
}