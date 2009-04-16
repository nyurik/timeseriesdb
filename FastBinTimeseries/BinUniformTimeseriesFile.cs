using System;
using System.Collections.Generic;
using System.IO;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Helper non-generic class aids in creating a new instance of <see cref="BinUniformTimeseriesFile{T}"/>.
    /// </summary>
    public class BinUniformTimeseriesFile
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
        public BinUniformTimeseriesFile(string fileName, TimeSpan itemTimeSpan, UtcDateTime firstTimestamp)
            : base(fileName)
        {
            ItemTimeSpan = itemTimeSpan;
            FirstFileTS = firstTimestamp;
        }

        #endregion

        #region Fields

        private UtcDateTime _firstFileTs;
        private TimeSpan _itemTimeSpan;

        public UtcDateTime FirstFileTS
        {
            get { return _firstFileTs; }
            set
            {
                ThrowOnInitialized();
                _firstFileTs = ValidateIndex(value);
            }
        }

        public UtcDateTime FirstUnavailableTS
        {
            get { return FirstFileTS + ItemTimeSpan.Multiple(Count); }
        }

        public TimeSpan ItemTimeSpan
        {
            get { return _itemTimeSpan; }
            set
            {
                ThrowOnInitialized();
                if (value > TimeSpan.FromDays(1))
                    throw new IOException(String.Format("Time slice {0} is > 1 day", value));
                if (TimeSpan.TicksPerDay%value.Ticks != 0)
                    throw new IOException(String.Format("TimeSpan.TicksPerDay must be divisible by time slice {0} ",
                                                        value));
                _itemTimeSpan = value;
            }
        }

        public int AdjustRangeToExistingData(ref UtcDateTime fromInclusive, ref UtcDateTime toExclusive)
        {
            if (fromInclusive < FirstFileTS)
                fromInclusive = FirstFileTS;
            else
                fromInclusive = new UtcDateTime(
                    FastBinFileUtils.RoundUpToMultiple(fromInclusive.Ticks, ItemTimeSpan.Ticks));

            if (toExclusive > FirstUnavailableTS)
                toExclusive = FirstUnavailableTS;
            else
                toExclusive = new UtcDateTime(
                    FastBinFileUtils.RoundUpToMultiple(toExclusive.Ticks, ItemTimeSpan.Ticks));

            if (fromInclusive >= FirstUnavailableTS || toExclusive <= FirstFileTS)
                return 0;

            long len = IndexToLong(toExclusive) - IndexToLong(fromInclusive);
            if (len > int.MaxValue)
                return 0;
            return (int) len;
        }

        Array IBinUniformTimeseriesFile.GenericReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive)
        {
            return ReadData(fromInclusive, toExclusive);
        }

        Array IBinUniformTimeseriesFile.GenericReadData(UtcDateTime fromInclusive, int count)
        {
            return ReadData(fromInclusive, count);
        }

        void IBinUniformTimeseriesFile.GenericWriteData(UtcDateTime firstItemIndex, Array buffer, int offset, int count)
        {
            WriteData(firstItemIndex, new ArraySegment<T>((T[]) buffer, offset, count));
        }

        public void TrimData(UtcDateTime newFirstUnavailableTimestamp)
        {
            PerformFileTrim(IndexToLong(newFirstUnavailableTimestamp));
        }

        #endregion

        private static readonly Version CurrentVersion = new Version(1, 1);
        private static readonly Version Ver10 = new Version(1, 0);

        protected override void ReadCustomHeader(BinaryReader stream, Version version, IDictionary<string, Type> typeMap)
        {
            if (version == CurrentVersion || version == Ver10)
            {
                ItemTimeSpan = TimeSpan.FromTicks(stream.ReadInt64());

                // in 1.0, DateTime was serialized as binary instead of UtcDateTime.Ticks
                FirstFileTS =
                    version == CurrentVersion
                        ? new UtcDateTime(stream.ReadInt64())
                        : new UtcDateTime(DateTime.FromBinary(stream.ReadInt64()));
            }
            else
                FastBinFileUtils.ThrowUnknownVersion(version, GetType());
        }

        protected override Version WriteCustomHeader(BinaryWriter stream)
        {
            stream.Write(ItemTimeSpan.Ticks);
            stream.Write(FirstFileTS.Ticks);

            return CurrentVersion;
        }

        public override string ToString()
        {
            return string.Format("{0}, firstTS={1}, slice={2}", base.ToString(), FirstFileTS, ItemTimeSpan);
        }

        private UtcDateTime ValidateIndex(UtcDateTime timestamp)
        {
            if (timestamp.Ticks%ItemTimeSpan.Ticks != 0)
                throw new IOException(
                    String.Format("The timestamp {0} must be aligned by the time slice {1}", timestamp, ItemTimeSpan));
            return timestamp;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, but not including <paramref name="toExclusive"/>.
        /// </summary>
        /// <returns>The total number of items read.</returns>
        public int ReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive, ArraySegment<T> buffer)
        {
            if (buffer.Array == null) throw new ArgumentNullException("buffer");
            Tuple<long, int> rng = CalcNeededBuffer(fromInclusive, toExclusive);
            PerformRead(rng.First, new ArraySegment<T>(buffer.Array, buffer.Offset, Math.Min(buffer.Count, rng.Second)));
            return rng.Second;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, but not including <paramref name="toExclusive"/>.
        /// </summary>
        public T[] ReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive)
        {
            Tuple<long, int> rng = CalcNeededBuffer(fromInclusive, toExclusive);
            var buffer = new T[rng.Second];

            PerformRead(rng.First, new ArraySegment<T>(buffer));

            return buffer;
        }

        /// <summary>
        /// Read items starting at <paramref name="fromInclusive"/>.
        /// </summary>
        /// <param name="fromInclusive">Index of the item to start from.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        public void ReadData(UtcDateTime fromInclusive, ArraySegment<T> buffer)
        {
            PerformRead(IndexToLong(fromInclusive), buffer);
        }

        /// <summary>
        /// Read <paramref name="count"/> items starting at <paramref name="fromInclusive"/>.
        /// </summary>
        /// <param name="fromInclusive">Index of the item to start from.</param>
        /// <param name="count">The number of items to be read.</param>
        /// <returns>New array of elements</returns>
        public T[] ReadData(UtcDateTime fromInclusive, int count)
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
        public void WriteData(UtcDateTime firstItemIndex, ArraySegment<T> buffer)
        {
            if (buffer.Array == null) throw new ArgumentException("buffer");

            if (!CanWrite) throw new InvalidOperationException("The file was opened as readonly");

            if (firstItemIndex < FirstFileTS)
                throw new ArgumentOutOfRangeException("firstItemIndex", firstItemIndex,
                                                      "Must be >= FirstFileTS (" + FirstFileTS + ")");
            if (firstItemIndex > FirstUnavailableTS)
                throw new ArgumentOutOfRangeException("firstItemIndex", firstItemIndex,
                                                      "Must be <= FirstUnavailableTS (" +
                                                      FirstUnavailableTS + ")");

            long itemLong = IndexToLong(firstItemIndex);

            PerformWrite(itemLong, buffer);
        }

        /// <summary>
        /// Returns the first index and the length of the data available in this file for the given range of dates
        /// </summary>
        protected Tuple<long, int> CalcNeededBuffer(UtcDateTime fromInclusive, UtcDateTime toExclusive)
        {
            if (fromInclusive.CompareTo(toExclusive) > 0)
                throw new ArgumentOutOfRangeException("fromInclusive", "'from' must be <= 'to'");

            long firstIndexIncl = IndexToLong(fromInclusive);
            return Tuple.Create(firstIndexIncl, (IndexToLong(toExclusive) - firstIndexIncl).ToInt32Checked());
        }

        protected long IndexToLong(UtcDateTime timestamp)
        {
            return (ValidateIndex(timestamp).Ticks - FirstFileTS.Ticks)/ItemTimeSpan.Ticks;
        }
    }
}