using System;
using System.IO;

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

        #endregion

        private static readonly Version CurrentVersion = new Version(1, 0);

        #region IBinUniformTimeseriesFile Members

        /// <summary>
        /// Returns adjusted index that would correspond to the valid timestamp in this file.
        /// The FirstFileTS has no affect, and index will always be valid.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public DateTime RoundDownToTimeSpanStart(DateTime index)
        {
            return index - TimeSpan.FromTicks(index.TimeOfDay.Ticks%ItemTimeSpan.Ticks);
        }

        #endregion

        protected override void ReadCustomHeader(BinaryReader stream, Version version)
        {
            if (version == CurrentVersion)
            {
                ItemTimeSpan = TimeSpan.FromTicks(stream.ReadInt64());
                FirstFileTS = ValidateIndex(DateTime.FromBinary(stream.ReadInt64()));
            }
            else
                Utilities.ThrowUnknownVersion(version, GetType());
        }

        protected override Version WriteCustomHeader(BinaryWriter stream)
        {
            stream.Write(ItemTimeSpan.Ticks);
            stream.Write(FirstFileTS.ToBinary());

            return CurrentVersion;
        }

        protected long IndexToLong(DateTime timestamp)
        {
            return (ValidateIndex(timestamp).Ticks - FirstFileTS.Ticks)/ItemTimeSpan.Ticks;
        }

        public override string ToString()
        {
            return string.Format("{0}, firstTS={1}, slice={2}", base.ToString(), FirstFileTS, ItemTimeSpan);
        }

        private DateTime ValidateIndex(DateTime timestamp)
        {
            if (timestamp.Kind != DateTimeKind.Utc)
                throw new ArgumentOutOfRangeException("timestamp", timestamp, "DateTime must be in UTC form");
            if (timestamp.TimeOfDay.Ticks % ItemTimeSpan.Ticks != 0)
                throw new IOException(
                    String.Format("The timestamp {0} must be aligned by the time slice {1}", timestamp, ItemTimeSpan));
            return timestamp;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, but not including <paramref name="toExclusive"/>.
        /// </summary>
        public void ReadData(DateTime fromInclusive, DateTime toExclusive, T[] buffer, int offset)
        {
            if (buffer == null) throw new ArgumentNullException("buffer");
            ReadData(fromInclusive, toExclusive, ref buffer, offset);
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, but not including <paramref name="toExclusive"/>.
        /// </summary>
        public T[] ReadData(DateTime fromInclusive, DateTime toExclusive)
        {
            T[] newData = null;
            ReadData(fromInclusive, toExclusive, ref newData, 0);
            return newData;
        }

        private void ReadData(DateTime fromInclusive, DateTime toExclusive, ref T[] buffer, int offset)
        {
            if (fromInclusive.CompareTo(toExclusive) > 0)
                throw new ArgumentOutOfRangeException("fromInclusive", "'from' must be <= 'to'");

            long firstIndexIncl = IndexToLong(fromInclusive);
            long lastIndexExcl = IndexToLong(toExclusive);

            // Switiching to int array refs. No 64bit array support yet
            int count = Utilities.ToInt32Checked(lastIndexExcl - firstIndexIncl);
            if (buffer == null)
            {
                buffer = new T[count];
                offset = 0;
            }

            Read(firstIndexIncl, buffer, offset, count);
        }

        /// <summary>
        /// Read <paramref name="count"/> items starting at <paramref name="fromInclusive"/>. Read backwards if count is negative.
        /// </summary>
        /// <param name="fromInclusive">Index of the item to start from.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        /// <param name="offset">The offset in buffer at which to begin copying items to the file.</param>
        /// <param name="count">The number of items to be read to the array.</param>
        public void ReadData(DateTime fromInclusive, T[] buffer, int offset, int count)
        {
            Read(IndexToLong(fromInclusive), buffer, offset, count);
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
            Read(IndexToLong(fromInclusive), buffer, 0, count);
            return buffer;
        }

        /// <summary>
        /// Write an array of items to the file.
        /// </summary>
        /// <param name="firstItemIndex">The index of the first value in the <paramref name="buffer"/> array.</param>
        /// <param name="buffer">Array of values to be written into a file. No action is performed when the array is empty.</param>
        public void WriteData(DateTime firstItemIndex, T[] buffer)
        {
            if (buffer == null) throw new ArgumentNullException("buffer");
            WriteData(firstItemIndex, buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Write an array of items to the file.
        /// </summary>
        /// <param name="firstItemIndex">The index of the first value in the <paramref name="buffer"/> array.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        /// <param name="offset">The offset in buffer at which to begin copying items to the file.</param>
        /// <param name="count">The number of items to be written from array. No action is performed when the count is 0.</param>
        public void WriteData(DateTime firstItemIndex, T[] buffer, int offset, int count)
        {
            Utilities.ValidateArrayParams(buffer, offset, count);
            if (!CanWrite) throw new InvalidOperationException("The file was opened as readonly");

            if (firstItemIndex < FirstFileTS)
                throw new ArgumentOutOfRangeException("firstItemIndex", firstItemIndex, "Must be >= FirstFileTS ("+FirstFileTS +")");
            if (firstItemIndex > FirstUnavailableTimestamp)
                throw new ArgumentOutOfRangeException("firstItemIndex", firstItemIndex, "Must be <= FirstUnavailableTimestamp (" + FirstUnavailableTimestamp + ")");
            
            long itemLong = IndexToLong(firstItemIndex);

            Write(itemLong, buffer, offset, count);
        }
    }
}