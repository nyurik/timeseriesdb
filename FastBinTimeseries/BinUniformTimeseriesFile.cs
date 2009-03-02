using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Object representing a binary-serialized timeseries file.
    /// </summary>
    public class BinUniformTimeseriesFile<T> : BinaryFile<T>
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
        public BinUniformTimeseriesFile(string fileName, TimeSpan itemTimeSpan)
            : this(fileName, itemTimeSpan, null)
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        /// <param name="itemTimeSpan">Unit of time each value represents.
        ///   If less than a day, the day must be evenly divisible by this value</param>
        /// <param name="customSerializer"></param>
        public BinUniformTimeseriesFile(string fileName, TimeSpan itemTimeSpan, IBinSerializer<T> customSerializer)
            : base(fileName, customSerializer)
        {
            ItemTimeSpan = itemTimeSpan;
            WriteHeader(); // Initialize header at the end of constructor
        }

        #endregion

        #region Fields

        private DateTime _firstFileTs;

        /// <summary>Span of time each item represents</summary>
        private TimeSpan _itemTimeSpan;

        /// <summary>The timestamp of the first item in the file</summary>
        public DateTime FirstFileTS
        {
            get { return _firstFileTs; }
            set
            {
                if (Count != 0)
                    throw new InvalidOperationException("Unable to set first file timestamp on a non-empty file");
                ValidateIndex(FirstFileTS);
                _firstFileTs = value;
            }
        }

        /// <summary>Represents the timestamp of the first value beyond the end of the existing data.
        /// (<see cref="BinaryFile{T}.Count"/> as a timestamp)</summary>
        public DateTime FirstUnavailableTimestamp
        {
            get { return new DateTime(FirstFileTS.Ticks + ItemTimeSpan.Ticks*Count); }
        }

        /// <summary>Span of time each item represents</summary>
        public TimeSpan ItemTimeSpan
        {
            get { return _itemTimeSpan; }

            private set
            {
                if (value > TimeSpan.FromDays(1))
                    throw new IOException(String.Format("Time slice {0} is > 1 day", value));
                if (TimeSpan.TicksPerDay%value.Ticks != 0)
                    throw new IOException(String.Format("TimeSpan.TicksPerDay must be divisible by time slice {0} ",
                                                        value));
                _itemTimeSpan = value;
            }
        }

        #endregion

        private static readonly Version CurrentVersion = new Version(1, 0);

        protected override void ReadCustomHeader(BinaryReader stream, Version version)
        {
            if (version == CurrentVersion)
            {
                ItemTimeSpan = TimeSpan.FromTicks(stream.ReadInt64());
                FirstFileTS = DateTime.FromBinary(stream.ReadInt64());
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
            ValidateIndex(timestamp);
            return (timestamp.Ticks - FirstFileTS.Ticks)/ItemTimeSpan.Ticks;
        }

        public override string ToString()
        {
            return string.Format("{0}, firstTS={1}, slice={2}", base.ToString(), FirstFileTS, ItemTimeSpan);
        }

        private void ValidateIndex(DateTime timestamp)
        {
            if (timestamp.TimeOfDay.Ticks%ItemTimeSpan.Ticks != 0)
                throw new IOException(
                    String.Format("The timestamp {0} must be aligned by the time slice {1}", timestamp, ItemTimeSpan));
        }

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
            ThrowOnDisposed();
            if (fromInclusive.CompareTo(toExclusive) > 0)
                throw new ArgumentOutOfRangeException("fromInclusive", "'from' must be <= 'to'");

            var firstIndexIncl = IndexToLong(fromInclusive);
            var lastIndexExcl = IndexToLong(toExclusive);

            var itemsCountLng = lastIndexExcl - firstIndexIncl;
            if (itemsCountLng > int.MaxValue)
                throw new ArgumentException(
                    String.Format(
                        "Attempted to get {0} items at once, which is over the maximum of {1}.",
                        itemsCountLng, Int32.MaxValue));

            // Switiching to int array refs. No 64bit array support yet
            var count = (int) itemsCountLng;
            if (buffer == null)
                buffer = new T[count];

            PerformFileAccess(firstIndexIncl, buffer, offset, count, false);
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
            PerformFileAccess(IndexToLong(fromInclusive), buffer, offset, count, false);
        }

        /// <summary>
        /// Read <paramref name="count"/> items starting at <paramref name="fromInclusive"/>. Read backwards if count is negative.
        /// </summary>
        /// <param name="fromInclusive">Index of the item to start from.</param>
        /// <param name="count">The number of items to be read.</param>
        /// <returns>New array of elements</returns>
        public T[] ReadData(DateTime fromInclusive, int count)
        {
            ThrowOnDisposed();

            var buffer = new T[count];
            PerformFileAccess(IndexToLong(fromInclusive), buffer, 0, count, false);
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
            ThrowOnDisposed();
            Utilities.ValidateArrayParams(buffer, offset, count);
            if (!CanWrite) throw new InvalidOperationException("The file was opened as readonly");

            ValidateIndex(firstItemIndex);
            if (!IsEmpty && firstItemIndex < FirstFileTS)
                throw new ArgumentOutOfRangeException("firstItemIndex",
                                                      "Must be >= FirstFileTS for non-empty data files");
            if (IsEmpty)
                FirstFileTS = firstItemIndex;

            var firstItemLong = IndexToLong(firstItemIndex);
            if (firstItemLong > Count + 1)
                throw new ArgumentException(
                    string.Format(
                        "Cannot add new data starting at {0} to a file with existing data from {1} to {2}, as it would leave an empty gap.",
                        firstItemIndex, FirstFileTS, FirstUnavailableTimestamp));

            var itemLong = IndexToLong(firstItemIndex);
            if (itemLong < 0 || itemLong > Count)
                throw new InvalidOperationException(
                    string.Format("Calculated file index of {0} must be between 0 and {1}(Count)", itemLong, Count));

            if (buffer.Length == 0)
                return; // validate parameters but don't change anything

            PerformFileAccess(itemLong, buffer, offset, count, true);
        }
    }
}