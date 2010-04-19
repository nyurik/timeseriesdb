using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Helper non-generic class aids in creating a new instance of <see cref="BinTimeseriesFile{T}"/>.
    /// </summary>
    public static class BinTimeseriesFile
    {
        /// <summary>
        /// Uses reflection to create an instance of <see cref="BinTimeseriesFile{T}"/>.
        /// </summary>
        public static IBinTimeseriesFile GenericNew(Type itemType, string fileName)
        {
            return (IBinTimeseriesFile)
                   Activator.CreateInstance(
                       typeof (BinTimeseriesFile<>).MakeGenericType(itemType),
                       fileName);
        }

        /// <summary>
        /// Uses reflection to create an instance of <see cref="BinTimeseriesFile{T}"/>.
        /// </summary>
        public static IBinTimeseriesFile GenericNew(Type itemType, string fileName, FieldInfo dateTimeFieldInfo)
        {
            return (IBinTimeseriesFile)
                   Activator.CreateInstance(
                       typeof (BinTimeseriesFile<>).MakeGenericType(itemType),
                       fileName, dateTimeFieldInfo);
        }
    }

    /// <summary>
    /// Object representing a binary-serialized timeseries file.
    /// </summary>
    public class BinTimeseriesFile<T> : BinaryFile<T>, IBinaryFile<T>, IBinTimeseriesFile, IStoredTimeSeries<T>,
                                        IHistFeedInt<T>
    {
        private static readonly DynamicSyncDictionary<Type, FieldInfo> TsFieldsCache =
            new DynamicSyncDictionary<Type, FieldInfo>(GetTimestampField);

        private static readonly Version Version10 = new Version(1, 0);
        private static readonly Version Version11 = new Version(1, 1);
        private UtcDateTime? _firstTimestamp;
        private UtcDateTime? _lastTimestamp;
        private FieldInfo _timestampFieldInfo;
        private bool _uniqueTimestamps;

        #region Constructors

        /// <summary>
        /// Allow Activator non-public instantiation
        /// </summary>
        protected BinTimeseriesFile()
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        public BinTimeseriesFile(string fileName)
            : this(fileName, TsFieldsCache.GetCreateValue(typeof (T)))
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        /// <param name="timestampFieldInfo">Field containing the UtcDateTime timestamp</param>
        public BinTimeseriesFile(string fileName, FieldInfo timestampFieldInfo)
            : base(fileName)
        {
            UniqueTimestamps = false;
            TimestampFieldInfo = timestampFieldInfo;
        }

        private static FieldInfo GetTimestampField(Type itemType)
        {
            FieldInfo[] fieldInfo = itemType.GetFields(TypeExtensions.AllInstanceMembers);
            if (fieldInfo.Length < 1)
                throw new InvalidOperationException("No fields found in type " + itemType.FullName);

            FieldInfo result = null;
            bool foundTsAttribute = false;
            bool foundMultiple = false;
            foreach (FieldInfo fi in fieldInfo)
                if (fi.FieldType == typeof (UtcDateTime))
                {
                    if (fi.ExtractSingleAttribute<TimestampAttribute>() != null)
                    {
                        if (foundTsAttribute)
                            throw new InvalidOperationException(
                                "More than one field has an TimestampAttribute attached in type " +
                                itemType.FullName);
                        foundTsAttribute = true;
                        result = fi;
                    }
                    else if (!foundTsAttribute)
                    {
                        if (result != null)
                            foundMultiple = true;
                        result = fi;
                    }
                }

            if (foundMultiple)
                throw new InvalidOperationException(
                    "Must explicitly specify the fieldInfo because there is more than one UtcDateTime field in type " +
                    itemType.FullName);
            if (result == null)
                throw new InvalidOperationException("No field of type UtcDateTime was found in type " +
                                                    itemType.FullName);

            return result;
        }

        protected override Version Init(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            var ver = reader.ReadVersion();
            if (ver != Version11 && ver != Version10)
                throw new IncompatibleVersionException(GetType(), ver);

            // UniqueTimestamps was not available in ver 1.0
            UniqueTimestamps = ver > Version10 && reader.ReadBoolean();

            string fieldName = reader.ReadString();

            FieldInfo fieldInfo = typeof(T).GetField(fieldName, TypeExtensions.AllInstanceMembers);
            if (fieldInfo == null)
                throw new InvalidOperationException(
                    string.Format("Timestamp field {0} was not found in type {1}", fieldName, typeof(T).FullName));
            TimestampFieldInfo = fieldInfo;

            return ver;
        }

        protected override Version WriteCustomHeader(BinaryWriter writer)
        {
            writer.WriteVersion(Version11);
            writer.Write(UniqueTimestamps);
            writer.Write(TimestampFieldInfo.Name);
            return Version11;
        }

        #endregion

        /// <summary>
        /// A delegate to a function that extracts timestamp of a given item
        /// </summary>
        public Func<T, UtcDateTime> TimestampAccessor { get; private set; }

        /// <summary>
        /// Enumerate all items one at a time using an internal buffer.
        /// </summary>
        /// <param name="from">The index of the first element to read. Inclusive if going forward, exclusive when going backwards</param>
        /// <param name="enumerateInReverse">Set to true if you want to enumerate backwards, from last to first</param>
        /// <param name="bufferSize">Size of the read buffer. If 0, the buffer will start small and grow with time</param>
        /// <returns></returns>
        public IEnumerable<ArraySegment<T>> StreamSegments(UtcDateTime from, bool enumerateInReverse, int bufferSize)
        {
            long index = FirstTimestampToIndex(from);
            if (enumerateInReverse)
                index--;

            return PerformStreaming(index, enumerateInReverse, bufferSize);
        }

        #region IBinaryFile<T> Members

        public void ReadData(long firstItemIdx, ArraySegment<T> buffer)
        {
            PerformFileAccess(firstItemIdx, buffer, false);
        }

        #endregion

        #region IBinTimeseriesFile Members

        public FieldInfo TimestampFieldInfo
        {
            get { return _timestampFieldInfo; }
            set
            {
                ThrowOnInitialized();
                if (value == null) throw new ArgumentNullException();

                TimestampAccessor = DynamicCodeFactory.Instance.CreateTsAccessor<T>(value);
                _timestampFieldInfo = value;
            }
        }

        public UtcDateTime? FirstFileTS
        {
            get
            {
                if (_firstTimestamp == null && Count > 0)
                {
                    var oneElementBuff = new T[1];
                    PerformFileAccess(0, new ArraySegment<T>(oneElementBuff), false);
                    _firstTimestamp = TimestampAccessor(oneElementBuff[0]);
                }
                return _firstTimestamp;
            }
        }

        public UtcDateTime? LastFileTS
        {
            get
            {
                if (_lastTimestamp == null && Count > 0)
                {
                    var oneElementBuff = new T[1];
                    PerformFileAccess(Count - 1, new ArraySegment<T>(oneElementBuff), false);
                    _lastTimestamp = TimestampAccessor(oneElementBuff[0]);
                }
                return _lastTimestamp;
            }
        }

        public bool UniqueTimestamps
        {
            get { return _uniqueTimestamps; }
            set
            {
                ThrowOnInitialized();
                _uniqueTimestamps = value;
            }
        }

        public long BinarySearch(UtcDateTime timestamp)
        {
            if (!UniqueTimestamps)
                throw new InvalidOperationException(
                    "This method call is only allowed for the unique timestamps file. Use BinarySearch(UtcDateTime, bool) instead.");
            return BinarySearch(timestamp, true);
        }

        public long BinarySearch(UtcDateTime timestamp, bool findFirst)
        {
            long start = 0L;
            long end = Count - 1;
            var buff = new T[2];
            var oneElementSegment = new ArraySegment<T>(buff, 0, 1);

            while (start <= end)
            {
                long mid = start + ((end - start) >> 1);

                if (end - start == 1 && !UniqueTimestamps && !findFirst)
                {
                    // for the special case where we are left with two elements,
                    // and searching for the last non-unique element,
                    // read both elements to see if the 2nd one matches our search
                    PerformFileAccess(mid, new ArraySegment<T>(buff), false);
                }
                else
                {
                    PerformFileAccess(mid, oneElementSegment, false);
                }


                int comp = TimestampAccessor(buff[0]).CompareTo(timestamp);
                if (comp == 0)
                {
                    if (UniqueTimestamps)
                        return mid;

                    // In case when the exact timestamp has been found and not forcing uniqueness,
                    // we must find the first/last of them in a row of equal timestamps.
                    // To do that, we continue dividing until the last element.
                    if (findFirst)
                    {
                        if (start == mid)
                            return mid;
                        end = mid;
                    }
                    else
                    {
                        if (end == mid)
                            return mid;

                        // special case - see above
                        if (end - start == 1)
                            return TimestampAccessor(buff[1]).CompareTo(timestamp) == 0 ? mid + 1 : mid;

                        start = mid;
                    }
                }
                else if (comp < 0)
                    start = mid + 1;
                else
                    end = mid - 1;
            }

            return ~start;
        }

        public void TruncateFile(UtcDateTime lastTimestampToPreserve)
        {
            long newCount = BinarySearch(lastTimestampToPreserve, false);
            newCount = newCount < 0 ? ~newCount : newCount + 1;

            TruncateFile(newCount);
        }

        public void TruncateFile(long newCount)
        {
            if (newCount == Count)
                return;

            PerformTruncateFile(newCount);

            // Invalidate timestamps
            if (Count == 0)
                _firstTimestamp = null;
            _lastTimestamp = null;
        }

        #endregion

        #region IHistFeed<T> Members

        public ITimeSeries<T> GetTimeSeries(UtcDateTime start, UtcDateTime end)
        {
            T[] result = ReadData(start, end, int.MaxValue);
            return new MergedTimeSeries<T>(result, TimestampAccessor);
        }

        ITimeSeries IHistFeedInt.GetTimeSeries(UtcDateTime start, UtcDateTime end)
        {
            return GetTimeSeries(start, end);
        }

        #endregion

        #region IStoredTimeSeries<T> Members

        public ITimeSeries<T> GetTimeSeries(long firstItemIdx, int count)
        {
            throw new NotImplementedException();
        }

        ITimeSeries IStoredTimeSeries.GetTimeSeries(long firstItemIdx, int count)
        {
            return GetTimeSeries(firstItemIdx, count);
        }

        public int ReadData(UtcDateTime fromInclusive, ArraySegment<T> buffer)
        {
            return ReadData(fromInclusive, UtcDateTime.MaxValue, buffer);
        }

        #endregion

        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        public void AppendData(ArraySegment<T> buffer)
        {
            if (buffer.Array == null) throw new ArgumentNullException("buffer");
            if (buffer.Count == 0)
                return;

            UtcDateTime firstBufferTs = TimestampAccessor(buffer.Array[buffer.Offset]);
            UtcDateTime newTs = firstBufferTs;
            UtcDateTime lastTs = LastFileTS ?? UtcDateTime.MinValue;

            bool isEmptyFile = Count == 0;
            if (!isEmptyFile)
            {
                // Make sure new data goes after the last item
                if (newTs < lastTs)
                    throw new ArgumentException(
                        string.Format("Last file timestamp ({0}) is greater than the first new item's timestamp ({1})",
                                      lastTs, newTs));
                if (UniqueTimestamps && newTs == lastTs)
                    throw new ArgumentException(
                        string.Format(
                            "Last file timestamp ({0}) equals to the first new item's timestamp (enfocing uniqueness)",
                            lastTs));
            }

            lastTs = newTs;

            // Validate new data
            int lastOffset = buffer.Offset + buffer.Count;
            for (int i = buffer.Offset + 1; i < lastOffset; i++)
            {
                newTs = TimestampAccessor(buffer.Array[i]);
                if (newTs < lastTs)
                    throw new ArgumentException(
                        string.Format(
                            "New item's timestamp at #{0} ({1}) is greater than timestamp of the following item #{2} ({3})",
                            i - 1, lastTs, i, newTs));
                if (UniqueTimestamps && newTs == lastTs)
                    throw new ArgumentException(
                        string.Format(
                            "New item's timestamp at #{0} ({1}) equals the timestamp of the following item #{2} (enforcing uniqueness)",
                            i - 1, lastTs, i));
                lastTs = newTs;
            }

            PerformFileAccess(Count, buffer, true);

            if (isEmptyFile)
                _firstTimestamp = firstBufferTs;
            _lastTimestamp = lastTs;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, 
        /// but not including <paramref name="toExclusive"/> into the <paramref name="buffer"/>.
        /// No more than buffer.Count items will be read.
        /// </summary>
        /// <returns>The total number of items read.</returns>
        public int ReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive, ArraySegment<T> buffer)
        {
            if (buffer.Array == null) throw new ArgumentNullException("buffer");
            Tuple<long, int> rng = CalcNeededBuffer(fromInclusive, toExclusive);

            PerformFileAccess(rng.Item1, new ArraySegment<T>(buffer.Array, buffer.Offset, Math.Min(buffer.Count, rng.Item2)), false);

            return rng.Item2;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, 
        /// but not including <paramref name="toExclusive"/>.
        /// </summary>
        /// <returns>An array of items no bigger than <paramref name="maxItemsToRead"/></returns>
        public T[] ReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive, int maxItemsToRead)
        {
            if (maxItemsToRead < 0) throw new ArgumentOutOfRangeException("maxItemsToRead", maxItemsToRead, "<0");
            Tuple<long, int> rng = CalcNeededBuffer(fromInclusive, toExclusive);

            var buffer = new T[Math.Min(maxItemsToRead, rng.Item2)];

            PerformFileAccess(rng.Item1, new ArraySegment<T>(buffer), false);

            return buffer;
        }

        /// <summary>
        /// Read all available data begining at a given timestamp
        /// </summary>
        public T[] ReadDataToEnd(UtcDateTime fromInclusive)
        {
            long firstItemIdx = FirstTimestampToIndex(fromInclusive);
            return ReadDataToEnd(firstItemIdx);
        }

        /// <summary>
        /// Read all available data begining at a given index
        /// </summary>
        public T[] ReadDataToEnd(long firstItemIdx)
        {
            int reqSize = (Count - firstItemIdx).ToIntCountChecked();
            var buffer = new T[reqSize];

            PerformFileAccess(firstItemIdx, new ArraySegment<T>(buffer), false);

            return buffer;
        }

        /// <summary>
        /// Returns the first index and the length of the data available in this file for the given range of dates
        /// </summary>
        protected Tuple<long, int> CalcNeededBuffer(UtcDateTime fromInclusive, UtcDateTime toExclusive)
        {
            if (fromInclusive.CompareTo(toExclusive) > 0)
                throw new ArgumentOutOfRangeException("fromInclusive", "'from' must be <= 'to'");

            long start = FirstTimestampToIndex(fromInclusive);
            long end = toExclusive == UtcDateTime.MaxValue ? Count : FirstTimestampToIndex(toExclusive);
            return Tuple.Create(start, (end - start).ToIntCountChecked());
        }

        private long FirstTimestampToIndex(UtcDateTime timestamp)
        {
            long start = BinarySearch(timestamp, true);
            if (start < 0)
                start = ~start;
            return start;
        }
    }
}