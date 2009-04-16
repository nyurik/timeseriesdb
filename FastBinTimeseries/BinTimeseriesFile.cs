using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Helper non-generic class aids in creating a new instance of <see cref="BinTimeseriesFile{T}"/>.
    /// </summary>
    public class BinTimeseriesFile
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
    public class BinTimeseriesFile<T> : BinaryFile<T>, IBinTimeseriesFile
    {
        private static readonly Version CurrentVersion = new Version(1, 1);
        private static readonly Version Version10 = new Version(1, 0);
        private UtcDateTime? _firstTimestamp;
        private bool _uniqueTimestamps;
        private UtcDateTime? _lastTimestamp;
        private FieldInfo _timestampFieldInfo;

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
            : this(fileName, GetTimestampField())
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

        private static FieldInfo GetTimestampField()
        {
            Type itemType = typeof (T);
            FieldInfo[] fieldInfo = itemType.GetFields(DynamicCodeFactory.AllInstanceMembers);
            if (fieldInfo.Length < 1)
                throw new InvalidOperationException("No fields found in type " + itemType.FullName);

            FieldInfo result = null;
            foreach (FieldInfo fi in fieldInfo)
                if (fi.FieldType == typeof (UtcDateTime))
                {
                    if (result != null)
                        throw new InvalidOperationException(
                            "Must explicitly specify the fieldInfo because there is more than one UtcDateTime field in type " +
                            itemType.FullName);
                    result = fi;
                }

            if (result == null)
                throw new InvalidOperationException("No field of type UtcDateTime was found in type " +
                                                    itemType.FullName);

            return result;
        }

        protected override void ReadCustomHeader(BinaryReader stream, Version version, IDictionary<string, Type> typeMap)
        {
            if (version == CurrentVersion || version == Version10)
            {
                // UniqueTimestamps was not available in ver 1.0
                UniqueTimestamps = version > Version10 && stream.ReadBoolean();

                string fieldName = stream.ReadString();

                FieldInfo fieldInfo = typeof (T).GetField(fieldName, DynamicCodeFactory.AllInstanceMembers);
                if (fieldInfo == null)
                    throw new InvalidOperationException(
                        string.Format("Timestamp field {0} was not found in type {1}", fieldName, typeof (T).FullName));
                TimestampFieldInfo = fieldInfo;
            }
            else
                FastBinFileUtils.ThrowUnknownVersion(version, GetType());
        }

        protected override Version WriteCustomHeader(BinaryWriter stream)
        {
            stream.Write(UniqueTimestamps);
            stream.Write(TimestampFieldInfo.Name);
            return CurrentVersion;
        }

        #endregion

        protected Func<T, UtcDateTime> TSAccessor { get; private set; }

        #region IBinTimeseriesFile Members

        public FieldInfo TimestampFieldInfo
        {
            get { return _timestampFieldInfo; }
            set
            {
                ThrowOnInitialized();
                if (value == null) throw new ArgumentNullException();

                TSAccessor = DynamicCodeFactory.Instance.CreateTSAccessor<T>(value);
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
                    PerformRead(0, new ArraySegment<T>(oneElementBuff));
                    _firstTimestamp = TSAccessor(oneElementBuff[0]);
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
                    PerformRead(Count - 1, new ArraySegment<T>(oneElementBuff));
                    _lastTimestamp = TSAccessor(oneElementBuff[0]);
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
                    PerformRead(mid, new ArraySegment<T>(buff));
                }
                else
                    PerformRead(mid, oneElementSegment);


                int comp = TSAccessor(buff[0]).CompareTo(timestamp);
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
                            return TSAccessor(buff[1]).CompareTo(timestamp) == 0 ? mid + 1 : mid;

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

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, 
        /// but not including <paramref name="toExclusive"/> into the <paramref name="buffer"/>.
        /// No more than <paramref name="buffer.Count"/> items will be read.
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
        /// Read data starting at <paramref name="fromInclusive"/>, up to, 
        /// but not including <paramref name="toExclusive"/>.
        /// </summary>
        /// <returns>An array of items no bigger than <paramref name="maxItemsToRead"/></returns>
        public T[] ReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive, int maxItemsToRead)
        {
            if (maxItemsToRead < 0) throw new ArgumentOutOfRangeException("maxItemsToRead", maxItemsToRead, "<0");
            Tuple<long, int> rng = CalcNeededBuffer(fromInclusive, toExclusive);

            var buffer = new T[Math.Min(maxItemsToRead, rng.Second)];

            PerformRead(rng.First, new ArraySegment<T>(buffer));

            return buffer;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusiveIndex"/> to fill up the <param name="buffer"/>.
        /// </summary>
        public void ReadData(long fromInclusiveIndex, ArraySegment<T> buffer)
        {
            PerformRead(fromInclusiveIndex, buffer);
        }

        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        public void AppendData(ArraySegment<T> buffer)
        {
            if (buffer.Array == null) throw new ArgumentNullException("buffer");
            if (buffer.Count == 0)
                return;

            UtcDateTime firstBufferTs = TSAccessor(buffer.Array[buffer.Offset]);
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
                newTs = TSAccessor(buffer.Array[i]);
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

            PerformWrite(Count, buffer);

            if (isEmptyFile)
                _firstTimestamp = firstBufferTs;
            _lastTimestamp = lastTs;
        }

        /// <summary>
        /// Returns the first index and the length of the data available in this file for the given range of dates
        /// </summary>
        protected Tuple<long, int> CalcNeededBuffer(UtcDateTime fromInclusive, UtcDateTime toExclusive)
        {
            if (fromInclusive.CompareTo(toExclusive) > 0)
                throw new ArgumentOutOfRangeException("fromInclusive", "'from' must be <= 'to'");

            long start = FirstTimestampToLong(fromInclusive);
            return Tuple.Create(start, (FirstTimestampToLong(toExclusive) - start).ToInt32Checked());
        }

        private long FirstTimestampToLong(UtcDateTime timestamp)
        {
            long start = BinarySearch(timestamp, true);
            if (start < 0)
                start = ~start;
            return start;
        }
    }
}