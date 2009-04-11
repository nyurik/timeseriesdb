using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public class BinTimeseriesFile<T> : BinaryFile<T>
    {
        private static readonly Version CurrentVersion = new Version(1, 0);
        private FieldInfo m_dateTimeFieldInfo;
        private DateTime m_lastTimestamp = DateTime.MinValue;

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
            : this(fileName, GetDateTimeField())
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        /// <param name="dateTimeFieldInfo">Field containing the PackedDateTime timestamp</param>
        public BinTimeseriesFile(string fileName, FieldInfo dateTimeFieldInfo)
            : base(fileName)
        {
            m_dateTimeFieldInfo = dateTimeFieldInfo;
            TSAccessor = DynamicCodeFactory.Instance.CreateTSAccessor<T>(dateTimeFieldInfo);
        }

        private static FieldInfo GetDateTimeField()
        {
            Type itemType = typeof (T);
            FieldInfo[] fieldInfo = itemType.GetFields(DynamicCodeFactory.AllInstanceMembers);
            if (fieldInfo.Length < 1)
                throw new InvalidOperationException("No fields found in type " + itemType.FullName);

            FieldInfo result = null;
            foreach (FieldInfo fi in fieldInfo)
                if (fi.FieldType == typeof (PackedDateTime))
                {
                    if (result != null)
                        throw new InvalidOperationException(
                            "Must explicitly specify the fieldInfo because there is more than one PackedDateTime field in type " +
                            itemType.FullName);
                    result = fi;
                }

            if (result == null)
                throw new InvalidOperationException("No field of type PackedDateTime was found in type " +
                                                    itemType.FullName);

            return result;
        }

        #endregion

        protected Func<T, PackedDateTime> TSAccessor { get; private set; }

        protected override void ReadCustomHeader(BinaryReader stream, Version version, IDictionary<string, Type> typeMap)
        {
            if (version == CurrentVersion)
            {
                string fieldName = stream.ReadString();
                m_dateTimeFieldInfo = typeof (T).GetField(fieldName, DynamicCodeFactory.AllInstanceMembers);

                if (m_dateTimeFieldInfo == null)
                    throw new InvalidOperationException(
                        string.Format("Timestamp field {0} was not found in type {1}", fieldName, typeof (T).FullName));

                TSAccessor = DynamicCodeFactory.Instance.CreateTSAccessor<T>(m_dateTimeFieldInfo);
            }
            else
                FastBinFileUtils.ThrowUnknownVersion(version, GetType());
        }

        protected override Version WriteCustomHeader(BinaryWriter stream)
        {
            stream.Write(m_dateTimeFieldInfo.Name);
            return CurrentVersion;
        }

        protected long BinarySearch(DateTime value)
        {
            long start = 0L;
            long end = Count - 1;
            var oneElementBuff = new T[1];

            while (start <= end)
            {
                long mid = start + ((end - start) >> 1);

                PerformRead(mid, new ArraySegment<T>(oneElementBuff));
                int comp = ((DateTime) TSAccessor(oneElementBuff[0])).CompareTo(value);
                if (comp == 0)
                    return mid;
                if (comp < 0)
                    start = mid + 1;
                else
                    end = mid - 1;
            }
            return ~start;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, 
        /// but not including <paramref name="toExclusive"/> into the <paramref name="buffer"/>.
        /// No more than <paramref name="buffer.Count"/> items will be read.
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
        /// Read data starting at <paramref name="fromInclusive"/>, up to, 
        /// but not including <paramref name="toExclusive"/>.
        /// </summary>
        /// <returns>An array of items no bigger than <paramref name="maxItemsToRead"/></returns>
        public T[] ReadData(DateTime fromInclusive, DateTime toExclusive, int maxItemsToRead)
        {
            if (maxItemsToRead < 0) throw new ArgumentOutOfRangeException("maxItemsToRead", maxItemsToRead, "<0");
            var rng = CalcNeededBuffer(fromInclusive, toExclusive);
            
            var buffer = new T[Math.Min(maxItemsToRead, rng.Second)];

            PerformRead(rng.First, new ArraySegment<T>(buffer));

            return buffer;
        }

        public void AppendData(ArraySegment<T> buffer)
        {
            if (buffer.Array == null) throw new ArgumentNullException("buffer");
            if (buffer.Count == 0)
                return;

            // Get last file timestamp
            DateTime lastDt = m_lastTimestamp;
            if (lastDt == DateTime.MinValue && Count > 0)
            {
                var oneElementBuff = new T[1];
                PerformRead(Count - 1, new ArraySegment<T>(oneElementBuff));
                m_lastTimestamp = lastDt = TSAccessor(oneElementBuff[0]);
            }

            // Make sure new data goes after the last item
            PackedDateTime newDt = TSAccessor(buffer.Array[buffer.Offset]);
            if (newDt < lastDt)
                throw new ArgumentException(
                    string.Format("Last file item ({0}) is greater than the first new item ({1})",
                                  lastDt, newDt));
            lastDt = newDt;

            // Validate new data
            int lastOffset = buffer.Offset + buffer.Count;
            for (int i = buffer.Offset + 1; i < lastOffset; i++)
            {
                newDt = TSAccessor(buffer.Array[i]);
                if (newDt < lastDt)
                    throw new ArgumentException(
                        string.Format("Item at #{0} ({1}) is greater than item #{2} ({3})",
                                      i - 1, lastDt, i, newDt));
                lastDt = newDt;
            }

            PerformWrite(Count, buffer);

            m_lastTimestamp = lastDt;
        }

        /// <summary>
        /// Returns the first index and the length of the data available in this file for the given range of dates
        /// </summary>
        protected Tuple<long, int> CalcNeededBuffer(DateTime fromInclusive, DateTime toExclusive)
        {
            if (fromInclusive.CompareTo(toExclusive) > 0)
                throw new ArgumentOutOfRangeException("fromInclusive", "'from' must be <= 'to'");

            long start = IndexToLong(fromInclusive);
            return Tuple.Create(start, (IndexToLong(toExclusive) - start).ToInt32Checked());
        }

        protected long IndexToLong(DateTime timestamp)
        {
            long start = BinarySearch(timestamp);
            if (start < 0)
                start = ~start;
            return start;
        }
    }
}