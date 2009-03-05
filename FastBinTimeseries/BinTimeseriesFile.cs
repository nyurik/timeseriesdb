using System;
using System.IO;
using System.Reflection;

namespace NYurik.FastBinTimeseries
{
    public class BinTimeseriesFile<T> : BinaryFile<T>
    {
        private static readonly Version CurrentVersion = new Version(1, 0);
        private FieldInfo DateTimeFieldInfo;
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
            : this(fileName, null)
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        /// <param name="customSerializer">Custom serializer or null for default</param>
        public BinTimeseriesFile(string fileName, IBinSerializer<T> customSerializer)
            : this(fileName, customSerializer, GetDateTimeField())
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        /// <param name="customSerializer">Custom serializer or null for default</param>
        /// <param name="dateTimeFieldInfo">Field containing the PackedDateTime timestamp</param>
        public BinTimeseriesFile(string fileName, IBinSerializer<T> customSerializer, FieldInfo dateTimeFieldInfo)
            : base(fileName, customSerializer)
        {
            DateTimeFieldInfo = dateTimeFieldInfo;
            TSAccessor = DynamicCodeFactory.Instance.CreateTSAccessor<T>(dateTimeFieldInfo);
        }

        private static FieldInfo GetDateTimeField()
        {
            var itemType = typeof (T);
            var fieldInfo = itemType.GetFields(DynamicCodeFactory.AllInstanceMembers);
            if (fieldInfo.Length < 1)
                throw new InvalidOperationException("No fields found in type " + itemType.FullName);

            FieldInfo result = null;
            foreach (var fi in fieldInfo)
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

        protected override void ReadCustomHeader(BinaryReader stream, Version version)
        {
            if (version == CurrentVersion)
            {
                var fieldName = stream.ReadString();
                DateTimeFieldInfo = typeof (T).GetField(fieldName, DynamicCodeFactory.AllInstanceMembers);

                if (DateTimeFieldInfo == null)
                    throw new InvalidOperationException(
                        string.Format("Timestamp field {0} was not found in type {1}", fieldName, typeof (T).FullName));

                TSAccessor = DynamicCodeFactory.Instance.CreateTSAccessor<T>(DateTimeFieldInfo);
            }
            else
                Utilities.ThrowUnknownVersion(version, GetType());
        }

        protected override Version WriteCustomHeader(BinaryWriter stream)
        {
            stream.Write(DateTimeFieldInfo.Name);
            return CurrentVersion;
        }

        protected long BinarySearch(DateTime value)
        {
            var start = 0L;
            var end = Count - 1;
            var oneElementBuff = new T[1];

            while (start <= end)
            {
                var mid = start + ((end - start) >> 1);

                PerformFileAccess(mid, oneElementBuff, 0, 1, Read);
                var comp = ((DateTime) TSAccessor(oneElementBuff[0])).CompareTo(value);
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
        /// but not including <paramref name="toExclusive"/>.
        /// </summary>
        /// <returns>An array of items no bigger than <paramref name="maxItemsToRead"/></returns>
        public T[] ReadData(DateTime fromInclusive, DateTime toExclusive, int maxItemsToRead)
        {
            T[] buffer = null;
            ReadData(fromInclusive, toExclusive, ref buffer, 0, maxItemsToRead);
            return buffer;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, 
        /// but not including <paramref name="toExclusive"/> into the <paramref name="buffer"/>
        /// starting at <paramref name="offset"/>. No more than <paramref name="maxItemsToRead"/> items
        /// will be read.
        /// </summary>
        /// <returns>The total number of items between the given timestamps.</returns>
        public int ReadData(DateTime fromInclusive, DateTime toExclusive, T[] buffer, int offset, int maxItemsToRead)
        {
            if (buffer == null) throw new ArgumentNullException("buffer");
            return ReadData(fromInclusive, toExclusive, ref buffer, offset, maxItemsToRead);
        }

        private int ReadData(DateTime fromInclusive, DateTime toExclusive, ref T[] buffer, int offset,
                             int maxItemsToRead)
        {
            if (buffer != null)
                Utilities.ValidateArrayParams(buffer, offset, maxItemsToRead);

            var start = BinarySearch(fromInclusive);
            if (start < 0)
                start = ~start;
            var end = BinarySearch(toExclusive);
            if (end < 0)
                end = ~end;

            var neededLength = Utilities.ToInt32Checked(end - start);
            if (buffer == null)
            {
                buffer = new T[Math.Min(neededLength, maxItemsToRead)];
                offset = 0;
            }

            PerformFileAccess(start, buffer, offset, Math.Min(neededLength, maxItemsToRead), Read);

            return neededLength;
        }

        public void AppendData(T[] buffer, int offset, int count)
        {
            Utilities.ValidateArrayParams(buffer, offset, count);
            if (count == 0)
                return;

            // Get last file timestamp
            var lastDt = m_lastTimestamp;
            if (lastDt == DateTime.MinValue && Count > 0)
            {
                var oneElementBuff = new T[1];
                PerformFileAccess(Count - 1, oneElementBuff, 0, 1, Read);
                m_lastTimestamp = lastDt = TSAccessor(oneElementBuff[0]);
            }

            // Make sure new data goes after the last item
            var newDt = TSAccessor(buffer[offset]);
            if (newDt < lastDt)
                throw new ArgumentException(
                    string.Format("Last file item ({0}) is greater than the first new item ({1})",
                                  lastDt, newDt));
            lastDt = newDt;

            // Validate new data
            var lastOffset = offset + count;
            for (var i = offset + 1; i < lastOffset; i++)
            {
                newDt = TSAccessor(buffer[i]);
                if (newDt < lastDt)
                    throw new ArgumentException(
                        string.Format("Item at #{0} ({1}) is greater than item #{2} ({3})",
                                      i - 1, lastDt, i, newDt));
                lastDt = newDt;
            }

            PerformFileAccess(Count, buffer, offset, count, Write);

            m_lastTimestamp = lastDt;
        }
    }
}