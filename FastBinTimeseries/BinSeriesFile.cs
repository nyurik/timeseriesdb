using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using JetBrains.Annotations;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries.Serializers;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Helper non-generic class aids in creating a new instance of <see cref="BinTimeseriesFile{T}"/>.
    /// </summary>
    public static class BinSeriesFile
    {
        /// <summary>
        /// Uses reflection to create an instance of <see cref="BinSeriesFile{TInd,TVal}"/>.
        /// </summary>
        public static IBinaryFile GenericNew(Type indType, Type itemType, string fileName,
                                             FieldInfo indexFieldInfo = null)
        {
            return (IBinaryFile)
                   Activator.CreateInstance(
                       typeof (BinSeriesFile<,>).MakeGenericType(indType, itemType),
                       fileName, indexFieldInfo);
        }
    }

    /// <summary>
    /// Object representing a binary-serialized long-based series file.
    /// </summary>
    public class BinSeriesFile<TInd, TVal> : BinaryFile<TVal>, IEnumerableFeed<TInd, TVal>
        where TInd : struct, IComparable<TInd>
    {
        private const int DefaultMaxBinaryCacheSize = 1 << 20;

        // ReSharper disable StaticFieldInGenericType
        private static readonly Version Version10 = new Version(1, 0);
        private static readonly Version Version11 = new Version(1, 1);
        // ReSharper restore StaticFieldInGenericType

        private TInd? _firstIndex;
        private FieldInfo _indexFieldInfo;
        private TInd? _lastIndex;
        private Tuple<long, ConcurrentDictionary<long, TInd>> _searchCache;
        private bool _uniqueIndexes;

        #region Constructors

        /// <summary>
        /// Allow Activator non-public instantiation
        /// </summary>
        protected BinSeriesFile()
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        /// <param name="indexFieldInfo">Field containing the TInd index, or null to get default</param>
        public BinSeriesFile(string fileName, FieldInfo indexFieldInfo = null)
            : base(fileName)
        {
            UniqueIndexes = false;
            IndexFieldInfo = indexFieldInfo ?? DynamicCodeFactory.Instance.Value.GetIndexField<TVal>();
        }

        protected override Version Init(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            Version ver = reader.ReadVersion();
            if (ver != Version11 && ver != Version10)
                throw new IncompatibleVersionException(GetType(), ver);

            // UniqueIndexes was not available in ver 1.0
            UniqueIndexes = ver > Version10 && reader.ReadBoolean();

            string fieldName = reader.ReadString();

            FieldInfo fieldInfo = typeof (TVal).GetField(fieldName, TypeExtensions.AllInstanceMembers);
            if (fieldInfo == null)
                throw new BinaryFileException(
                    "Index field {0} was not found in type {1}", fieldName, typeof (TVal).FullName);
            IndexFieldInfo = fieldInfo;

            return ver;
        }

        protected override Version WriteCustomHeader(BinaryWriter writer)
        {
            writer.WriteVersion(Version11);
            writer.Write(UniqueIndexes);
            writer.Write(IndexFieldInfo.Name);
            return Version11;
        }

        #endregion

        /// <summary> Number of binary search lookups to cache. 0-internal defaults, negative-disable </summary>
        public int BinarySearchCacheSize { get; set; }

        public FieldInfo IndexFieldInfo
        {
            get { return _indexFieldInfo; }
            set
            {
                ThrowOnInitialized();
                if (value == null)
                    throw new ArgumentNullException("value");

                IndexAccessor = DynamicCodeFactory.Instance.Value.GetIndexAccessor<TVal, TInd>(value);
                _indexFieldInfo = value;
            }
        }

        public TInd? FirstFileIndex
        {
            get
            {
                long count = Count;
                ResetOnChangedAndGetCache(count, false);

                if (_firstIndex == null && count > 0)
                {
                    var oneElementBuff = new TVal[1];
                    PerformFileAccess(0, new ArraySegment<TVal>(oneElementBuff), false);
                    _firstIndex = IndexAccessor(oneElementBuff[0]);
                }
                return _firstIndex;
            }
        }

        public TInd? LastFileIndex
        {
            get
            {
                long count = Count;
                ResetOnChangedAndGetCache(count, false);

                if (_lastIndex == null && count > 0)
                {
                    var oneElementBuff = new TVal[1];
                    PerformFileAccess(count - 1, new ArraySegment<TVal>(oneElementBuff), false);
                    _lastIndex = IndexAccessor(oneElementBuff[0]);
                }
                return _lastIndex;
            }
        }

        public bool UniqueIndexes
        {
            get { return _uniqueIndexes; }
            set
            {
                ThrowOnInitialized();
                _uniqueIndexes = value;
            }
        }

        #region IEnumerableFeed<TInd,TVal> Members

        /// <summary>
        /// A delegate to a function that extracts index of a given item
        /// </summary>
        public Func<TVal, TInd> IndexAccessor { get; private set; }

        public IEnumerable<ArraySegment<TVal>> StreamSegments(TInd from, bool inReverse, int bufferSize)
        {
            long index = FirstIndexToPos(from);
            if (inReverse)
                index--;

            return PerformStreaming(index, inReverse, bufferSize);
        }

        #endregion

        public void ReadData(long firstItemIdx, ArraySegment<TVal> buffer)
        {
            PerformFileAccess(firstItemIdx, buffer, false);
        }

        public long BinarySearch(TInd index)
        {
            if (!UniqueIndexes)
                throw new InvalidOperationException(
                    "This method call is only allowed for the unique index file. Use BinarySearch(TInd, bool) instead.");
            return BinarySearch(index, true);
        }

        public long BinarySearch(TInd index, bool findFirst)
        {
            long start = 0L;
            long count = Count;
            long end = count - 1;

            // Optimize in case we search outside of the file
            if (count <= 0)
                return ~0;

            TInd? tmp = FirstFileIndex;
            if (tmp == null || index.CompareTo(tmp.Value) < 0)
                return ~0;

            tmp = LastFileIndex;
            if (tmp == null || index.CompareTo(tmp.Value) > 0)
                return ~count;

            var buff = new TVal[2];
            var oneElementSegment = new ArraySegment<TVal>(buff, 0, 1);

            ConcurrentDictionary<long, TInd> cache = null;
            if (BinarySearchCacheSize >= 0)
            {
                cache = ResetOnChangedAndGetCache(count, true);
                if (cache.Count > (BinarySearchCacheSize == 0 ? DefaultMaxBinaryCacheSize : BinarySearchCacheSize))
                    cache.Clear();
            }

            while (start <= end)
            {
                long mid = start + ((end - start) >> 1);
                TInd timeAtMid;
                TInd timeAtMid2 = default(TInd);

                // Read new value from file unless we already have it pre-cached in the dictionary
                if (end - start == 1 && !UniqueIndexes && !findFirst)
                {
                    // for the special case where we are left with two elements,
                    // and searching for the last non-unique element,
                    // read both elements to see if the 2nd one matches our search
                    if (cache == null
                        || !cache.TryGetValue(mid, out timeAtMid)
                        || !cache.TryGetValue(mid + 1, out timeAtMid2))
                    {
                        PerformFileAccess(mid, new ArraySegment<TVal>(buff), false);
                        timeAtMid = IndexAccessor(buff[0]);
                        timeAtMid2 = IndexAccessor(buff[1]);
                        if (cache != null)
                        {
                            cache.TryAdd(mid, timeAtMid);
                            cache.TryAdd(mid + 1, timeAtMid2);
                        }
                    }
                }
                else
                {
                    if (cache == null || !cache.TryGetValue(mid, out timeAtMid))
                    {
                        PerformFileAccess(mid, oneElementSegment, false);
                        timeAtMid = IndexAccessor(buff[0]);
                        if (cache != null)
                            cache.TryAdd(mid, timeAtMid);
                    }
                }

                int comp = timeAtMid.CompareTo(index);
                if (comp == 0)
                {
                    if (UniqueIndexes)
                        return mid;

                    // In case when the exact index has been found and not forcing uniqueness,
                    // we must find the first/last of them in a row of equal indexes.
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
                            return timeAtMid2.CompareTo(index) == 0 ? mid + 1 : mid;

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

        public void TruncateFile(TInd lastIndexToPreserve)
        {
            long newCount = BinarySearch(lastIndexToPreserve, false);
            newCount = newCount < 0 ? ~newCount : newCount + 1;

            TruncateFile(newCount);
        }

        public void TruncateFile(long newCount)
        {
            if (newCount == Count)
                return;

            PerformTruncateFile(newCount);

            // Invalidate index
            if (Count == 0)
                _firstIndex = null;
            _lastIndex = null;
        }

        private ConcurrentDictionary<long, TInd> ResetOnChangedAndGetCache(long count, bool createCache)
        {
            Tuple<long, ConcurrentDictionary<long, TInd>> sc = _searchCache;

            if (sc == null || sc.Item1 != count || (createCache && sc.Item2 == null))
            {
                lock (LockObj)
                {
                    sc = _searchCache;
                    bool countChanged = sc == null || sc.Item1 != count;

                    if (countChanged)
                    {
                        // always reset just in case
                        _firstIndex = null;
                        _lastIndex = null;
                    }

                    if (countChanged || (createCache && sc.Item2 == null))
                    {
                        ConcurrentDictionary<long, TInd> cache =
                            createCache ? new ConcurrentDictionary<long, TInd>() : null;

                        _searchCache = Tuple.Create(count, cache);
                        return cache;
                    }
                }
            }

            return sc.Item2;
        }

        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        [Obsolete("Use overloaded method")]
        public void AppendData(ArraySegment<TVal> buffer)
        {
            if (buffer.Array == null)
                throw new ArgumentNullException("buffer");
            AppendData(new[] {buffer});
        }
        
        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        public void AppendData([NotNull] IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncation = false)
        {
            if (bufferStream == null)
                throw new ArgumentNullException("bufferStream");
            foreach (var seg in ProcessWriteStream(bufferStream, allowFileTruncation))
                PerformFileAccess(Count, seg, true);
        }

        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        public IEnumerable<ArraySegment<TVal>> ProcessWriteStream([NotNull] IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncations)
        {
            var isFirstSeg = true;

            TInd lastTs = LastFileIndex ?? default(TInd);
            int segInd = 0;
            bool isEmptyFile = Count == 0;

            foreach (ArraySegment<TVal> buffer in bufferStream)
            {
                if (buffer.Array == null)
                    throw new SerializationException("BufferStream may not contain ArraySegments with null Array");
                if (buffer.Count == 0)
                    continue;

                TInd firstBufferTs = IndexAccessor(buffer.Array[buffer.Offset]);
                TInd newTs = firstBufferTs;

                if (!isEmptyFile)
                {
                    // Make sure new data goes after the last item
                    if (newTs.CompareTo(lastTs) < 0)
                    {
                        if (!allowFileTruncations)
                            throw new BinaryFileException(
                                "Last index in {2} ({0}) is greater than the first new item's index ({1})",
                                lastTs, newTs, isFirstSeg ? "file" : "segment");
                    } else if (UniqueIndexes && newTs.CompareTo(lastTs) == 0)
                        throw new BinaryFileException(
                            "Last index in {1} ({0}) equals to the first new item's index (enfocing uniqueness)",
                            lastTs, isFirstSeg ? "file" : "segment");
                }

                lastTs = newTs;

                // Validate new data
                int lastOffset = buffer.Offset + buffer.Count;
                for (int i = buffer.Offset + 1; i < lastOffset; i++)
                {
                    newTs = IndexAccessor(buffer.Array[i]);
                    if (newTs.CompareTo(lastTs) < 0)
                        throw new BinaryFileException(
                            "Segment {4}, new item's index at #{0} ({1}) is greater than index of the following item #{2} ({3})",
                            i - 1, lastTs, i, newTs, segInd);
                    if (UniqueIndexes && newTs.CompareTo(lastTs) == 0)
                        throw new BinaryFileException(
                            "Segment {4} new item's index at #{0} ({1}) equals the index of the following item #{2} (enforcing uniqueness)",
                            i - 1, lastTs, i, segInd);
                    lastTs = newTs;
                }

                yield return buffer;

                if (isEmptyFile)
                    _firstIndex = firstBufferTs;
                _lastIndex = lastTs;
                isEmptyFile = false;
                isFirstSeg = false;
                segInd++;
            }
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, 
        /// but not including <paramref name="toExclusive"/> into the <paramref name="buffer"/>.
        /// No more than buffer.Count items will be read.
        /// </summary>
        /// <returns>The total number of items read.</returns>
        public int ReadData(TInd fromInclusive, TInd toExclusive, ArraySegment<TVal> buffer)
        {
            if (buffer.Array == null)
                throw new ArgumentNullException("buffer");
            Tuple<long, int> rng = CalcNeededBuffer(fromInclusive, toExclusive);

            PerformFileAccess(
                rng.Item1,
                new ArraySegment<TVal>(buffer.Array, buffer.Offset, Math.Min(buffer.Count, rng.Item2)),
                false);

            return rng.Item2;
        }

        /// <summary>
        /// Read data starting at <paramref name="fromInclusive"/>, up to, 
        /// but not including <paramref name="toExclusive"/>.
        /// </summary>
        /// <returns>An array of items no bigger than <paramref name="maxItemsToRead"/></returns>
        public TVal[] ReadData(TInd fromInclusive, TInd toExclusive, int maxItemsToRead)
        {
            if (maxItemsToRead < 0)
                throw new ArgumentOutOfRangeException("maxItemsToRead", maxItemsToRead, "<0");
            Tuple<long, int> rng = CalcNeededBuffer(fromInclusive, toExclusive);

            var buffer = new TVal[Math.Min(maxItemsToRead, rng.Item2)];

            PerformFileAccess(rng.Item1, new ArraySegment<TVal>(buffer), false);

            return buffer;
        }

        /// <summary>
        /// Read all available data begining at a given index
        /// </summary>
        public TVal[] ReadDataToEnd(TInd fromInclusive)
        {
            long firstItemIdx = FirstIndexToPos(fromInclusive);
            return ReadDataToEnd(firstItemIdx);
        }

        /// <summary>
        /// Read all available data begining at a given index
        /// </summary>
        public TVal[] ReadDataToEnd(long firstItemIdx)
        {
            int reqSize = (Count - firstItemIdx).ToIntCountChecked();
            var buffer = new TVal[reqSize];

            PerformFileAccess(firstItemIdx, new ArraySegment<TVal>(buffer), false);

            return buffer;
        }

        /// <summary>
        /// Returns the first index and the length of the data available in this file for the given range of dates
        /// </summary>
        protected Tuple<long, int> CalcNeededBuffer(TInd fromInclusive, TInd toExclusive)
        {
            if (fromInclusive.CompareTo(toExclusive) > 0)
                throw new ArgumentOutOfRangeException("fromInclusive", "'from' must be <= 'to'");

            long start = FirstIndexToPos(fromInclusive);
            long end = FirstIndexToPos(toExclusive);
            return Tuple.Create(start, (end - start).ToIntCountChecked());
        }

        private long FirstIndexToPos(TInd index)
        {
            long start = BinarySearch(index, true);
            if (start < 0)
                start = ~start;
            return start;
        }
    }
}