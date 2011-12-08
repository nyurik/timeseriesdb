using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using JetBrains.Annotations;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries.Serializers;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Helper non-generic class aids in creating a new instance of <see cref="BinTimeseriesFile{T}"/>.
    /// </summary>
    public static class BinCompressedSeriesFile
    {
        /// <summary>
        /// Uses reflection to create an instance of <see cref="BinCompressedSeriesFile{TInd,TVal}"/>.
        /// </summary>
        public static IBinaryFile GenericNew(Type indType, Type itemType, string fileName,
                                             FieldInfo indexFieldInfo = null)
        {
            return (IBinaryFile)
                   Activator.CreateInstance(
                       typeof (BinCompressedSeriesFile<,>).MakeGenericType(indType, itemType),
                       fileName, indexFieldInfo);
        }
    }

    /// <summary>
    /// Object representing a binary-serialized long-based series file.
    /// </summary>
    public class BinCompressedSeriesFile<TInd, TVal> : BinaryFile<byte>, IEnumerableFeed<TInd, TVal>
        where TInd : struct, IComparable<TInd>
    {
        private const int DefaultMaxBinaryCacheSize = 1 << 20;

        // ReSharper disable StaticFieldInGenericType
        private static readonly Version Version10 = new Version(1, 0);
        private int _blockSize;
        // ReSharper restore StaticFieldInGenericType

        private BufferProvider<byte> _bufferByteProvider = new BufferProvider<byte>();
        private BufferProvider<TVal> _bufferProvider;
        private TInd? _firstIndex;
        private FieldInfo _indexFieldInfo;
        private TInd? _lastIndex;
        private int _maxItemByteSize;

        private Tuple<long, ConcurrentDictionary<long, TInd>> _searchCache;
        private DynamicSerializer<TVal> _serializer;
        private bool _uniqueIndexes;

        private int BlockSize
        {
            get { return _blockSize; }
            set
            {
                ThrowOnInitialized();
                if (value <= 0) throw new ArgumentOutOfRangeException("value", value, "<= 0");
                _blockSize = value;
            }
        }

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
                    TInd tmp;
                    if (TryGetFirst(StreamSegments(default(TInd), maxItemCount: 1), out tmp))
                        _firstIndex = tmp;
                }

                return _firstIndex;
            }
        }

        public TInd? LastFileIndex
        {
            get
            {
                return null;
//                long count = Count;
//                ResetOnChangedAndGetCache(count, false);
//
//                if (_lastIndex == null && count > 0)
//                {
//                    TInd tmp;
//                    if (TryGetFirst(StreamSegments(TInd.MaxValue, maxItemCount: 1), out tmp))
//                        _firstIndex = tmp;
//                }
//                return _lastIndex;
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

        #region Constructors

        /// <summary>
        /// Allow Activator non-public instantiation
        /// </summary>
        protected BinCompressedSeriesFile()
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        /// <param name="indexFieldInfo">Field containing the TInd index, or null to get default</param>
        public BinCompressedSeriesFile(string fileName, FieldInfo indexFieldInfo = null)
            : base(fileName)
        {
            UniqueIndexes = false;
            IndexFieldInfo = indexFieldInfo ?? DynamicCodeFactory.Instance.Value.GetIndexField<TVal>();
            _serializer = DynamicSerializer<TVal>.CreateDefault();
            BlockSize = 16*1024;
        }

        protected override Version Init(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            Version ver = reader.ReadVersion();
            if (ver != Version10)
                throw new IncompatibleVersionException(GetType(), ver);

            BlockSize = reader.ReadInt32();
            UniqueIndexes = reader.ReadBoolean();
            string fieldName = reader.ReadString();

            FieldInfo fieldInfo = typeof (TVal).GetField(fieldName, TypeExtensions.AllInstanceMembers);
            if (fieldInfo == null)
                throw new BinaryFileException(
                    "Index field {0} was not found in type {1}", fieldName, typeof (TVal).FullName);

            IndexFieldInfo = fieldInfo;

            _serializer = DynamicSerializer<TVal>.CreateFromReader(reader, typeMap);
            _maxItemByteSize = _serializer.RootField.GetMaxByteSize();

            return ver;
        }

        protected override Version WriteCustomHeader(BinaryWriter writer)
        {
            writer.WriteVersion(Version10);
            writer.Write(BlockSize);
            writer.Write(UniqueIndexes);
            writer.Write(IndexFieldInfo.Name);
            _serializer.WriteCustomHeader(writer);

            int minBlockSize = _serializer.GetMinimumBlockSize();
            if (BlockSize < minBlockSize)
                throw new SerializerException("BlockSize ({0}) must be at least {1} bytes", BlockSize, minBlockSize);

            _maxItemByteSize = _serializer.RootField.GetMaxByteSize();

            return Version10;
        }

        #endregion

        #region IEnumerableFeed<TInd,TVal> Members

        /// <summary>
        /// A delegate to a function that extracts index of a given item
        /// </summary>
        public Func<TVal, TInd> IndexAccessor { get; private set; }

        public IEnumerable<Buffer<TVal>> StreamSegments(TInd fromInd, bool inReverse = false,
                                                        IEnumerable<Buffer<TVal>> bufferProvider = null,
                                                        long maxItemCount = long.MaxValue)
        {
            long count = GetCount();

            // BUG: FirstIndexToPos(fromInd);
            long index = inReverse ? FastBinFileUtils.RoundDownToMultiple(count, BlockSize) : 0;


            return StreamSegments(index, count, inReverse, bufferProvider, maxItemCount);
        }

        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        public void AppendData(IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncation = false)
        {
            if (bufferStream == null)
                throw new ArgumentNullException("bufferStream");

            PerformWriteStreaming(ProcessWriteStream(bufferStream, allowFileTruncation));
        }

        #endregion

        private bool TryGetFirst(IEnumerable<Buffer<TVal>> stream, out TInd value, bool inReverse = false)
        {
            foreach (var b in stream)
            {
                value = IndexAccessor(inReverse ? b.Array[b.Count - 1] : b.Array[0]);
                return true;
            }

            value = default(TInd);
            return false;
        }

        public override long Count
        {
            get
            {
                throw new InvalidOperationException("Unable to calculate Count for ");
            }
        }

        private IEnumerable<Buffer<TVal>> StreamSegments(long index, long fileCount, bool inReverse = false,
                                                         IEnumerable<Buffer<TVal>> bufferProvider = null,
                                                         long maxItemCount = long.MaxValue)
        {
            CodecReader codec = null;
            if (index % BlockSize != 0)
                throw new ArgumentOutOfRangeException("index", index, "Must be a multiple of BlockSize=" + BlockSize);

            IEnumerator<Buffer<TVal>> valBufs = null;
            try
            {
                if (_bufferByteProvider==null)
                    _bufferByteProvider = new BufferProvider<byte>();

                int firstBlockSize;

                IEnumerable<Buffer<byte>> byteBuffs;
                if (maxItemCount < BlockSize / _maxItemByteSize)
                {
                    firstBlockSize = (int) Math.Min(maxItemCount*_maxItemByteSize, fileCount);
                    byteBuffs = _bufferByteProvider.YieldFixedSize(firstBlockSize);
                }
                else
                {
                    int smallSize = FastBinFileUtils.RoundUpToMultiple(16 * MinPageSize, BlockSize);
                    firstBlockSize = inReverse && index > fileCount - BlockSize ? (int)(fileCount % BlockSize) : smallSize;
                    
                    byteBuffs = _bufferByteProvider.YieldFixed(
                        firstBlockSize, smallSize, 4, FastBinFileUtils.RoundUpToMultiple(MaxLargePageSize/16, BlockSize));
                }

                var firstItemIdx = inReverse ? index + firstBlockSize : index;

                foreach (var seg in PerformStreaming(firstItemIdx, inReverse, byteBuffs))
                {
                    valBufs =
                        (bufferProvider
                         ?? (_bufferProvider ?? (_bufferProvider = new BufferProvider<TVal>()))
                                .YieldMaxGrowingBuffer(
                                    maxItemCount, 16*MinPageSize/ItemSize, 5, MaxLargePageSize/ItemSize))
                            .GetEnumerator();

                    if (!valBufs.MoveNext())
                        yield break;

                    Buffer<TVal> retBuf = valBufs.Current;

                    if (codec == null)
                        codec = new CodecReader(seg);
                    else
                        codec.AttachBuffer(seg);

                    long blocks = FastBinFileUtils.RoundUpToMultiple(seg.Count, BlockSize)/BlockSize;
                    for (int i = 0; i < blocks; i++)
                    {
                        codec.BufferPos = i*BlockSize;
                        _serializer.DeSerialize(codec, retBuf, int.MaxValue);
                    }

                    yield return retBuf;
                }
            }
            finally
            {
                if (valBufs != null)
                    valBufs.Dispose();
            }
        }

        public long Search(TInd index)
        {
            if (!UniqueIndexes)
                throw new InvalidOperationException(
                    "This method call is only allowed for the unique index file. Use BinarySearch(TInd, bool) instead.");
            return Search(index, true);
        }

        public long Search(TInd index, bool findFirst)
        {
            long start = 0L;
            long blockCount = FastBinFileUtils.RoundUpToMultiple(Count, BlockSize)/BlockSize;
            long end = blockCount - 1;

            // empty file
            if (blockCount <= 0)
                return ~0;

            var buff = new TVal[2];
            var oneElementSegment = new ArraySegment<TVal>(buff, 0, 1);

            ConcurrentDictionary<long, TInd> cache = null;
            if (BinarySearchCacheSize >= 0)
            {
                cache = ResetOnChangedAndGetCache(blockCount, true);
                if (cache.Count > (BinarySearchCacheSize == 0 ? DefaultMaxBinaryCacheSize : BinarySearchCacheSize))
                    cache.Clear();
            }

            bool useMma = UseMemoryMappedAccess(1, false);

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
                        throw new NotImplementedException();
//                        if (PerformUnsafeBlockAccess(mid, false, new ArraySegment<TVal>(buff), count*ItemSize, useMma)
//                            < 2)
//                            throw new BinaryFileException("Unable to read two blocks");

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
                        throw new NotImplementedException();
//                        if (PerformUnsafeBlockAccess(mid, false, oneElementSegment, count*ItemSize, useMma) < 1)
//                            throw new BinaryFileException("Unable to read index block");
                        timeAtMid = IndexAccessor(oneElementSegment.Array[0]);
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
            long newCount = Search(lastIndexToPreserve, false);
            newCount = newCount < 0 ? ~newCount : newCount + 1;

            TruncateFile(newCount);
        }

        public void TruncateFile(long newCount)
        {
            long fileCount = Count;
            if (newCount == fileCount)
                return;

            PerformTruncateFile(newCount);

            // Invalidate index
            if (newCount == 0)
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
        private IEnumerable<ArraySegment<byte>> ProcessWriteStream(
            [NotNull] IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncations)
        {
            TInd lastTs = LastFileIndex ?? default(TInd);
            long count = GetCount();
            bool isEmptyFile = count == 0;

            using (IEnumerator<TVal> iterator = VerifyValues(bufferStream).GetEnumerator())
            {
                if (!iterator.MoveNext())
                    yield break;

                TInd firstBufferTs = IndexAccessor(iterator.Current);
                TInd newTs = firstBufferTs;

                if (!isEmptyFile)
                {
                    // Make sure new data goes after the last item
                    if (newTs.CompareTo(lastTs) < 0)
                    {
                        if (!allowFileTruncations)
                            throw new BinaryFileException(
                                "Last index in file ({0}) is greater than the first new item's index ({1})",
                                lastTs, newTs);
                    }
                    else if (UniqueIndexes && newTs.CompareTo(lastTs) == 0)
                        throw new BinaryFileException(
                            "Last index in file ({0}) equals to the first new item's index (enfocing uniqueness)",
                            lastTs);
                }

                var codec = new CodecWriter(BlockSize);
                while (true)
                {
                    codec.Count = 0;
                    bool hasMore = _serializer.Serialize(codec, iterator);
                    if (codec.Count == 0)
                        throw new SerializerException("Internal serializer error: buffer is empty");

                    yield return new ArraySegment<byte>(codec.Buffer, 0, codec.Count);

                    if (!hasMore)
                        break;
                }

                if (isEmptyFile)
                    _firstIndex = firstBufferTs;
                _lastIndex = null;
            }
        }

        private IEnumerable<TVal> VerifyValues(IEnumerable<ArraySegment<TVal>> stream)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            bool isFirst = true;
            TInd lastInd = default(TInd);
            long segInd = 0;

            foreach (var v in stream)
            {
                if (v.Count > 0)
                {
                    for (int i = v.Offset; i < v.Count; i++)
                    {
                        TInd newInd = IndexAccessor(v.Array[i]);

                        if (!isFirst)
                        {
                            if (newInd.CompareTo(lastInd) < 0)
                                throw new BinaryFileException(
                                    "Segment {0}, last item's index {1} is greater than index of the following item #{2} ({3})",
                                    segInd, lastInd, i, newInd);

                            if (UniqueIndexes && newInd.CompareTo(lastInd) == 0)
                                throw new BinaryFileException(
                                    "Segment {0}, last item's index {1} equals to the following item's index at #{2} (enforcing index uniqueness)",
                                    segInd, lastInd, i);
                        }
                        else
                            isFirst = false;

                        lastInd = newInd;

                        yield return v.Array[i];
                    }
                }

                segInd++;
            }
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
            long start = Search(index, true);
            if (start < 0)
                start = ~start;
            return start;
        }
    }
}