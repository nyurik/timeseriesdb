using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using JetBrains.Annotations;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries.CommonCode;
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
        private int _minItemByteSize;

        private Tuple<long, ConcurrentDictionary<long, TInd>> _searchCache;
        private DynamicSerializer<TVal> _serializer;
        private bool _uniqueIndexes;

        public int BlockSize
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
                long cachedFileCount = GetCount();
                ResetOnChangedAndGetCache(cachedFileCount, false);

                if (_firstIndex == null && cachedFileCount > 0)
                {
                    TInd tmp;
                    if (TryGetIndex(0, cachedFileCount, out tmp))
                        _firstIndex = tmp;
                }

                return _firstIndex;
            }
        }

        public TInd? LastFileIndex
        {
            get
            {
                long cachedFileCount = GetCount();
                ResetOnChangedAndGetCache(cachedFileCount, false);

                if (_lastIndex == null && cachedFileCount > 0)
                {
                    TInd tmp;
                    if (TryGetIndex(long.MaxValue, cachedFileCount, out tmp, inReverse: true))
                        _lastIndex = tmp;
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

        public override long Count
        {
            get { throw new InvalidOperationException("Count is not available for compressed files"); }
        }

        public DynamicSerializer<TVal> FieldSerializer
        {
            get { return _serializer; }
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
            _minItemByteSize = FieldSerializer.RootField.GetMinByteSize();
            _maxItemByteSize = FieldSerializer.RootField.GetMaxByteSize();

            return ver;
        }

        protected override Version WriteCustomHeader(BinaryWriter writer)
        {
            writer.WriteVersion(Version10);
            writer.Write(BlockSize);
            writer.Write(UniqueIndexes);
            writer.Write(IndexFieldInfo.Name);
            FieldSerializer.WriteCustomHeader(writer);

            _minItemByteSize = FieldSerializer.RootField.GetMinByteSize();
            _maxItemByteSize = FieldSerializer.RootField.GetMaxByteSize();
            if (BlockSize < _maxItemByteSize + CodecBase.ReservedSpace)
                throw new SerializerException("BlockSize ({0}) must be at least {1} bytes", BlockSize, _maxItemByteSize);

            return Version10;
        }

        #endregion

        #region IEnumerableFeed<TInd,TVal> Members

        /// <summary>
        /// A delegate to a function that extracts index of a given item
        /// </summary>
        public Func<TVal, TInd> IndexAccessor { get; private set; }

        public IEnumerable<ArraySegment<TVal>> StreamSegments(TInd fromInd, bool inReverse = false,
                                                              IEnumerable<Buffer<TVal>> bufferProvider = null,
                                                              long maxItemCount = long.MaxValue)
        {
            long cachedFileCount = GetCount();

            long block = Search(fromInd, cachedFileCount);
            if (block < 0)
                block = ~block - 1;
            else if (!UniqueIndexes)
                block--;

            return StreamSegments(fromInd, block, cachedFileCount, inReverse, bufferProvider, maxItemCount);
        }

        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        public void AppendData(IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncation = false)
        {
            PerformWriteStreaming(ProcessWriteStream(bufferStream, allowFileTruncation));
        }

        #endregion

        private IEnumerable<ArraySegment<TVal>> StreamSegments(TInd firstInd, long firstBlockInd, long cachedFileCount,
                                                               bool inReverse = false,
                                                               IEnumerable<Buffer<TVal>> bufferProvider = null,
                                                               long maxItemCount = long.MaxValue)
        {
            if (maxItemCount < 0)
                throw new ArgumentOutOfRangeException("maxItemCount", maxItemCount, "<0");

            if (cachedFileCount == 0 || maxItemCount == 0)
                yield break;

            long fileCountInBlocks = CalcBlockCount(cachedFileCount);

            if (firstBlockInd < 0)
            {
                if (inReverse)
                    yield break;
                firstBlockInd = 0;
            }
            else if (firstBlockInd >= fileCountInBlocks)
            {
                if (!inReverse)
                    yield break;
                firstBlockInd = fileCountInBlocks - 1;
            }

            if (_bufferByteProvider == null)
                _bufferByteProvider = new BufferProvider<byte>();

            int firstBlockSize = GetBlockSize(firstBlockInd, cachedFileCount);
            int smallSize = FastBinFileUtils.RoundUpToMultiple(MinPageSize, BlockSize);
            int largeSize = FastBinFileUtils.RoundUpToMultiple(MaxLargePageSize/16, BlockSize);

            IEnumerable<Buffer<byte>> byteBuffs = _bufferByteProvider.YieldFixed(
                firstBlockSize, BlockSize, smallSize, 4, largeSize);

            long firstItemIdx = firstBlockInd*BlockSize + (inReverse ? firstBlockSize - 1 : 0);

            CodecReader codec = null;

            foreach (var retBuf in 
                (bufferProvider
                 ?? (_bufferProvider ?? (_bufferProvider = new BufferProvider<TVal>()))
                        .YieldMaxGrowingBuffer(
                            maxItemCount, MinPageSize/ItemSize, 5, MaxLargePageSize/ItemSize)))
            {
                foreach (var seg in PerformStreaming(firstItemIdx, inReverse, byteBuffs, cachedCount: cachedFileCount))
                {
                    if (codec == null)
                        codec = new CodecReader(seg);
                    else
                        codec.AttachBuffer(seg);

                    // ignore suggested count, fill in as much as possible
                    retBuf.Count = 0;

                    long blocks = CalcBlockCount(seg.Count);
                    for (int i = 0; i < blocks; i++)
                    {
                        codec.BufferPos = i*BlockSize;
                        FieldSerializer.DeSerialize(codec, retBuf, int.MaxValue);
                    }

                    int pos = retBuf.Array.BinarySearch(
                        firstInd, (val, ind) => IndexAccessor(val).CompareTo(ind),
                        UniqueIndexes ? ListExtensions.Find.AnyEqual : ListExtensions.Find.FirstEqual, 0,
                        retBuf.Count);

                    int count, offset;
                    if (inReverse)
                    {
                        offset = 0;
                        count = pos < 0 ? ~pos : pos + 1;
                        if (count > maxItemCount)
                        {
                            int shrinkBy = count - (int) maxItemCount;
                            offset += shrinkBy;
                            count -= shrinkBy;
                        }
                    }
                    else
                    {
                        offset = pos < 0 ? ~pos : pos;
                        count = retBuf.Count - offset;
                        if (count > maxItemCount)
                            count = (int) maxItemCount;
                    }

                    if (count > 0)
                    {
                        maxItemCount -= count;
                        yield return new ArraySegment<TVal>(retBuf.Array, offset, count);

                        if (maxItemCount <= 0)
                            yield break;
                    }

//                    if (oneBlockOnly)
//                        yield break;
                }

                yield break;
            }
        }

        private bool TryGetIndex(long blockIndex, long cachedFileCount, out TInd index, bool inReverse = false)
        {
            index = default(TInd);

            if (cachedFileCount == 0)
                return false;

            long fileCountInBlocks = CalcBlockCount(cachedFileCount);

            if (blockIndex < 0)
            {
                if (inReverse)
                    return false;
                blockIndex = 0;
            }
            else if (blockIndex >= fileCountInBlocks)
            {
                if (!inReverse)
                    return false;
                blockIndex = fileCountInBlocks - 1;
            }

            if (_bufferByteProvider == null)
                _bufferByteProvider = new BufferProvider<byte>();

            // calc byte buffer size for one item or one block if going backwards
            int byteBufSize = inReverse
                                  ? GetBlockSize(blockIndex, cachedFileCount)
                                  : _maxItemByteSize + CodecBase.ReservedSpace;

            IEnumerable<Buffer<byte>> byteBuffs = _bufferByteProvider.YieldFixedSize(byteBufSize);

            long firstItemIdx = blockIndex*BlockSize + (inReverse ? byteBufSize - 1 : 0);

            foreach (var retBuf in
                (_bufferProvider ?? (_bufferProvider = new BufferProvider<TVal>()))
                    .YieldFixedSize(inReverse ? CalcMaxItemsInBlock(byteBufSize) : 1))
            {
                foreach (var seg in
                    // ReSharper disable PossibleMultipleEnumeration
                    PerformStreaming(firstItemIdx, inReverse, byteBuffs, cachedCount: cachedFileCount))
                    // ReSharper restore PossibleMultipleEnumeration
                {
                    // ignore suggested count, fill in as much as possible
                    retBuf.Count = 0;

                    if (seg.Count > BlockSize)
                        throw new InvalidOperationException(
                            "Logic error: seg.Count " + seg.Count + " > BlockSize " + BlockSize);

                    var codec = new CodecReader(seg);

                    // InReverse we always decode the whole block
                    FieldSerializer.DeSerialize(codec, retBuf, inReverse ? int.MaxValue : 1);

                    index = IndexAccessor(inReverse ? retBuf.Array[retBuf.Count - 1] : retBuf.Array[0]);
                    return true;
                }
            }

            throw new InvalidOperationException("Logic error: buffer provider did not yield one buffer");
        }

        private long Search(TInd index, long cachedFileCount)
        {
            long start = 0L;
            long blockCount = CalcBlockCount(cachedFileCount);
            long end = blockCount - 1;

            // empty file
            if (blockCount <= 0)
                return ~0;

            ConcurrentDictionary<long, TInd> cache = null;
            if (BinarySearchCacheSize >= 0)
            {
                cache = ResetOnChangedAndGetCache(blockCount, true);
                if (cache.Count > (BinarySearchCacheSize == 0 ? DefaultMaxBinaryCacheSize : BinarySearchCacheSize))
                    cache.Clear();
            }

            while (start <= end)
            {
                long mid = start + ((end - start) >> 1);
                TInd timeAtMid;

                // Read new value from file unless we already have it pre-cached in the dictionary
                if (cache == null || !cache.TryGetValue(mid, out timeAtMid))
                {
                    if (!TryGetIndex(mid, cachedFileCount, out timeAtMid))
                        throw new BinaryFileException("Unable to read index block #{0}", mid);

                    if (cache != null)
                        cache.TryAdd(mid, timeAtMid);
                }

                int comp = timeAtMid.CompareTo(index);
                if (comp == 0)
                {
                    if (UniqueIndexes)
                        return mid;

                    // In case when the exact index has been found and not forcing uniqueness,
                    // we must find the first of them in a row of equal indexes.
                    // To do that, we continue dividing until the last element.
                    if (start == mid)
                        return mid;
                    end = mid;
                }
                else if (comp < 0)
                    start = mid + 1;
                else
                    end = mid - 1;
            }

            return ~start;
        }

        public void TruncateFile(TInd firstIndexToErase)
        {
            long newCount = Search(firstIndexToErase, GetCount());
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
            if (bufferStream == null)
                throw new ArgumentNullException("bufferStream");

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
                    bool hasMore = FieldSerializer.Serialize(codec, iterator);
                    if (codec.Count == 0)
                        throw new SerializerException("Internal serializer error: buffer is empty");

                    yield return codec.UsedBuffer;

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

        private int GetBlockSize(long blockIndex, long cachedFileCount)
        {
            int byteBufSize = BlockSize;

            // if last block
            if (blockIndex == CalcBlockCount(cachedFileCount) - 1)
            {
                // adjust for last block which may be smaller
                var lastSize = (int) (cachedFileCount%BlockSize);
                if (lastSize > 0 && byteBufSize > lastSize)
                    byteBufSize = lastSize;
            }

            return byteBufSize;
        }

        private int CalcMaxItemsInBlock(int blockSize)
        {
            return 1 + (blockSize - _maxItemByteSize)/_minItemByteSize;
        }

        private long CalcBlockCount(long byteSize)
        {
            return FastBinFileUtils.RoundUpToMultiple(byteSize, BlockSize)/BlockSize;
        }
    }
}