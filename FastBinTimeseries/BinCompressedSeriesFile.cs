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
                    if (TryGetValue(StreamSegments(0, cachedFileCount, maxItemCount: 1), out tmp))
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
                    if (TryGetValue(
                        StreamSegments(long.MaxValue, cachedFileCount, inReverse: true, maxItemCount: 1), out tmp, true))
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

        public IEnumerable<Buffer<TVal>> StreamSegments(TInd fromInd, bool inReverse = false,
                                                        IEnumerable<Buffer<TVal>> bufferProvider = null,
                                                        long maxItemCount = long.MaxValue)
        {
            long cachedFileCount = GetCount();

            long start = Search(fromInd, cachedFileCount);
            if (start < 0)
                start = ~start - 1;
            else if (!UniqueIndexes)
                start--;

            return StreamSegments(start, cachedFileCount, inReverse, bufferProvider, maxItemCount);
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

        private bool TryGetValue(IEnumerable<Buffer<TVal>> stream, out TInd value, bool inReverse = false)
        {
            foreach (var b in stream)
            {
                value = IndexAccessor(inReverse ? b.Array[b.Count - 1] : b.Array[0]);
                return true;
            }

            value = default(TInd);
            return false;
        }

        private IEnumerable<Buffer<TVal>> StreamSegments(long blockIndex, long fileCount, bool inReverse = false,
                                                         IEnumerable<Buffer<TVal>> bufferProvider = null,
                                                         long maxItemCount = long.MaxValue,
                                                         bool blockInitValOnly = false)
        {
            if (fileCount == 0)
                yield break;

            long fileCountInBlocks = FastBinFileUtils.RoundUpToMultiple(fileCount, BlockSize)/BlockSize;

            if (blockIndex < 0)
            {
                if (inReverse)
                    yield break;
                blockIndex = 0;
            }
            else if (blockIndex >= fileCountInBlocks)
            {
                if (!inReverse)
                    yield break;
                blockIndex = fileCountInBlocks - 1;
            }

            if (_bufferByteProvider == null)
                _bufferByteProvider = new BufferProvider<byte>();

            int smallSize = FastBinFileUtils.RoundUpToMultiple(MinPageSize, BlockSize);
            int largeSize = FastBinFileUtils.RoundUpToMultiple(MaxLargePageSize/16, BlockSize);

            int firstBlockSize;
            if (maxItemCount <= BlockSize / _maxItemByteSize)
            {
                if (inReverse)
                    firstBlockSize = BlockSize;
                else
                    firstBlockSize = (int) (maxItemCount*_maxItemByteSize);
            } 
            else
                firstBlockSize = smallSize;
            
            if (blockIndex == fileCountInBlocks - 1)
            {
                var lastFileBlockSize = (int) (fileCount%BlockSize);
                if (lastFileBlockSize == 0)
                    lastFileBlockSize = BlockSize;

                if (firstBlockSize > lastFileBlockSize)
                    firstBlockSize = lastFileBlockSize;
            }

            IEnumerable<Buffer<byte>> byteBuffs = _bufferByteProvider.YieldFixed(
                firstBlockSize, smallSize, 4, largeSize);

            long firstItemIdx = blockIndex*BlockSize + (inReverse ? firstBlockSize - 1 : 0);

            CodecReader codec = null;
            IEnumerator<Buffer<TVal>> valBufs = null;
            try
            {
                foreach (var seg in PerformStreaming(firstItemIdx, inReverse, byteBuffs, cachedCount: fileCount))
                {
                    valBufs =
                        (bufferProvider
                         ?? (_bufferProvider ?? (_bufferProvider = new BufferProvider<TVal>()))
                                .YieldMaxGrowingBuffer(
                                    maxItemCount, MinPageSize/ItemSize, 5, MaxLargePageSize/ItemSize))
                            .GetEnumerator();

                    if (!valBufs.MoveNext())
                        yield break;

                    Buffer<TVal> retBuf = valBufs.Current;

                    if (codec == null)
                        codec = new CodecReader(seg);
                    else
                        codec.AttachBuffer(seg);

                    // ignore suggested count, fill in as much as possible
                    retBuf.Count = 0;

                    long blocks = FastBinFileUtils.RoundUpToMultiple(seg.Count, BlockSize)/BlockSize;
                    for (int i = 0; i < blocks; i++)
                    {
                        codec.BufferPos = i*BlockSize;

                        int max;
                        if (!inReverse)
                        {
                            long left = maxItemCount - retBuf.Count;
                            max = left > int.MaxValue ? int.MaxValue : (int) left;
                            if (max <= 0)
                                break;
                        }
                        else
                        {
                            // on reverse we must decode the whole seg to preserve the order
                            max = int.MaxValue;
                        }

                        // InReverse we always decode the whole block
                        FieldSerializer.DeSerialize(codec, retBuf, max);
                    }

                    maxItemCount -= retBuf.Count;
                    yield return retBuf;

                    if (maxItemCount <= 0)
                        yield break;
                }
            }
            finally
            {
                if (valBufs != null)
                    valBufs.Dispose();
            }
        }

        private long Search(TInd index, long cachedFileCount)
        {
            long start = 0L;
            long blockCount = FastBinFileUtils.RoundUpToMultiple(cachedFileCount, BlockSize)/BlockSize;
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
                    if (!TryGetValue(StreamSegments(mid, cachedFileCount, false, maxItemCount: 1), out timeAtMid))
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
    }
}