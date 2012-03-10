#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of FastBinTimeseries library
 * 
 *  FastBinTimeseries is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  FastBinTimeseries is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with FastBinTimeseries.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.EmitExtensions;
using NYurik.FastBinTimeseries.Serializers;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Helper non-generic class aids in creating a new instance of <see cref="BinCompressedSeriesFile{TInd,TVal}"/>.
    /// </summary>
    public static class BinCompressedSeriesFile
    {
        /// <summary>
        /// Uses reflection to create an instance of <see cref="BinCompressedSeriesFile{TInd,TVal}"/>.
        /// </summary>
        public static IWritableFeed GenericNew(
            Type indType, Type itemType, string fileName,
            FieldInfo indexFieldInfo = null)
        {
            return (IWritableFeed)
                   Activator.CreateInstance(
                       typeof (BinCompressedSeriesFile<,>).MakeGenericType(indType, itemType),
                       fileName, indexFieldInfo);
        }
    }

    /// <summary>
    /// Object representing a binary-serialized long-based series file.
    /// </summary>
    public class BinCompressedSeriesFile<TInd, TVal> : BinaryFile<byte>, IWritableFeed<TInd, TVal>
        where TInd : IComparable<TInd>
    {
        private const int DefaultMaxBinaryCacheSize = 1 << 20;

        // ReSharper disable StaticFieldInGenericType
        private static readonly Version Version10 = new Version(1, 0);
        // ReSharper restore StaticFieldInGenericType

        private readonly BufferProvider<byte> _bufferByteProvider = new BufferProvider<byte>();
        private int _blockSize;
        private BufferProvider<TVal> _bufferProvider;
        private TInd _firstIndex;
        private bool _hasFirstIndex;
        private bool _hasLastIndex;
        private FieldInfo _indexFieldInfo;
        private TInd _lastIndex;
        private int _maxItemByteSize;
        private int _minItemByteSize;

        private Tuple<long, ConcurrentDictionary<long, TInd>> _searchCache;
        private DynamicSerializer<TVal> _serializer;
        private bool _uniqueIndexes;

        public bool ValidateOnRead { get; set; }

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

        protected override Version Init(BinaryReader reader, Func<string, Type> typeResolver)
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

            _serializer = DynamicSerializer<TVal>.CreateFromReader(reader, typeResolver);
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

        #region IWritableFeed<TInd,TVal> Members

        TDst IGenericInvoker2.RunGenericMethod<TDst, TArg>(IGenericCallable2<TDst, TArg> callable, TArg arg)
        {
            return callable.Run<TInd, TVal>(this, arg);
        }

        public TInd FirstIndex
        {
            get
            {
                long cachedFileCount = GetCount();
                ResetOnChangedAndGetCache(cachedFileCount, false);

                if (!_hasFirstIndex && cachedFileCount > 0)
                {
                    TInd tmp;
                    if (TryGetIndex(0, cachedFileCount, out tmp))
                    {
                        _firstIndex = tmp;
                        _hasFirstIndex = true;
                    }
                }

                if (_hasFirstIndex)
                    return _firstIndex;
                return default(TInd);
            }
            private set
            {
                _firstIndex = value;
                _hasFirstIndex = true;
            }
        }

        public TInd LastIndex
        {
            get
            {
                long cachedFileCount = GetCount();
                ResetOnChangedAndGetCache(cachedFileCount, false);

                if (!_hasLastIndex && cachedFileCount > 0)
                {
                    TInd tmp;
                    if (TryGetIndex(long.MaxValue, cachedFileCount, out tmp, inReverse: true))
                    {
                        _lastIndex = tmp;
                        _hasLastIndex = true;
                    }
                }

                if (_hasLastIndex)
                    return _lastIndex;
                return default(TInd);
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

        /// <summary>
        /// A delegate to a function that extracts index of a given item
        /// </summary>
        public Func<TVal, TInd> IndexAccessor { get; private set; }

        public IEnumerable<ArraySegment<TVal>> StreamSegments(
            TInd fromInd, bool inReverse = false,
            IEnumerable<Buffer<TVal>> bufferProvider = null,
            long maxItemCount = long.MaxValue)
        {
            long cachedFileCount = GetCount();

            long block = GetBlockByIndex(fromInd, cachedFileCount);

            return StreamSegments(fromInd, block, cachedFileCount, inReverse, bufferProvider, maxItemCount);
        }

        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        public void AppendData(IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncation = false)
        {
            if (bufferStream == null)
                throw new ArgumentNullException("bufferStream");

            long count = GetCount();
            bool isEmptyFile = count == 0;

            using (IEnumerator<TVal> newValues = VerifyValues(bufferStream).GetEnumerator())
            {
                if (!newValues.MoveNext())
                    return;

                TInd firstBufferInd = IndexAccessor(newValues.Current);

                IEnumerator<TVal> iterToDispose = null;
                try
                {
                    IEnumerator<TVal> mergedIter = newValues;
                    long firstBufferBlock = 0;

                    if (!isEmptyFile)
                    {
                        if (!allowFileTruncation)
                        {
                            TInd lastInd = LastIndex;
                            int cmp = firstBufferInd.CompareTo(lastInd);
                            if (cmp < 0)
                                throw new BinaryFileException(
                                    "Last index in file ({0}) is greater than the first new item's index ({1})",
                                    lastInd, firstBufferInd);
                            else if (cmp == 0 && UniqueIndexes)
                                throw new BinaryFileException(
                                    "Last index in file ({0}) equals to the first new item's index (enfocing uniqueness)",
                                    lastInd);

                            // Round down so that if we have incomplete block, it will be appended.
                            firstBufferBlock = FastBinFileUtils.RoundDownToMultiple(count, BlockSize)/BlockSize;
                        }
                        else
                        {
                            firstBufferBlock = GetBlockByIndex(firstBufferInd, count);
                        }

                        if (firstBufferBlock >= 0)
                        {
                            // start at the begining of the found block
                            iterToDispose = JoinStreams(
                                firstBufferInd, newValues, !UniqueIndexes && !allowFileTruncation,
                                StreamSegments(default(TInd), firstBufferBlock, count).StreamSegmentValues()
                                ).GetEnumerator();

                            mergedIter = iterToDispose;
                            if (!mergedIter.MoveNext())
                                throw new InvalidOperationException(
                                    "Logic error: mergedIter must have at least one value");
                        }
                        else
                        {
                            // Re-writing the whole file using new values
                            firstBufferBlock = 0;
                        }
                    }

                    using (IEnumerator<ArraySegment<byte>> mergedEnmr = SerializeStream(mergedIter).GetEnumerator())
                    {
                        if (mergedEnmr.MoveNext())
                            PerformWriteStreaming(mergedEnmr, firstBufferBlock*BlockSize);
                    }

                    if (isEmptyFile)
                        FirstIndex = firstBufferInd;
                    _hasLastIndex = false;
                }
                finally
                {
                    // newValues is disposed as part of using() statement
                    if (iterToDispose != null)
                        iterToDispose.Dispose();
                }
            }
        }

        #endregion

        private IEnumerable<ArraySegment<TVal>> StreamSegments(
            TInd firstInd, long firstBlockInd, long cachedFileCount,
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

            int firstBlockSize = GetBlockSize(firstBlockInd, cachedFileCount);
            int smallSize = FastBinFileUtils.RoundUpToMultiple(MinPageSize, BlockSize);
            int largeSize = FastBinFileUtils.RoundUpToMultiple(MaxLargePageSize/16, BlockSize);

            IEnumerable<Buffer<byte>> byteBuffs = _bufferByteProvider.YieldFixed(
                firstBlockSize, BlockSize, smallSize, 4, largeSize);

            long firstItemIdx = firstBlockInd*BlockSize + (inReverse ? firstBlockSize - 1 : 0);

            CodecReader codec = null;
            try
            {
                foreach (var retBuf in
                    (bufferProvider
                     ?? (_bufferProvider ?? (_bufferProvider = new BufferProvider<TVal>()))
                            .YieldMaxGrowingBuffer(
                                maxItemCount, MinPageSize/ItemSize, 5, MaxLargePageSize/ItemSize)))
                {
                    foreach (
                        var seg in PerformStreaming(firstItemIdx, inReverse, byteBuffs, cachedCount: cachedFileCount))
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
                            if (ValidateOnRead)
                                codec.Validate(BlockSize);
                        }

                        // todo: search should only be performed on first iteration
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
            finally
            {
                if (codec != null)
                    codec.Dispose();
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

            // calc byte buffer size for one item or one block if going backwards
            int byteBufSize = inReverse
                                  ? GetBlockSize(blockIndex, cachedFileCount)
                                  : _maxItemByteSize + CodecBase.ReservedSpace;

            IEnumerable<Buffer<byte>> byteBuffs = _bufferByteProvider.YieldSingleFixedSize(byteBufSize);

            long firstItemIdx = blockIndex*BlockSize + (inReverse ? byteBufSize - 1 : 0);

            foreach (var retBuf in
                (_bufferProvider ?? (_bufferProvider = new BufferProvider<TVal>()))
                    .YieldSingleFixedSize(inReverse ? CalcMaxItemsInBlock(byteBufSize) : 1))
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

                    using (var codec = new CodecReader(seg))
                        FieldSerializer.DeSerialize(codec, retBuf, inReverse ? int.MaxValue : 1);

                    index = IndexAccessor(inReverse ? retBuf.Array[retBuf.Count - 1] : retBuf.Array[0]);
                    return true;
                }
            }

            // Logic error: buffer provider did not yield any buffers
            throw new InvalidOperationException();
        }

        /// <summary>
        /// Using binary search, find the block that would contain needed index.
        /// Returns -1 if index is before the first item.
        /// </summary>
        private long GetBlockByIndex(TInd index, long cachedFileCount)
        {
            long block = Search(index, cachedFileCount);
            if (block < 0)
                block = ~block - 1;
            else if (!UniqueIndexes)
                block--;
            return block;
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
                TInd indAtMid;

                // Read new value from file unless we already have it pre-cached in the dictionary
                if (cache == null || !cache.TryGetValue(mid, out indAtMid))
                {
                    if (!TryGetIndex(mid, cachedFileCount, out indAtMid))
                        throw new BinaryFileException("Unable to read index block #{0}", mid);

                    if (cache != null)
                        cache.TryAdd(mid, indAtMid);
                }

                int comp = indAtMid.CompareTo(index);
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
                        _hasFirstIndex = false;
                        _hasLastIndex = false;
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

        private IEnumerable<ArraySegment<byte>> SerializeStream(IEnumerator<TVal> mergedIter)
        {
            using (var codec = new CodecWriter(BlockSize))
            {
                while (true)
                {
                    codec.Count = 0;
                    bool hasMore = FieldSerializer.Serialize(codec, mergedIter);
                    if (codec.Count == 0)
                        throw new SerializerException("Internal serializer error: buffer is empty");

                    yield return codec.AsArraySegment();

                    if (!hasMore)
                        break;
                }
            }
        }

        /// <summary>
        /// Slightly hacky merge of an old values enumerable with an iterator over the new ones.
        /// This code assumes that it will be called only once for this IEnumerator.
        /// If includeEquals = true, includes old items with the index that equals to the first new one.
        /// </summary>
        private IEnumerable<TVal> JoinStreams(
            TInd firstNewInd, IEnumerator<TVal> newValues, bool includeEquals, IEnumerable<TVal> existingValues)
        {
            Func<TVal, bool> comp;
            if (includeEquals)
                comp = val => IndexAccessor(val).CompareTo(firstNewInd) <= 0;
            else
                comp = val => IndexAccessor(val).CompareTo(firstNewInd) < 0;

            foreach (TVal v in existingValues)
            {
                if (comp(v))
                    yield return v;
                else
                    break;
            }

            do
            {
                yield return newValues.Current;
            } while (newValues.MoveNext());
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
                            int cmp = newInd.CompareTo(lastInd);
                            if (cmp < 0)
                                throw new BinaryFileException(
                                    "Segment {0}, last item's index {1} is greater than index of the following item #{2} ({3})",
                                    segInd, lastInd, i, newInd);

                            if (UniqueIndexes && cmp == 0)
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
            return blockSize/_minItemByteSize;
        }

        private long CalcBlockCount(long byteSize)
        {
            return FastBinFileUtils.RoundUpToMultiple(byteSize, BlockSize)/BlockSize;
        }
    }
}