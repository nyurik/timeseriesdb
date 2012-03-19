#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of TimeSeriesDb library
 * 
 *  TimeSeriesDb is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  TimeSeriesDb is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with TimeSeriesDb.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using JetBrains.Annotations;
using NYurik.TimeSeriesDb.Common;
using NYurik.TimeSeriesDb.Serializers;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

namespace NYurik.TimeSeriesDb
{
    /// <summary>
    ///   Helper non-generic class aids in creating a new instance of <see cref="BinCompressedSeriesFile{TInd,TVal}" /> .
    /// </summary>
    public static class BinCompressedSeriesFile
    {
        /// <summary>
        ///   Uses reflection to create an instance of <see cref="BinCompressedSeriesFile{TInd,TVal}" /> .
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
    ///   Object representing a binary-serialized long-based series file.
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

        private CachedIndex<TInd> _searchCache;
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

        /// <summary>
        /// Number of binary search lookups to cache. 0-internal defaults, negative-disable
        /// </summary>
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

        private CachedIndex<TInd> SearchCache
        {
            get
            {
                if (_searchCache == null)
                {
                    ThrowOnNotInitialized();

                    _searchCache = new CachedIndex<TInd>(
                        DefaultMaxBinaryCacheSize, () => BinarySearchCacheSize, GetCount, GetIndex,
                        cnt =>
                            {
                                _hasFirstIndex = false;
                                _hasLastIndex = false;
                            });
                }

                return _searchCache;
            }
        }

        public override long Count
        {
            get { throw new NotSupportedException("Count is not available for compressed files"); }
        }

        public DynamicSerializer<TVal> FieldSerializer
        {
            get { return _serializer; }
        }

        #region Constructors

        /// <summary>
        ///   Allow Activator non-public instantiation
        /// </summary>
        [UsedImplicitly]
        protected BinCompressedSeriesFile()
        {
        }

        /// <summary>
        ///   Create new timeseries file. If the file already exists, an <see cref="IOException" /> is thrown.
        /// </summary>
        /// <param name="fileName"> A relative or absolute path for the file to create. </param>
        /// <param name="indexFieldInfo"> Field containing the TInd index, or null to get default </param>
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

            FieldInfo fieldInfo = typeof (TVal).GetField(fieldName, TypeUtils.AllInstanceMembers);
            if (fieldInfo == null)
                throw new BinaryFileException(
                    "Index field {0} was not found in type {1}", fieldName, typeof (TVal).FullName);

            IndexFieldInfo = fieldInfo;

            _serializer = DynamicSerializer<TVal>.CreateFromReader(reader, typeResolver);
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

        #region IWritableFeed<TInd,TVal> Members

        public override bool IsEmpty
        {
            get { return SearchCache.Count == 0; }
        }

        TDst IGenericInvoker2.RunGenericMethod<TDst, TArg>(IGenericCallable2<TDst, TArg> callable, TArg arg)
        {
            return callable.Run<TInd, TVal>(this, arg);
        }

        public TInd FirstIndex
        {
            get { return GetFirstIndex(SearchCache.Count); }
            private set
            {
                _firstIndex = value;
                _hasFirstIndex = true;
            }
        }

        public TInd LastIndex
        {
            get { return GetLastIndex(SearchCache.Count); }
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
        ///   A delegate to a function that extracts index of a given item
        /// </summary>
        public Func<TVal, TInd> IndexAccessor { get; private set; }

        public IEnumerable<ArraySegment<TVal>> StreamSegments(
            TInd fromInd = default(TInd), bool inReverse = false,
            IEnumerable<Buffer<TVal>> bufferProvider = null,
            long maxItemCount = long.MaxValue)
        {
            long cachedFileCount = SearchCache.Count;

            long block = GetBlockByIndex(fromInd, inReverse, cachedFileCount);

            return StreamSegments(fromInd, block, cachedFileCount, inReverse, bufferProvider, maxItemCount);
        }

        /// <summary>
        ///   Add new items at the end of the existing file
        /// </summary>
        public void AppendData(IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncation = false)
        {
            if (bufferStream == null)
                throw new ArgumentNullException("bufferStream");

            long count = SearchCache.Count;
            bool isEmptyFile = count == 0;

            using (IEnumerator<TVal> newValues = VerifyValues(bufferStream).GetEnumerator())
            {
                if (!newValues.MoveNext())
                    return;

                TInd firstNewItemInd = IndexAccessor(newValues.Current);

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
                            int cmp = firstNewItemInd.CompareTo(lastInd);
                            if (cmp < 0)
                                throw new BinaryFileException(
                                    "Last index in file ({0}) is greater than the first new item's index ({1})",
                                    lastInd, firstNewItemInd);
                            else if (cmp == 0 && UniqueIndexes)
                                throw new BinaryFileException(
                                    "Last index in file ({0}) equals to the first new item's index (enfocing uniqueness)",
                                    lastInd);

                            // Round down so that if we have incomplete block, it will be appended.
                            firstBufferBlock = Utils.RoundDownToMultiple(count, BlockSize)/BlockSize;
                        }
                        else
                        {
                            firstBufferBlock = GetBlockByIndex(firstNewItemInd, false, count);
                        }

                        if (firstBufferBlock >= 0)
                        {
                            // start at the begining of the found block
                            iterToDispose = JoinStreams(
                                firstNewItemInd, newValues, !UniqueIndexes && !allowFileTruncation,
                                StreamSegments(default(TInd), firstBufferBlock, count).Stream()
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
                        FirstIndex = firstNewItemInd;
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

        private TInd GetFirstIndex(long count)
        {
            return GetFirstLastIndex(count, true, ref _hasFirstIndex, ref _firstIndex);
        }

        private TInd GetLastIndex(long count)
        {
            return GetFirstLastIndex(count, false, ref _hasLastIndex, ref _lastIndex);
        }

        private TInd GetFirstLastIndex(long count, bool isFirst, ref bool hasIndex, ref TInd index)
        {
            if (!hasIndex && count > 0)
            {
                if (isFirst)
                    index = SearchCache.GetValueAt(0);
                else
                {
                    long block = GetBlockByIndex(default(TInd), true, count);
                    ArraySegment<TVal> res = StreamSegments(default(TInd), block, count, true, null, 1).FirstOrDefault();
                    if (res.Array == null || res.Count != 1)
                        throw new BinaryFileException("Unable to read last value");
                    index = IndexAccessor(res.Array[res.Offset]);
                }
                hasIndex = true;
            }

            return hasIndex ? index : default(TInd);
        }

        private IEnumerable<ArraySegment<TVal>> StreamSegments(
            TInd firstInd, long firstBlockInd, long cachedFileCount,
            bool inReverse = false,
            IEnumerable<Buffer<TVal>> bufferProvider = null,
            long maxItemCount = long.MaxValue)
        {
            if (maxItemCount < 0)
                throw new ArgumentOutOfRangeException("maxItemCount", maxItemCount, "<0");

            if (cachedFileCount <= 0 || maxItemCount == 0)
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

            bool getFullBlock = Utils.IsDefault(firstInd);
            int firstBlockSize = GetBlockSize(firstBlockInd, cachedFileCount);
            int smallSize = Utils.RoundUpToMultiple(MinPageSize, BlockSize);
            int largeSize = Utils.RoundUpToMultiple(MaxLargePageSize/16, BlockSize);

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
                    foreach (var seg in 
                        PerformStreaming(firstItemIdx, inReverse, byteBuffs, cachedCount: cachedFileCount))
                    {
                        if (codec == null)
                            codec = new CodecReader(seg);
                        else
                            codec.AttachBuffer(seg);

                        // ignore suggested count, fill in as much as possible
                        retBuf.Count = 0;

                        // todo: might optimize here for forward iteration with the given maxItemCount

                        long blocks = CalcBlockCount(seg.Count);
                        for (int i = 0; i < blocks; i++)
                        {
                            codec.BufferPos = i*BlockSize;
                            FieldSerializer.DeSerialize(codec, retBuf, int.MaxValue);
                            if (ValidateOnRead)
                                codec.Validate(BlockSize);
                        }

                        var res = inReverse ? retBuf.AsArraySegmentReversed() : retBuf.AsArraySegment();

                        int offset =
                            getFullBlock
                                ? ~0
                                : (int) Utils.BinarySearch(
                                    firstInd, res.Offset, res.Count, UniqueIndexes, inReverse,
                                    p => IndexAccessor(res.Array[p]));

                        if (offset < 0)
                            offset = ~offset;
                        else if (inReverse)
                            offset++;

                        getFullBlock = true;

                        int count = res.Count - offset;
                        if (count > maxItemCount)
                            count = (int) maxItemCount;

                        if (count > 0)
                        {
                            maxItemCount -= count;
                            yield return new ArraySegment<TVal>(res.Array, offset, count);

                            if (maxItemCount <= 0)
                                yield break;
                        }
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

        private TInd GetIndex(long blockIndex, long cachedFileCount)
        {
            if (cachedFileCount == 0)
                throw new BinaryFileException();

            long fileCountInBlocks = CalcBlockCount(cachedFileCount);

            if (blockIndex < 0)
                blockIndex = 0;
            else if (blockIndex >= fileCountInBlocks)
                throw new BinaryFileException();

            // calc byte buffer size for one item or one block if going backwards
            int byteBufSize = _maxItemByteSize + CodecBase.ReservedSpace;

            IEnumerable<Buffer<byte>> byteBuffs = _bufferByteProvider.YieldSingleFixedSize(byteBufSize);

            long firstItemIdx = blockIndex*BlockSize + 0;

            foreach (var retBuf in
                (_bufferProvider ?? (_bufferProvider = new BufferProvider<TVal>()))
                    .YieldSingleFixedSize(1))
            {
                foreach (var seg in
                    // ReSharper disable PossibleMultipleEnumeration
                    PerformStreaming(firstItemIdx, false, byteBuffs, cachedCount: cachedFileCount))
                    // ReSharper restore PossibleMultipleEnumeration
                {
                    // ignore suggested count, fill in as much as possible
                    retBuf.Count = 0;

                    if (seg.Count > BlockSize)
                        throw new InvalidOperationException(
                            "Logic error: seg.Count " + seg.Count + " > BlockSize " + BlockSize);

                    using (var codec = new CodecReader(seg))
                        FieldSerializer.DeSerialize(codec, retBuf, 1);

                    return IndexAccessor(retBuf.Array[0]);
                }
            }

            // Logic error: buffer provider did not yield any buffers
            throw new InvalidOperationException();
        }

        /// <summary>
        /// Using binary search, find the block that would contain needed index.
        /// Returns -1 if index is before the first item.
        /// </summary>
        private long GetBlockByIndex(TInd index, bool inReverse, long cachedFileCount)
        {
            long blockCount = CalcBlockCount(cachedFileCount);

            if (blockCount == 0)
                return -1;

            if (Utils.IsDefault(index))
            {
                if (inReverse) // Start from the last block
                    return blockCount - 1;

                // When appending, it is possible for the first item in file to have index == default, so use merge.
                return 0;
            }

            long block = Utils.BinarySearch(
                index, 0, blockCount, UniqueIndexes, false, SearchCache.GetValueAt);

            if (block < 0)
                block = ~block - 1;
            else if (!UniqueIndexes)
                block--;
            return block;
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
                        var val = v.Array[i];
                        // ReSharper disable CompareNonConstrainedGenericWithNull
                        if (val == null)
                            throw new BinaryFileException("Segment {0}, item #{1} is null", segInd, i);

                        TInd newInd = IndexAccessor(val);
                        if (newInd == null)
                        {
                            throw new BinaryFileException(
                                "Segment {0}, item #{1} has an index field of type {2} set to null",
                                segInd, i, typeof (TInd).FullName);
                        }
                        // ReSharper restore CompareNonConstrainedGenericWithNull

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

                        yield return val;
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
                    return lastSize;
            }

            return byteBufSize;
        }

        private long CalcBlockCount(long byteSize)
        {
            return Utils.RoundUpToMultiple(byteSize, BlockSize)/BlockSize;
        }
    }
}