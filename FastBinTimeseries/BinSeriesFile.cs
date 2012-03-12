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
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using JetBrains.Annotations;
using NYurik.FastBinTimeseries.EmitExtensions;
using NYurik.FastBinTimeseries.Serializers;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Helper non-generic class aids in creating a new instance of <see cref="BinSeriesFile{TInd,TVal}"/>.
    /// </summary>
    public static class BinSeriesFile
    {
        /// <summary>
        /// Uses reflection to create an instance of <see cref="BinSeriesFile{TInd,TVal}"/>.
        /// </summary>
        public static IWritableFeed GenericNew(
            Type indType, Type itemType, string fileName,
            FieldInfo indexFieldInfo = null)
        {
            return (IWritableFeed)
                   Activator.CreateInstance(
                       typeof (BinSeriesFile<,>).MakeGenericType(indType, itemType),
                       fileName, indexFieldInfo);
        }
    }

    /// <summary>
    /// Object representing a binary-serialized index-based series file.
    /// </summary>
    public class BinSeriesFile<TInd, TVal> : BinaryFile<TVal>, IWritableFeed<TInd, TVal>
        where TInd : IComparable<TInd>
    {
        private const int DefaultMaxBinaryCacheSize = 1 << 20;

        // ReSharper disable StaticFieldInGenericType
        private static readonly Version Version10 = new Version(1, 0);
        private static readonly Version Version11 = new Version(1, 1);
        // ReSharper restore StaticFieldInGenericType

        private TInd _firstIndex;
        private bool _hasFirstIndex;
        private bool _hasLastIndex;
        private FieldInfo _indexFieldInfo;
        private TInd _lastIndex;
        private Tuple<long, ConcurrentDictionary<long, TInd>> _searchCache;
        private bool _uniqueIndexes;

        #region Constructors

        /// <summary>
        /// Allow Activator non-public instantiation
        /// </summary>
        [UsedImplicitly]
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

        protected override Version Init(BinaryReader reader, Func<string, Type> typeResolver)
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

        #region IWritableFeed<TInd,TVal> Members

        TDst IGenericInvoker2.RunGenericMethod<TDst, TArg>(IGenericCallable2<TDst, TArg> callable, TArg arg)
        {
            return callable.Run<TInd, TVal>(this, arg);
        }

        public TInd FirstIndex
        {
            get
            {
                long count = GetCount();
                ResetOnChangedAndGetCache(count, false);

                if (!_hasFirstIndex && count > 0)
                {
                    ArraySegment<TVal> seg = PerformStreaming(0, false, maxItemCount: 1).FirstOrDefault();
                    if (seg.Count > 0)
                    {
                        _firstIndex = IndexAccessor(seg.Array[0]);
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
                long count = GetCount();
                ResetOnChangedAndGetCache(count, false);

                if (!_hasLastIndex && count > 0)
                {
                    ArraySegment<TVal> seg = PerformStreaming(count - 1, false, maxItemCount: 1).FirstOrDefault();
                    if (seg.Count > 0)
                    {
                        _lastIndex = IndexAccessor(seg.Array[0]);
                        _hasLastIndex = true;
                    }
                }

                if (_hasLastIndex)
                    return _lastIndex;
                return default(TInd);
            }
            private set
            {
                _lastIndex = value;
                _hasLastIndex = true;
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
            TInd fromInd = default(TInd), bool inReverse = false,
            IEnumerable<Buffer<TVal>> bufferProvider = null,
            long maxItemCount = Int64.MaxValue)
        {
            long start;
            if (!FastBinFileUtils.IsDefault(fromInd))
            {
                start = BinarySearch(fromInd, true);
                if (start < 0)
                    start = ~start;
                if (inReverse)
                    start--;
            }
            else
            {
                start = inReverse ? long.MaxValue : 0;
            }

            return PerformStreaming(start, inReverse, bufferProvider, maxItemCount);
        }

        public void AppendData(IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncation = false)
        {
            if (bufferStream == null)
                throw new ArgumentNullException("bufferStream");

            using (
                IEnumerator<ArraySegment<TVal>> streamEnmr =
                    ProcessWriteStream(bufferStream, allowFileTruncation).GetEnumerator())
                if (streamEnmr.MoveNext())
                    PerformWriteStreaming(streamEnmr);
        }

        #endregion

        protected long BinarySearch(TInd index)
        {
            if (!UniqueIndexes)
                throw new InvalidOperationException(
                    "This method call is only allowed for the unique index file. Use BinarySearch(TInd, bool) instead.");
            return BinarySearch(index, true);
        }

        protected long BinarySearch(TInd index, bool findFirst)
        {
            long start = 0L;
            long count = GetCount();
            long end = count - 1;

            // Optimize in case we search outside of the file
            if (count <= 0)
                return ~0;

            if (index.CompareTo(FirstIndex) < 0)
                return ~0;

            if (index.CompareTo(LastIndex) > 0)
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

            bool useMma = UseMemoryMappedAccess(1, false);

            while (start <= end)
            {
                long mid = start + ((end - start) >> 1);
                TInd indAtMid;
                TInd indAtMid2 = default(TInd);

                // Read new value from file unless we already have it pre-cached in the dictionary
                if (end - start == 1 && !UniqueIndexes && !findFirst)
                {
                    // for the special case where we are left with two elements,
                    // and searching for the last non-unique element,
                    // read both elements to see if the 2nd one matches our search
                    if (cache == null
                        || !cache.TryGetValue(mid, out indAtMid)
                        || !cache.TryGetValue(mid + 1, out indAtMid2))
                    {
                        if (PerformUnsafeBlockAccess(mid, false, new ArraySegment<TVal>(buff), count*ItemSize, useMma)
                            < 2)
                            throw new BinaryFileException("Unable to read two blocks");

                        indAtMid = IndexAccessor(buff[0]);
                        indAtMid2 = IndexAccessor(buff[1]);
                        if (cache != null)
                        {
                            cache.TryAdd(mid, indAtMid);
                            cache.TryAdd(mid + 1, indAtMid2);
                        }
                    }
                }
                else
                {
                    if (cache == null || !cache.TryGetValue(mid, out indAtMid))
                    {
                        if (PerformUnsafeBlockAccess(mid, false, oneElementSegment, count*ItemSize, useMma) < 1)
                            throw new BinaryFileException("Unable to read index block");
                        indAtMid = IndexAccessor(oneElementSegment.Array[0]);
                        if (cache != null)
                            cache.TryAdd(mid, indAtMid);
                    }
                }

                int comp = indAtMid.CompareTo(index);
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
                            return indAtMid2.CompareTo(index) == 0 ? mid + 1 : mid;

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

        /// <summary>
        /// Truncate file, making sure that no items exist with <paramref name="deleteOnAndAfter"/> index or greater.
        /// </summary>
        public void TruncateFile(TInd deleteOnAndAfter)
        {
            long newCount = BinarySearch(deleteOnAndAfter, true);
            if (newCount < 0) newCount = ~newCount;

            TruncateFile(newCount);
        }

        protected void TruncateFile(long newCount)
        {
            long fileCount = GetCount();
            if (newCount == fileCount)
                return;

            PerformTruncateFile(newCount);

            // Invalidate index
            if (newCount == 0)
                _hasFirstIndex = false;
            _hasLastIndex = false;
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

        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        private IEnumerable<ArraySegment<TVal>> ProcessWriteStream(
            [NotNull] IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncations)
        {
            bool isFirstSeg = true;

            bool isEmptyFile = IsEmpty;
            TInd prevSegLast = isEmptyFile ? default(TInd) : LastIndex;
            int segInd = 0;

            foreach (var buffer in bufferStream)
            {
                if (buffer.Array == null)
                    throw new SerializationException("BufferStream may not contain ArraySegments with null Array");
                if (buffer.Count == 0)
                    continue;

                // Validate new data
                Tuple<TInd, TInd> rng = ValidateSegmentOrder(
                    buffer, IndexAccessor, UniqueIndexes, segInd);

                if (!isEmptyFile)
                {
                    // Make sure new data's first index is after the last item in file or previous segment
                    int cmp = rng.Item1.CompareTo(prevSegLast);
                    bool needTruncation = cmp < 0 || (cmp == 0 && (UniqueIndexes || allowFileTruncations));

                    if (needTruncation)
                    {
                        if (!allowFileTruncations)
                            if (cmp < 0)
                                throw new BinaryFileException(
                                    "Last index in {2} ({0}) is greater than the first new item's index ({1})",
                                    prevSegLast, rng.Item1, isFirstSeg ? "file" : "segment");
                            else
                                throw new BinaryFileException(
                                    "Last index in {1} ({0}) equals to the first new item's index (enfocing uniqueness)",
                                    prevSegLast, isFirstSeg ? "file" : "segment");

                        TruncateFile(rng.Item1);
                    }
                }

                yield return buffer;

                if (isEmptyFile)
                    FirstIndex = rng.Item1;
                prevSegLast = rng.Item2;
                LastIndex = rng.Item2;
                isEmptyFile = false;
                isFirstSeg = false;
                allowFileTruncations = false;
                segInd++;
            }
        }

        private static Tuple<TInd, TInd> ValidateSegmentOrder(
            ArraySegment<TVal> buffer, Func<TVal, TInd> indAccessor, bool uniqueIndexes, int segInd)
        {
            int lastOffset = buffer.Offset + buffer.Count;

            TVal[] data = buffer.Array;
            TInd firstInd = indAccessor(data[buffer.Offset]);
            TInd lastInd = firstInd;

            for (int i = buffer.Offset + 1; i < lastOffset; i++)
            {
                TInd newInd = indAccessor(data[i]);
                int cmp = newInd.CompareTo(lastInd);

                if (cmp < 0)
                    throw new BinaryFileException(
                        "Segment {4}, new item's index at #{0} ({1}) is greater than index of the following item #{2} ({3})",
                        i - 1, lastInd, i, newInd, segInd);
                if (uniqueIndexes && cmp == 0)
                    throw new BinaryFileException(
                        "Segment {4} new item's index at #{0} ({1}) equals the index of the following item #{2} (enforcing uniqueness)",
                        i - 1, lastInd, i, segInd);

                lastInd = newInd;
            }

            return Tuple.Create(firstInd, lastInd);
        }
    }
}