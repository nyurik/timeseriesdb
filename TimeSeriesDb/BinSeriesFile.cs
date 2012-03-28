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
using System.Runtime.Serialization;
using JetBrains.Annotations;
using NYurik.TimeSeriesDb.Common;
using NYurik.TimeSeriesDb.Serializers;

namespace NYurik.TimeSeriesDb
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
        private CachedIndex<TInd> _searchCache;
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

            FieldInfo fieldInfo = typeof (TVal).GetField(fieldName, TypeUtils.AllInstanceMembers);
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

        private CachedIndex<TInd> SearchCache
        {
            get
            {
                if (_searchCache == null)
                {
                    ThrowOnNotInitialized();

                    var buff = new TVal[1];
                    var oneElementSegment = new ArraySegment<TVal>(buff);
                    int itemSize = ItemSize;

                    _searchCache = new CachedIndex<TInd>(
                        DefaultMaxBinaryCacheSize, () => BinarySearchCacheSize, GetCount,
                        (ind, cnt) =>
                            {
                                if (PerformUnsafeBlockAccess(ind, false, oneElementSegment, cnt*itemSize, false) < 1)
                                    throw new BinaryFileException("Unable to read index block");
                                return IndexAccessor(oneElementSegment.Array[0]);
                            },
                        cnt =>
                            {
                                _hasFirstIndex = false;
                                _hasLastIndex = false;
                            });
                }

                return _searchCache;
            }
        }

        #region IWritableFeed<TInd,TVal> Members

        public override bool IsEmpty
        {
            get { return SearchCache.Count == 0; }
        }

        TDst IGenericInvoker2.RunGenericMethod<TDst, TArg>(IGenericCallable2<TDst, TArg> callable, TArg arg)
        {
            if (callable == null) throw new ArgumentNullException("callable");
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
            FeedUtils.AssertPositiveIndex(fromInd);

            long start;
            if (!FeedUtils.IsDefault(fromInd))
            {
                start = BinarySearch(fromInd);
                if (start < 0)
                    start = ~start;
                if (inReverse)
                    start--;
            }
            else
            {
                start = inReverse ? long.MaxValue : 0;
            }

            IEnumerable<ArraySegment<TVal>> stream = PerformStreaming(start, inReverse, bufferProvider, maxItemCount);

            return inReverse
                       ? stream.Select(
                           seg =>
                               {
                                   Array.Reverse(seg.Array, seg.Offset, seg.Count);
                                   return seg;
                               })
                       : stream;
        }

        public void AppendData(IEnumerable<ArraySegment<TVal>> newData, bool allowFileTruncation = false)
        {
            if (newData == null)
                throw new ArgumentNullException("newData");

            using (IEnumerator<ArraySegment<TVal>> streamEnmr =
                ProcessWriteStream(newData, allowFileTruncation)
                    .GetEnumerator())
            {
                if (streamEnmr.MoveNext())
                    PerformWriteStreaming(streamEnmr);
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
                index = SearchCache.GetValueAt(isFirst ? 0 : count - 1);
                hasIndex = true;
            }

            return hasIndex ? index : default(TInd);
        }

        /// <summary>
        /// Search for the first occurence of the index in the file
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        protected long BinarySearch(TInd value)
        {
            long count = SearchCache.Count;

            // Optimize in case we search outside of the file
            if (count <= 0)
                return ~0;

            if (value.CompareTo(GetFirstIndex(count)) < 0)
                return ~0;

            if (value.CompareTo(GetLastIndex(count)) > 0)
                return ~count;

            return Utils.BinarySearch(value, 0L, count, UniqueIndexes, false, SearchCache.GetValueAt);
        }

        /// <summary>
        /// Truncate file, making sure that no items exist with <paramref name="deleteOnAndAfter"/> index or greater.
        /// </summary>
        public void TruncateFile(TInd deleteOnAndAfter)
        {
            long newCount = BinarySearch(deleteOnAndAfter);
            if (newCount < 0) newCount = ~newCount;

            TruncateFile(newCount);
        }

        protected void TruncateFile(long newCount)
        {
            long fileCount = SearchCache.Count;
            if (newCount == fileCount)
                return;

            PerformTruncateFile(newCount);

            // Invalidate index
            if (newCount == 0)
                _hasFirstIndex = false;
            _hasLastIndex = false;
        }

        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        private IEnumerable<ArraySegment<TVal>> ProcessWriteStream(
            [NotNull] IEnumerable<ArraySegment<TVal>> newData, bool allowFileTruncations)
        {
            bool isFirstSeg = true;

            bool isEmptyFile = IsEmpty;
            TInd prevSegLast = isEmptyFile ? default(TInd) : LastIndex;
            int segInd = 0;

            foreach (var seg in newData)
            {
                if (seg.Array == null)
                    throw new SerializationException("BufferStream may not contain ArraySegments with null Array");
                if (seg.Count == 0)
                    continue;

                // Validate new data
                Tuple<TInd, TInd> rng = ValidateSegmentOrder(seg, IndexAccessor, UniqueIndexes, segInd);

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
                                    "Last index in {0} ({1}) is greater than the first new item's index ({2})",
                                    isFirstSeg ? "file" : "segment", prevSegLast, rng.Item1);
                            else
                                throw new BinaryFileException(
                                    "Last index in {0} ({1}) equals to the first new item's index (enfocing uniqueness)",
                                    isFirstSeg ? "file" : "segment", prevSegLast);

                        TruncateFile(rng.Item1);
                    }
                }

                yield return seg;

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

            FeedUtils.AssertPositiveIndex(firstInd);

            for (int i = buffer.Offset + 1; i < lastOffset; i++)
            {
                TInd newInd = indAccessor(data[i]);
                int cmp = newInd.CompareTo(lastInd);

                if (cmp < 0)
                    throw new BinaryFileException(
                        "Segment {0}, new item's index at #{1} ({2}) is greater than index of the following item #{3} ({4})",
                        segInd, i - 1, lastInd, i, newInd);
                if (uniqueIndexes && cmp == 0)
                    throw new BinaryFileException(
                        "Segment {0} new item's index at #{1} ({2}) equals the index of the following item #{3} (enforcing uniqueness)",
                        segInd, i - 1, lastInd, i);

                lastInd = newInd;
            }

            return Tuple.Create(firstInd, lastInd);
        }
    }
}