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
using System.Collections.Generic;

namespace NYurik.FastBinTimeseries
{
    public static class EnumerableFeedExtensions
    {
        public static IEnumerable<TVal> Stream<TInd, TVal>(
            this IEnumerableFeed<TInd, TVal> feed,
            TInd fromInd = default(TInd), TInd untilInd = default(TInd), bool inReverse = false,
            IEnumerable<Buffer<TVal>> bufferProvider = null, long maxItemCount = long.MaxValue)
            where TInd : IComparable<TInd>
        {
            return Stream(() => feed, fromInd, untilInd, inReverse, bufferProvider, maxItemCount);
        }

        public static IEnumerable<TVal> Stream<TInd, TVal>(
            Func<IEnumerableFeed<TInd, TVal>> feedFactory,
            TInd fromInd = default(TInd), TInd untilInd = default(TInd), bool inReverse = false,
            IEnumerable<Buffer<TVal>> bufferProvider = null, long maxItemCount = long.MaxValue,
            Action<IEnumerableFeed<TInd, TVal>> onDispose = null)
            where TInd : IComparable<TInd>
        {
            if (feedFactory == null)
                throw new ArgumentNullException("feedFactory");

            IEnumerableFeed<TInd, TVal> feed = feedFactory();
            try
            {
                foreach (var segm in feed.StreamSegments(fromInd, untilInd, inReverse, bufferProvider, maxItemCount))
                {
                    if (inReverse)
                        for (int i = segm.Offset + segm.Count - 1; i >= segm.Offset; i--)
                            yield return segm.Array[i];
                    else
                        for (int i = segm.Offset; i < segm.Offset + segm.Count; i++)
                            yield return segm.Array[i];
                }
            }
            finally
            {
                if (onDispose != null)
                    onDispose(feed);
            }
        }

        public static IEnumerable<ArraySegment<TVal>> StreamSegments<TInd, TVal>(
            this IEnumerableFeed<TInd, TVal> feed,
            TInd fromInd = default(TInd), TInd untilInd = default(TInd), bool inReverse = false,
            IEnumerable<Buffer<TVal>> bufferProvider = null, long maxItemCount = long.MaxValue)
            where TInd : IComparable<TInd>
        {
            if (feed == null)
                throw new ArgumentNullException("feed");

            return FastBinFileUtils.IsDefault(untilInd)
                       ? feed.StreamSegments(fromInd, inReverse, bufferProvider, maxItemCount)
                       : StreamSegmentsUntil(feed, fromInd, untilInd, inReverse, bufferProvider, maxItemCount);
        }

        private static IEnumerable<ArraySegment<TVal>> StreamSegmentsUntil<TInd, TVal>(
            IEnumerableFeed<TInd, TVal> feed,
            TInd fromInd, TInd untilInd, bool inReverse,
            IEnumerable<Buffer<TVal>> bufferProvider, long maxItemCount)
            where TInd : IComparable<TInd>
        {
            Func<TVal, TInd> tsa = feed.IndexAccessor;

            foreach (var segm in feed.StreamSegments(fromInd, inReverse, bufferProvider, maxItemCount))
            {
                if (segm.Count == 0)
                    continue;

                if (inReverse
                        ? tsa(segm.Array[segm.Offset]).CompareTo(untilInd) >= 0
                        : tsa(segm.Array[segm.Offset + segm.Count - 1]).CompareTo(untilInd) < 0)
                {
                    yield return segm;
                    continue;
                }

                var pos = (int)
                          FastBinFileUtils.BinarySearch(
                              untilInd, segm.Offset, segm.Count, false, i => tsa(segm.Array[i]));

                if (pos < 0)
                    pos = ~pos;

                if (inReverse)
                {
                    int count = segm.Count - pos;
                    if (count > 0)
                        yield return new ArraySegment<TVal>(segm.Array, pos, count);
                }
                else
                {
                    if (pos > 0)
                        yield return new ArraySegment<TVal>(segm.Array, segm.Offset, pos - segm.Offset);
                }

                yield break;
            }
        }

        public static IEnumerable<T> StreamSegmentValues<T>(
            this ArraySegment<T> arraySegment,
            bool inReverse = false)
        {
            return new[] {arraySegment}.StreamSegmentValues(inReverse);
        }

        public static IEnumerable<T> StreamSegmentValues<T>(
            this IEnumerable<ArraySegment<T>> stream,
            bool inReverse = false)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            if (inReverse)
            {
                foreach (var v in stream)
                    if (v.Count > 0)
                        for (int i = v.Count + v.Offset - 1; i >= v.Offset; i--)
                            yield return v.Array[i];
            }
            else
            {
                foreach (var v in stream)
                    if (v.Count > 0)
                    {
                        int max = v.Offset + v.Count;
                        for (int i = v.Offset; i < max; i++)
                            yield return v.Array[i];
                    }
            }
        }
    }
}