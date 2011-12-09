using System;
using System.Collections.Generic;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public static class EnumerableFeedExtensions
    {
        public static IEnumerable<TVal> Stream<TInd, TVal>(
            this IEnumerableFeed<TInd, TVal> feed,
            TInd from, TInd? until = null, bool inReverse = false, IEnumerable<Buffer<TVal>> bufferProvider = null,
            long maxItemCount = long.MaxValue)
            where TInd : struct, IComparable<TInd>
        {
            return Stream(() => feed, from, until, inReverse, bufferProvider, maxItemCount: maxItemCount);
        }

        public static IEnumerable<TVal> Stream<TInd, TVal>(
            Func<IEnumerableFeed<TInd, TVal>> feedFactory,
            TInd from, TInd? until = null, bool inReverse = false, IEnumerable<Buffer<TVal>> bufferProvider = null,
            Action<IEnumerableFeed<TInd, TVal>> onDispose = null, long maxItemCount = long.MaxValue)
            where TInd : struct, IComparable<TInd>
        {
            if (feedFactory == null)
                throw new ArgumentNullException("feedFactory");

            IEnumerableFeed<TInd, TVal> feed = feedFactory();
            try
            {
                foreach (var segm in feed.StreamSegments(from, until, inReverse, bufferProvider, maxItemCount))
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

//        [Obsolete("Use StreamSegments<TInd, TVal>() instead")]
//        public static IEnumerable<ArraySegment<TVal>> StreamSegments<TVal>(
//            this IEnumerableFeed<TVal> feed,
//            UtcDateTime fromInd, UtcDateTime? untilInd = null, bool inReverse = false, int bufferSize = 0)
//        {
//            if (feed == null)
//                throw new ArgumentNullException("feed");
//
//            return untilInd == null
//                       ? feed.StreamSegments(fromInd, inReverse, bufferSize)
//                       : StreamSegmentsUntil(
//                           (IEnumerableFeed<UtcDateTime, TVal>) feed, fromInd, untilInd.Value, inReverse, bufferSize);
//        }
//
        public static IEnumerable<ArraySegment<TVal>> StreamSegments<TInd, TVal>(
            this IEnumerableFeed<TInd, TVal> feed, TInd fromInd, TInd? until = null, bool inReverse = false,
            IEnumerable<Buffer<TVal>> bufferProvider = null, long maxItemCount = long.MaxValue)
            where TInd : struct, IComparable<TInd>
        {
            if (feed == null)
                throw new ArgumentNullException("feed");

            return until == null
                       ? feed.StreamSegments(fromInd, inReverse, bufferProvider, maxItemCount)
                       : StreamSegmentsUntil(feed, fromInd, until.Value, inReverse, bufferProvider, maxItemCount);
        }

        private static IEnumerable<ArraySegment<TVal>> StreamSegmentsUntil<TInd, TVal>(
            IEnumerableFeed<TInd, TVal> feed, TInd fromInd, TInd untilInd, bool inReverse,
            IEnumerable<Buffer<TVal>> bufferProvider, long maxItemCount)
            where TInd : struct, IComparable<TInd>
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

                int pos = segm.Array.BinarySearch(
                    untilInd, (v, ts) => tsa(v).CompareTo(ts),
                    ListExtensions.Find.FirstEqual,
                    segm.Offset, segm.Count);
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
                        yield return new ArraySegment<TVal>(segm.Array, segm.Offset, pos);
                }

                yield break;
            }
        }


        public static IEnumerable<T> StreamSegmentValues<T>(this IEnumerable<ArraySegment<T>> stream,
                                                            bool inReverse = false)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            if (inReverse)
            {
                foreach (var v in stream)
                    if (v.Count > 0)
                        for (int i = v.Count - 1; i >= v.Offset; i--)
                            yield return v.Array[i];
            }
            else
            {
                foreach (var v in stream)
                    if (v.Count > 0)
                        for (int i = v.Offset; i < v.Count; i++)
                            yield return v.Array[i];
            }
        }
    }
}