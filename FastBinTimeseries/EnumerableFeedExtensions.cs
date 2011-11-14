using System;
using System.Collections.Generic;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public static class EnumerableFeedExtensions
    {
        public static IEnumerable<TVal> Stream<TInd, TVal>(
            this IEnumerableFeed<TInd, TVal> feed,
            TInd from, TInd? until = null, bool inReverse = false, int bufferSize = 0)
            where TInd : struct, IComparable<TInd>
        {
            return Stream(() => feed, from, until, inReverse, bufferSize);
        }

        public static IEnumerable<TVal> Stream<TInd, TVal>(
            Func<IEnumerableFeed<TInd, TVal>> feedFactory,
            TInd from, TInd? until = null, bool inReverse = false, int bufferSize = 0,
            Action<IEnumerableFeed<TInd, TVal>> onDispose = null)
            where TInd : struct, IComparable<TInd>
        {
            if (feedFactory == null)
                throw new ArgumentNullException("feedFactory");

            IEnumerableFeed<TInd, TVal> feed = feedFactory();
            try
            {
                foreach (var segm in feed.StreamSegments(from, until, inReverse, bufferSize))
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
            TInd from, TInd? until = null, bool inReverse = false, int bufferSize = 0)
            where TInd : struct, IComparable<TInd>
        {
            if (feed == null)
                throw new ArgumentNullException("feed");

            return until == null
                       ? feed.StreamSegments(from, inReverse, bufferSize)
                       : StreamSegmentsUntil(feed, from, until.Value, inReverse, bufferSize);
        }

        private static IEnumerable<ArraySegment<TVal>> StreamSegmentsUntil<TInd, TVal>(
            IEnumerableFeed<TInd, TVal> feed, TInd from,
            TInd until, bool inReverse,
            int bufferSize) where TInd : struct, IComparable<TInd>
        {
            Func<TVal, TInd> tsa = feed.IndexAccessor;

            foreach (var segm in feed.StreamSegments(from, inReverse, bufferSize))
            {
                if (segm.Count == 0)
                    continue;

                if (inReverse
                        ? tsa(segm.Array[segm.Offset]).CompareTo(until) >= 0
                        : tsa(segm.Array[segm.Offset + segm.Count - 1]).CompareTo(until) < 0)
                {
                    yield return segm;
                    continue;
                }

                int pos = segm.Array.BinarySearch(
                    until, (v, ts) => tsa(v).CompareTo(ts),
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
    }
}