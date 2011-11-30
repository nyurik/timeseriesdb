using System;
using System.Collections.Generic;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public static class EnumerableFeedExtensions
    {
//        [Obsolete("Use Stream<TInd, TVal>() instead")]
//        public static IEnumerable<T> Stream<T>(this IEnumerableFeed<T> feed, UtcDateTime from, UtcDateTime? until = null,
//                                               bool inReverse = false, int bufferSize = 0)
//        {
//            return ((IEnumerableFeed<UtcDateTime, T>) feed).Stream(from, until, inReverse, bufferSize);
//        }

        [Obsolete("Use Stream<TInd, TVal>() instead")]
        public static IEnumerable<T> Stream<T>(Func<IEnumerableFeed<T>> feedFactory,
                                               UtcDateTime from, UtcDateTime? until = null,
                                               bool inReverse = false, IEnumerable<T[]> bufferProvider = null,
                                               Action<IEnumerableFeed<T>> onDispose = null)
        {
            return Stream(
                () => (IEnumerableFeed<UtcDateTime, T>) feedFactory(),
                from, until, inReverse, bufferProvider,
                onDispose != null
                    ? f => onDispose((IEnumerableFeed<T>) f)
                    : (Action<IEnumerableFeed<UtcDateTime, T>>) null);
        }

        public static IEnumerable<TVal> Stream<TInd, TVal>(
            this IEnumerableFeed<TInd, TVal> feed,
            TInd from, TInd? until = null, bool inReverse = false, IEnumerable<TVal[]> bufferProvider = null)
            where TInd : struct, IComparable<TInd>
        {
            return Stream(() => feed, from, until, inReverse, bufferProvider);
        }

        public static IEnumerable<TVal> Stream<TInd, TVal>(
            Func<IEnumerableFeed<TInd, TVal>> feedFactory,
            TInd from, TInd? until = null, bool inReverse = false, IEnumerable<TVal[]> bufferProvider = null,
            Action<IEnumerableFeed<TInd, TVal>> onDispose = null)
            where TInd : struct, IComparable<TInd>
        {
            if (feedFactory == null)
                throw new ArgumentNullException("feedFactory");

            IEnumerableFeed<TInd, TVal> feed = feedFactory();
            try
            {
                foreach (var segm in feed.StreamSegments(from, until, inReverse, bufferProvider))
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
//            UtcDateTime from, UtcDateTime? until = null, bool inReverse = false, int bufferSize = 0)
//        {
//            if (feed == null)
//                throw new ArgumentNullException("feed");
//
//            return until == null
//                       ? feed.StreamSegments(from, inReverse, bufferSize)
//                       : StreamSegmentsUntil(
//                           (IEnumerableFeed<UtcDateTime, TVal>) feed, from, until.Value, inReverse, bufferSize);
//        }
//
        public static IEnumerable<ArraySegment<TVal>> StreamSegments<TInd, TVal>(
            this IEnumerableFeed<TInd, TVal> feed,
            TInd from, TInd? until = null, bool inReverse = false, IEnumerable<TVal[]> bufferProvider = null)
            where TInd : struct, IComparable<TInd>
        {
            if (feed == null)
                throw new ArgumentNullException("feed");

            return until == null
                       ? feed.StreamSegments(from, inReverse, bufferProvider)
                       : StreamSegmentsUntil(feed, from, until.Value, inReverse, bufferProvider);
        }

        private static IEnumerable<ArraySegment<TVal>> StreamSegmentsUntil<TInd, TVal>(
            IEnumerableFeed<TInd, TVal> feed, TInd from,
            TInd until, bool inReverse,
            IEnumerable<TVal[]> bufferProvider) where TInd : struct, IComparable<TInd>
        {
            Func<TVal, TInd> tsa = feed.IndexAccessor;

            foreach (var segm in feed.StreamSegments(from, inReverse, bufferProvider))
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