using System;
using System.Collections.Generic;

namespace NYurik.FastBinTimeseries
{
    public class FeedConverter<TIndex, TOld, TNew> : IEnumerableFeed<TIndex, TNew>
        where TIndex : IComparable<TIndex>
    {
        private readonly Func<TOld, TNew> _converter;
        private readonly IEnumerableFeed<TIndex, TOld> _feed;
        private readonly Func<TOld, bool> _predicate;
        private readonly Func<TNew, TIndex> _timestampAccessor;

        public FeedConverter(IEnumerableFeed<TIndex, TOld> feed,
                             Func<TOld, TNew> converter,
                             Func<TNew, TIndex> timestampAccessor,
                             Func<TOld, bool> predicate = null)
        {
            if (feed == null) throw new ArgumentNullException("feed");
            if (converter == null) throw new ArgumentNullException("converter");
            if (timestampAccessor == null) throw new ArgumentNullException("timestampAccessor");

            _feed = feed;
            _timestampAccessor = timestampAccessor;
            _converter = converter;
            _predicate = predicate;
        }

        #region IEnumerableFeed<TIndex,TNew> Members

        public void Dispose()
        {
            _feed.Dispose();
        }

        public TDst RunGenericMethod<TDst, TArg>(IGenericCallable<TDst, TArg> callable, TArg arg)
        {
            return callable.Run<TNew>(this, arg);
        }

        public Type ItemType
        {
            get { return typeof (TNew); }
        }

        public string Tag
        {
            get { return _feed.Tag; }
        }

        public Func<TNew, TIndex> IndexAccessor
        {
            get { return _timestampAccessor; }
        }

        public IEnumerable<ArraySegment<TNew>> StreamSegments(TIndex fromInd, bool inReverse = false,
                                                              IEnumerable<Buffer<TNew>> bufferProvider = null,
                                                              long maxItemCount = long.MaxValue)
        {
            Func<TOld, TNew> conv = _converter;
            Func<TOld, bool> filter = _predicate;

            using (
                IEnumerator<Buffer<TNew>> buffers =
                    (bufferProvider ?? (new BufferProvider<TNew>().YieldMaxGrowingBuffer(maxItemCount, 10, 10, 10000))).
                        GetEnumerator())
            {
                Buffer<TNew> buff = null;
                foreach (
                    TOld old in
                        _feed.StreamSegments(fromInd, inReverse, maxItemCount: maxItemCount)
                            .StreamSegmentValues(inReverse))
                {
                    if (filter != null && !filter(old))
                        continue;

                    if (buff == null)
                    {
                        if (!buffers.MoveNext())
                            yield break;
                        buff = buffers.Current;
                    }

                    buff.Add(conv(old));

                    if (buff.Count == buff.Capacity)
                    {
                        yield return buff.AsArraySegment();
                        buff = null;
                    }
                }

                if (buff != null)
                    yield return buff.AsArraySegment();
            }
        }

        #endregion
    }
}