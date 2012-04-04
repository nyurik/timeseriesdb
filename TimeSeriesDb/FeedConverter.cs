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
using NYurik.TimeSeriesDb.Common;

namespace NYurik.TimeSeriesDb
{
    public class FeedConverter<TInd, TOld, TNew> : IEnumerableFeed<TInd, TNew>
        where TInd : IComparable<TInd>
    {
        private readonly Func<TOld, TNew> _converter;
        private readonly IEnumerableFeed<TInd, TOld> _feed;
        private readonly Func<TNew, TInd> _indexAccessor;
        private readonly Func<TOld, bool> _predicate;

        public FeedConverter(
            IEnumerableFeed<TInd, TOld> feed,
            Func<TOld, TNew> converter,
            Func<TNew, TInd> indexAccessor,
            Func<TOld, bool> predicate = null)
        {
            if (feed == null) throw new ArgumentNullException("feed");
            if (converter == null) throw new ArgumentNullException("converter");
            if (indexAccessor == null) throw new ArgumentNullException("indexAccessor");

            _feed = feed;
            _indexAccessor = indexAccessor;
            _converter = converter;
            _predicate = predicate;
        }

        #region IEnumerableFeed<TInd,TNew> Members

        TDst IGenericInvoker2.RunGenericMethod<TDst, TArg>(IGenericCallable2<TDst, TArg> callable, TArg arg)
        {
            if (callable == null) throw new ArgumentNullException("callable");
            return callable.Run<TInd, TNew>(this, arg);
        }

        public Func<TNew, TInd> IndexAccessor
        {
            get { return _indexAccessor; }
        }

        public IEnumerable<ArraySegment<TNew>> StreamSegments(
            TInd fromInd = default(TInd), bool inReverse = false,
            IEnumerable<Buffer<TNew>> bufferProvider = null,
            long maxItemCount = long.MaxValue)
        {
            FeedUtils.AssertPositiveIndex(fromInd);

            Func<TOld, TNew> conv = _converter;
            Func<TOld, bool> filter = _predicate;

            using (
                IEnumerator<Buffer<TNew>> buffers =
                    (bufferProvider ?? (new BufferProvider<TNew>().YieldMaxGrowingBuffer(maxItemCount, 10, 10, 10000))).
                        GetEnumerator())
            {
                Buffer<TNew> buff = null;
                int maxCount = 0;

                foreach (TOld old in _feed.Stream(fromInd, inReverse: inReverse, maxItemCount: maxItemCount))
                {
                    if (filter != null && !filter(old))
                        continue;

                    if (buff == null)
                    {
                        if (!buffers.MoveNext())
                            yield break;

                        buff = buffers.Current;
                        maxCount = buff.Count > 0 ? buff.Count : buff.Capacity;
                        buff.Count = 0;
                    }

                    buff.Add(conv(old));
                    maxCount--;

                    if (maxCount <= 0)
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