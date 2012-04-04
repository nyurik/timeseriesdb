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
using System.Threading;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb
{
    public static class DynamicFeed
    {
        private static readonly GenCallable Callable = new GenCallable();

        public static IGenericInvoker2 Create(
            [NotNull] Func<IGenericInvoker2> factory, [NotNull] Action<IGenericInvoker2> disposer)
        {
            if (factory == null) throw new ArgumentNullException("factory");
            if (disposer == null) throw new ArgumentNullException("disposer");

            var feed = factory();
            try
            {
                return feed.RunGenericMethod(
                    Callable, new Tuple<Func<IGenericInvoker2>, Action<IGenericInvoker2>>(factory, disposer));
            }
            finally
            {
                disposer(feed);
            }
        }

        #region Nested type: GenCallable

        private class GenCallable :
            IGenericCallable2<IGenericInvoker2, Tuple<Func<IGenericInvoker2>, Action<IGenericInvoker2>>>
        {
            #region IGenericCallable2<IGenericInvoker2,Tuple<Func<IGenericInvoker2>,Action<IGenericInvoker2>>> Members

            public IGenericInvoker2 Run<TInd, TVal>(
                IGenericInvoker2 source, Tuple<Func<IGenericInvoker2>, Action<IGenericInvoker2>> arg)
                where TInd : IComparable<TInd>
            {
                var feed = (IEnumerableFeed<TInd, TVal>) source;

                return new DynamicFeed<TInd, TVal>(
                    () => (IEnumerableFeed<TInd, TVal>) arg.Item1(), f => arg.Item2(f), feed.IndexAccessor);
            }

            #endregion
        }

        #endregion
    }

    /// <summary>
    /// Helper class that allows factory-based feed creation and custom disposal.
    /// </summary>
    public class DynamicFeed<TInd, TVal> : IEnumerableFeed<TInd, TVal>
        where TInd : IComparable<TInd>
    {
        private readonly Action<IEnumerableFeed<TInd, TVal>> _disposer;
        private readonly Func<IEnumerableFeed<TInd, TVal>> _factory;
        private readonly Lazy<Func<TVal, TInd>> _indexAccessor;

        public DynamicFeed(
            [NotNull] Func<IEnumerableFeed<TInd, TVal>> factory, [NotNull] Action<IEnumerableFeed<TInd, TVal>> disposer,
            Func<TVal, TInd> indexAccessor = null)
        {
            if (factory == null) throw new ArgumentNullException("factory");
            if (disposer == null) throw new ArgumentNullException("disposer");
            _factory = factory;
            _disposer = disposer;
            _indexAccessor =
                indexAccessor == null
                    ? new Lazy<Func<TVal, TInd>>(
                          () =>
                              {
                                  var enmr = _factory();
                                  try
                                  {
                                      return enmr.IndexAccessor;
                                  }
                                  finally
                                  {
                                      _disposer(enmr);
                                  }
                              })
                    : new Lazy<Func<TVal, TInd>>(() => indexAccessor, LazyThreadSafetyMode.None);
        }

        #region IEnumerableFeed<TInd,TVal> Members

        public TDst RunGenericMethod<TDst, TArg>(IGenericCallable2<TDst, TArg> callable, TArg arg)
        {
            if (callable == null) throw new ArgumentNullException("callable");
            return callable.Run<TInd, TVal>(this, arg);
        }

        public Func<TVal, TInd> IndexAccessor
        {
            get { return _indexAccessor.Value; }
        }

        public IEnumerable<ArraySegment<TVal>> StreamSegments(
            TInd fromInd = default(TInd), bool inReverse = false, IEnumerable<Buffer<TVal>> bufferProvider = null,
            long maxItemCount = long.MaxValue)
        {
            var enmr = _factory();
            try
            {
                foreach (var seg in enmr.StreamSegments(fromInd, inReverse, bufferProvider, maxItemCount))
                    yield return seg;
            }
            finally
            {
                _disposer(enmr);
            }
        }

        #endregion
    }
}