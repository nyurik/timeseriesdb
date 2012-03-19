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
using System.Text;

namespace NYurik.TimeSeriesDb.Examples
{
    internal static class Utils
    {
        public static IEnumerable<ArraySegment<T>> GenerateData<T>(long start, long count, Func<long, T> newItem)
        {
            // In regular cases, data should be yielded in much larger segments to optimize IO operations
            const int segSize = 8;
            var arr = new T[segSize];

            int i = 0;
            for (long c = start; c < start + count; c++)
            {
                if (i >= arr.Length)
                {
                    yield return new ArraySegment<T>(arr);
                    i = 0;
                }

                arr[i++] = newItem(c);
            }

            if (i > 0)
                yield return new ArraySegment<T>(arr, 0, i);
        }

        /// <summary>
        /// Join multiple items converting each to string using provided converter with a separator in-between.
        /// </summary>
        public static string JoinStr<T>(this IEnumerable<T> source, string separator = ", ",
                                        Converter<T, string> converter = null)
        {
            if (source == null)
                throw new ArgumentNullException("source");
            if (separator == null)
                throw new ArgumentNullException("separator");
            
            if (converter == null)
            {
                // ReSharper disable CompareNonConstrainedGenericWithNull
                converter = i => i == null ? "" : i.ToString();
                // ReSharper restore CompareNonConstrainedGenericWithNull
            }

            bool isFirst = true;
            var sb = new StringBuilder();
            foreach (T t in source)
            {
                if (isFirst)
                    isFirst = false;
                else
                    sb.Append(separator);

                sb.Append(converter(t));
            }
            return sb.ToString();
        }

        public static string DumpFeed(IEnumerableFeed f)
        {
            return f.RunGenericMethod(new DumpHelper(), null);
        }

        #region Nested type: DumpHelper

        private class DumpHelper : IGenericCallable2<string, object>
        {
            #region IGenericCallable2<string,object> Members

            public string Run<TInd, TVal>(IGenericInvoker source, object arg)
                where TInd : IComparable<TInd>
            {
                var f = (IEnumerableFeed<TInd, TVal>) source;

                var sb = new StringBuilder();
                foreach (TVal v in f.Stream())
                {
                    sb.Append(v);
                    sb.Append("\n");
                }
                return sb.Length == 0 ? "(empty)" : sb.ToString();
            }

            #endregion
        }

        #endregion
    }
}