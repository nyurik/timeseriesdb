#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

#endregion

using System;
using System.Collections.Generic;
using System.Text;

namespace NYurik.TimeSeriesDb.Samples
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
        public static string JoinStr<T>(
            this IEnumerable<T> source, string separator = ", ",
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

        public static string DumpFeed(IGenericInvoker2 f)
        {
            return f.RunGenericMethod(new DumpHelper(), null);
        }

        #region Nested type: DumpHelper

        private class DumpHelper : IGenericCallable2<string, object>
        {
            #region IGenericCallable2<string,object> Members

            public string Run<TInd, TVal>(IGenericInvoker2 source, object arg)
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