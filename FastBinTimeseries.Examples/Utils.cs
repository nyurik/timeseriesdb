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

namespace NYurik.FastBinTimeseries.Examples
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
    }
}