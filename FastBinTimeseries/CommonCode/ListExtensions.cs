#region COPYRIGHT

/*
 *     Copyright 2009-2011 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
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

namespace NYurik.FastBinTimeseries.CommonCode
{
    public static class ListExtensions
    {
        #region Find enum

        public enum Find : byte
        {
            AnyEqual,
            LastEqual,
            FirstEqual,
        }

        #endregion

        public static int BinarySearch<TItem, TKey>(
            this IList<TItem> list, TKey value,
            Func<TItem, TKey, int> comparer, Find search = Find.AnyEqual,
            int index = 0, int? length = null)
        {
            if (list == null) throw new ArgumentNullException("list");
            if (comparer == null) throw new ArgumentNullException("comparer");
            if (index < 0) throw new ArgumentOutOfRangeException("index", index, "< 0");

            byte midRoundingDirection;
            switch (search)
            {
                case Find.AnyEqual:
                case Find.FirstEqual:
                    midRoundingDirection = 0;
                    break;
                case Find.LastEqual:
                    midRoundingDirection = 1;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("search");
            }

            int start = index;
            int end;
            if (length.HasValue)
            {
                if (length.Value < 0)
                    throw new ArgumentOutOfRangeException("length", length, "< 0");
                end = start + length.Value - 1;
                if (end >= list.Count)
                    throw new ArgumentOutOfRangeException("length", length, "index + length > list.Count");
            }
            else
                end = list.Count - 1;


            while (start <= end)
            {
                int mid = start + ((end - start + midRoundingDirection) >> 1);

                int comp = comparer(list[mid], value);

                if (start == end)
                    return comp == 0 ? start : comp > 0 ? ~start : ~(start + 1);

                if (comp < 0)
                {
                    start = mid + 1;
                }
                else if (comp > 0)
                    end = mid - 1;
                else
                {
                    switch (search)
                    {
                        case Find.AnyEqual:
                            return mid;
                        case Find.LastEqual:
                            start = mid;
                            break;
                        case Find.FirstEqual:
                            end = mid;
                            break;
                    }
                }
            }
            return ~start;
        }
    }
}