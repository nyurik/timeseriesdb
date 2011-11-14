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