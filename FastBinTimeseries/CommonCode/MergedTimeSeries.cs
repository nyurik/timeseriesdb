using System;
using System.Collections.Generic;

namespace NYurik.FastBinTimeseries.CommonCode
{
    [Serializable]
    public class MergedTimeSeries<T> : ITimeSeries<T>
    {
        private readonly T[] _values;

        public MergedTimeSeries(int size, Func<T, UtcDateTime> timestampAccessor)
        {
            if (timestampAccessor == null) throw new ArgumentNullException("timestampAccessor");
            if (size < 0) throw new ArgumentOutOfRangeException("size", size, "must be non-negative");

            _values = new T[size];
            TimestampAccessor = timestampAccessor;
        }

        public MergedTimeSeries(T[] values, Func<T, UtcDateTime> timestampAccessor)
        {
            if (values == null) throw new ArgumentNullException("values");
            if (timestampAccessor == null) throw new ArgumentNullException("timestampAccessor");

            _values = values;
            TimestampAccessor = timestampAccessor;
        }

        public T[] Values
        {
            get { return _values; }
        }

        /// <summary>
        /// A delegate to a function that extracts timestamp of a given item
        /// </summary>
        public Func<T, UtcDateTime> TimestampAccessor { get; private set; }

        #region ITimeSeries<T> Members

        public T this[int index]
        {
            get { return _values[index]; }
        }

        public Type GetElementType()
        {
            return typeof (T);
        }

        object ISeries.GetValueSlow(int index)
        {
            return _values[index];
        }

        public int Count
        {
            get { return _values.Length; }
        }

        public int BinarySearch(UtcDateTime timestamp)
        {
            return BinarySearch(_values, timestamp, TimestampComparer);
        }

        public UtcDateTime GetTimestamp(int index)
        {
            return TimestampAccessor(_values[index]);
        }

        #endregion

        private int TimestampComparer(T value, UtcDateTime timestamp)
        {
            return (int) (TimestampAccessor(value) - timestamp).Ticks;
        }

        public override string ToString()
        {
            string res = string.Format("{0} {1} values", Count, typeof (T).Name);
            return _values.Length != 0
                       ? string.Format("{0} {1:o}-{2:o}", res, GetTimestamp(0), GetTimestamp(_values.Length - 1))
                       : res;
        }

        public static int BinarySearch<TItem, TKey>(IList<TItem> list, TKey value, Func<TItem, TKey, int> comparer)
        {
            int start = 0;
            int end = (0 + list.Count) - 1;
            while (start <= end)
            {
                int mid = start + ((end - start) >> 1);
                int comp = comparer(list[mid], value);
                if (comp == 0)
                    return mid;
                if (comp < 0)
                    start = mid + 1;
                else
                    end = mid - 1;
            }
            return ~start;
        }
    }
}