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

namespace NYurik.FastBinTimeseries.CommonCode
{
    [Serializable]
    public class TimeSeries<T> : ITimeSeries<T>
    {
        private readonly UtcDateTime[] _timestamps;
        private readonly T[] _values;

        public TimeSeries(int size)
        {
            if (size < 0) throw new ArgumentOutOfRangeException("size", size, "must be non-negative");
            _timestamps = new UtcDateTime[size];
            _values = new T[size];
        }

        public TimeSeries(UtcDateTime[] timestamps, T[] values)
        {
            if (timestamps == null) throw new ArgumentNullException("timestamps");
            if (values == null) throw new ArgumentNullException("values");
            if (timestamps.Length != values.Length) throw new ArgumentException("Array sizes must be identical");
            _timestamps = timestamps;
            _values = values;
        }

        public UtcDateTime[] Timestamps
        {
            get { return _timestamps; }
        }

        public T[] Values
        {
            get { return _values; }
        }

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
            return Array.BinarySearch(_timestamps, timestamp);
        }

        public UtcDateTime GetTimestamp(int index)
        {
            return _timestamps[index];
        }

        #endregion

        public override string ToString()
        {
            string res = string.Format("{0} {1} values", Count, typeof (T).Name);
            return _timestamps.Length != 0
                       ? string.Format("{0} {1:o}-{2:o}", res, _timestamps[0], _timestamps[_timestamps.Length - 1])
                       : res;
        }
    }

    public class UniformTimeSeries<T> : ITimeSeries<T>
    {
        private readonly UtcDateTime _firstTimestamp;
        private readonly TimeSpan _itemSpan;
        private readonly T[] _values;

        public UniformTimeSeries(UtcDateTime firstTimestamp, TimeSpan itemSpan, T[] values)
        {
            if (values == null) throw new ArgumentNullException("values");
            if (itemSpan <= TimeSpan.Zero) throw new ArgumentOutOfRangeException("itemSpan", itemSpan, "<= 0");

            _firstTimestamp = firstTimestamp;
            _itemSpan = itemSpan;
            _values = values;
        }

        public UtcDateTime FirstTimestamp
        {
            get { return _firstTimestamp; }
        }

        public T[] Values
        {
            get { return _values; }
        }

        public TimeSpan ItemSpan
        {
            get { return _itemSpan; }
        }

        #region ITimeSeries<T> Members

        public int Count
        {
            get { return _values.Length; }
        }

        public Type GetElementType()
        {
            return typeof (T);
        }

        object ISeries.GetValueSlow(int index)
        {
            return _values[index];
        }

        public int BinarySearch(UtcDateTime timestamp)
        {
            if (timestamp < FirstTimestamp)
                return ~0;

            if (timestamp > FirstTimestamp + ItemSpan.Multiply(Count - 1))
                return ~Count;

            TimeSpan t = (timestamp - FirstTimestamp);
            var div = (int) (t.Ticks/ItemSpan.Ticks);
            if (t.Ticks%ItemSpan.Ticks != 0)
                return ~(div + 1);
            return div;
        }

        public UtcDateTime GetTimestamp(int index)
        {
            if (index < 0) throw new ArgumentOutOfRangeException("index", index, "<0");
            if (index >= Count) throw new ArgumentOutOfRangeException("index", index, ">=Count");
            return FirstTimestamp + ItemSpan.Multiply(index);
        }

        public T this[int index]
        {
            get { return _values[index]; }
        }

        #endregion

        public override string ToString()
        {
            return string.Format(
                "{0} {1} values starting at {2} every {3}", Count, typeof (T).Name, FirstTimestamp,
                ItemSpan);
        }
    }
}