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
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb
{
    public class Buffer<T>
    {
        private T[] _buffer;
        private int _count;

        public Buffer(int bufferSize)
        {
            if (bufferSize < 0) throw new ArgumentOutOfRangeException("bufferSize", bufferSize, "<0");
            _buffer = new T[bufferSize];
        }

        public Buffer([NotNull] T[] buffer, int count = 0)
        {
            if (buffer == null) throw new ArgumentNullException("buffer");
            _buffer = buffer;
            _count = count;
        }

        public int Capacity
        {
            get { return Array.Length; }
        }

        public int Count
        {
            get { return _count; }
            set
            {
                if (_count != value)
                {
                    if (value < 0) throw new ArgumentOutOfRangeException("value", value, "<0");
                    if (value > Array.Length) throw new ArgumentOutOfRangeException("value", value, ">Capacity");
                    if (value < _count)
                        System.Array.Clear(Array, value, _count - value);
                    _count = value;
                }
            }
        }

        public T[] Array
        {
            get { return _buffer; }
        }

        public void Add(T value)
        {
            int len = Array.Length;
            if (_count == len)
                RealocatePreserving(len*2);

            int pos = _count++;
            Array[pos] = value;
        }

        private void RealocatePreserving(int newSize)
        {
            var tmp = new T[newSize];
            System.Array.Copy(Array, tmp, Count);
            _buffer = tmp;
        }

        public ArraySegment<T> AsArraySegmentReversed()
        {
            var tmp = AsArraySegment();
            System.Array.Reverse(tmp.Array, tmp.Offset, tmp.Count);
            return tmp;
        }

        public ArraySegment<T> AsArraySegment()
        {
            return new ArraySegment<T>(Array, 0, Count);
        }
    }
}