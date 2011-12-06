using System;
using System.Diagnostics.Contracts;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries
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

        [Pure]
        public ArraySegment<T> AsArraySegment
        {
            get { return new ArraySegment<T>(Array, 0, _count); }
        }

        [Pure]
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
                    if (value > Capacity) throw new ArgumentOutOfRangeException("value", value, ">Capacity");
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
            if (_count == Array.Length)
                RealocatePreserving(Array.Length*2);
            Array[_count++] = value;
        }

        private void RealocatePreserving(int newSize)
        {
            var tmp = new T[newSize];
            System.Array.Copy(Array, tmp, Count);
            _buffer = tmp;
        }

        public void ShiftLeft(int offset, int count)
        {
            if (offset != 0)
                System.Array.Copy(_buffer, offset, _buffer, 0, count);
            Count = count;
        }
    }
}