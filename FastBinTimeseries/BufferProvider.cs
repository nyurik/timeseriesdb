using System;
using System.Collections.Generic;
using System.Threading;

namespace NYurik.FastBinTimeseries
{
    public class BufferProvider<T>
    {
        private WeakReference _buffer;

        /// <summary>
        /// With each yield, this method gives an array that could either be the same instance as before,
        /// or a different (bigger) one. A weak reference will be kept so as to reduce the number of
        /// memory allocations. The method is thread safe.
        /// </summary>
        public IEnumerable<Buffer<T>> GetBuffers(int initSize, int grownSize, int growupAfter)
        {
            WeakReference weakRef = Interlocked.Exchange(ref _buffer, null);

            Buffer<T> buffer = (weakRef != null ? weakRef.Target as Buffer<T> : null);
            if (buffer == null || buffer.Capacity < initSize)
                buffer = new Buffer<T>(initSize);

            try
            {
                int iterations = 0;
                bool grow = true;

                while (true)
                {
                    if (grow)
                    {
                        if (iterations++ > growupAfter && buffer.Capacity < grownSize)
                            buffer = new Buffer<T>(grownSize);
                        grow = false;
                    }

                    yield return buffer;
                }
            }
            finally
            {
                _buffer = new WeakReference(buffer);
            }
        }
    }
}