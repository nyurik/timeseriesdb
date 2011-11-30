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
        public IEnumerable<T[]> GetBuffers(int initSize, int grownSize, int growupAfter)
        {
            WeakReference weakRef = Interlocked.Exchange(ref _buffer, null);

            T[] buffer = (weakRef != null ? weakRef.Target as T[] : null);
            if (buffer == null || buffer.Length < initSize)
                buffer = new T[initSize];

            try
            {
                int iterations = 0;
                bool grow = true;

                while (true)
                {
                    if (grow)
                    {
                        if (iterations++ > growupAfter && buffer.Length < grownSize)
                            buffer = new T[grownSize];
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