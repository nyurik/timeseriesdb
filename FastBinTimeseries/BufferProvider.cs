using System;
using System.Collections.Generic;
using System.Threading;

namespace NYurik.FastBinTimeseries
{
    public class BufferProvider<T>
    {
        private WeakReference _buffer;

        /// <summary>
        /// Yield maximum available buffer every time. If the buffer is smaller than initSize,
        /// allocate [initSize] items first, and after growAfter iterations, grow it to the largeSize.
        /// Buffer.Count will always be set to 0
        /// </summary>
        public IEnumerable<Buffer<T>> YieldMaxGrowingBuffer(long maxItemCount, int initSize, int growAfter,
                                                            int largeSize)
        {
            Buffer<T> buffer = GetBufferRef();

            int size = initSize > maxItemCount ? (int) maxItemCount : initSize;
            if (buffer == null || buffer.Capacity < size)
                buffer = new Buffer<T>(size);

            try
            {
                for (int i = 0; i < growAfter; i++)
                {
					buffer.Count = buffer.Capacity;
                    maxItemCount -= buffer.Capacity;
                    yield return buffer;
                }

                size = largeSize > maxItemCount ? (int) maxItemCount : largeSize;

                if (buffer.Capacity < size)
                    buffer = new Buffer<T>(size);

                while (true)
                {
					buffer.Count = buffer.Capacity;
                    maxItemCount -= buffer.Capacity;
                    yield return buffer;
                }
            }
            finally
            {
                _buffer = new WeakReference(buffer);
            }
        }

        public IEnumerable<Buffer<T>> YieldFixedSize(int size)
        {
            Buffer<T> buffer = GetBufferRef();

            if (buffer == null || buffer.Capacity < size)
                buffer = new Buffer<T>(size);

            buffer.Count = size;
            yield return buffer;
        }

        /// <summary>
        /// Yield a buffer that could either be the same instance as before, or a larger one.
        /// A weak reference will be kept so as to reduce the number of memory allocations. The method is thread safe.
        /// </summary>
        public IEnumerable<Buffer<T>> YieldFixed(int firstSize, int smallSize, int growAfter, int largeSize)
        {
            if (smallSize <= 0 || largeSize <= 0)
                throw new ArgumentException("smallSize and largeSize must not be 0");

            Buffer<T> buffer = GetBufferRef();

            try
            {
                if (firstSize > 0)
                {
                    if (buffer == null || buffer.Capacity < firstSize)
                        buffer = new Buffer<T>(firstSize);

                    buffer.Count = firstSize;
                    yield return buffer;
                }
               
                if (buffer == null || buffer.Capacity < smallSize)
                    buffer = new Buffer<T>(smallSize);

                for (int i = 0; i < growAfter; i++)
                {
                    buffer.Count = smallSize;
                    yield return buffer;
                }

                if (buffer.Capacity < largeSize)
                    buffer = new Buffer<T>(largeSize);

                while (true)
                {
                    buffer.Count = largeSize;
                    yield return buffer;
                }
            }
            finally
            {
                if (buffer != null)
                    _buffer = new WeakReference(buffer);
            }
        }

        private Buffer<T> GetBufferRef()
        {
            WeakReference weakRef = Interlocked.Exchange(ref _buffer, null);
            return (weakRef != null ? weakRef.Target as Buffer<T> : null);
        }
    }
}