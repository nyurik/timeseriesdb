using System;
using System.Collections.Generic;
using System.Threading;

namespace NYurik.FastBinTimeseries.CommonCode
{
    public class SynchronizedDictionary<TKey, TValue>
    {
        private readonly Dictionary<TKey, TValue> _dictionary = new Dictionary<TKey, TValue>();
        private readonly ReaderWriterLock _rwLock = new ReaderWriterLock();

        public SynchronizedDictionary()
        {
            LockTimeout = TimeSpan.FromSeconds(15);
        }

        public TimeSpan LockTimeout { get; set; }

        public TValue GetCreateValue(TKey key, Func<TKey, TValue> createItemMethod)
        {
            bool isCached;
            TValue tsAccessorType;
            _rwLock.AcquireReaderLock(LockTimeout);
            try
            {
                isCached = _dictionary.TryGetValue(key, out tsAccessorType);
            }
            finally
            {
                _rwLock.ReleaseReaderLock();
            }

            if (!isCached)
            {
                _rwLock.AcquireWriterLock(LockTimeout);
                try
                {
                    // double check - in case another thread already created it
                    if (!_dictionary.TryGetValue(key, out tsAccessorType))
                    {
                        _dictionary[key] = tsAccessorType = createItemMethod(key);
                    }
                }
                finally
                {
                    _rwLock.ReleaseWriterLock();
                }
            }
            return tsAccessorType;
        }

        public void Remove(TKey key)
        {
            _rwLock.AcquireWriterLock(LockTimeout);
            try
            {
                _dictionary.Remove(key);
            }
            finally
            {
                _rwLock.ReleaseWriterLock();
            }
        }
    }
}