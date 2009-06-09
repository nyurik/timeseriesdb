using System;
using System.Collections.Generic;
using System.Threading;

namespace NYurik.FastBinTimeseries.CommonCode
{
    /// <summary>
    /// A thread-safe, read-optimized dictionary that dynamically creates missing items
    /// </summary>
    public class DynamicSyncDictionary<TKey, TValue>
    {
        private readonly Dictionary<TKey, TValue> _dictionary = new Dictionary<TKey, TValue>();
        private readonly ReaderWriterLock _rwLock = new ReaderWriterLock();

        public DynamicSyncDictionary(Func<TKey, TValue> createNewItemMethod)
        {
            CreateNewItemMethod = createNewItemMethod;
            LockTimeout = TimeSpan.FromSeconds(15);
        }

        public TimeSpan LockTimeout { get; set; }

        public Func<TKey, TValue> CreateNewItemMethod { get; set; }

        public TValue GetCreateValue(TKey key)
        {
            if (CreateNewItemMethod == null)
                throw new InvalidOperationException("This object was not created with a default createNewItem method");

            return GetCreateValue(key, CreateNewItemMethod);
        }

        public TValue GetCreateValue(TKey key, Func<TKey, TValue> createNewItemMethod)
        {
            if (createNewItemMethod == null) throw new ArgumentNullException("createNewItemMethod");

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
                        _dictionary[key] = tsAccessorType = createNewItemMethod(key);
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