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