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
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb
{
    internal class CachedIndex<TInd>
        where TInd : IComparable<TInd>
    {
        private readonly Dictionary<long, TInd> _cache = new Dictionary<long, TInd>();
        private readonly int _defaultMaxCacheSize;
        private readonly Func<long> _getCount;
        private readonly Func<int> _getMaxCacheSize;
        private readonly Func<long, long, TInd> _getValueAt;
        private readonly Action<long> _onCountChange;
        private long _cachedCount;

        public CachedIndex(
            int defaultMaxCacheSize, [NotNull] Func<int> getMaxCacheSize, [NotNull] Func<long> getCount,
            [NotNull] Func<long, long, TInd> getValueAt, [NotNull] Action<long> onCountChange)
        {
            if (getCount == null) throw new ArgumentNullException("getCount");
            if (getValueAt == null) throw new ArgumentNullException("getValueAt");
            if (getMaxCacheSize == null) throw new ArgumentNullException("getMaxCacheSize");
            if (onCountChange == null) throw new ArgumentNullException("onCountChange");

            _defaultMaxCacheSize = defaultMaxCacheSize;
            _getCount = getCount;
            _getValueAt = getValueAt;
            _getMaxCacheSize = getMaxCacheSize;
            _onCountChange = onCountChange;
        }

        public long Count
        {
            get
            {
                // Always get count, and if it differs from the previous ones, reset cache
                long count = _getCount();
                if (_cachedCount != count)
                {
                    _cache.Clear();
                    _cachedCount = count;
                    _onCountChange(count);
                }
                return count;
            }
        }

        public TInd GetValueAt(long index)
        {
            long mcs = _getMaxCacheSize();
            if (mcs < 0)
                return _getValueAt(index, _cachedCount);

            TInd val;
            if (!_cache.TryGetValue(index, out val))
            {
                if (_cache.Count > (mcs == 0 ? _defaultMaxCacheSize : mcs))
                    _cache.Clear();
                _cache[index] = val = _getValueAt(index, _cachedCount);
            }
            return val;
        }
    }
}