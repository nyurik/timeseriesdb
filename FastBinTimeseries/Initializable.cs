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

namespace NYurik.FastBinTimeseries
{
    public abstract class Initializable
    {
        private bool _isInitialized;

        public bool IsInitialized
        {
            get { return _isInitialized; }
            protected set
            {
                ThrowOnInitialized();
                _isInitialized = value;
            }
        }

        protected void ThrowOnNotInitialized()
        {
            if (!_isInitialized)
                throw new InvalidOperationException("This instance has not been initialized");
        }

        protected void ThrowOnInitialized()
        {
            if (_isInitialized)
                throw new InvalidOperationException("This instance has already been initialized");
        }
    }
}