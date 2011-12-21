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

namespace NYurik.FastBinTimeseries.CommonCode
{
    public interface ISeries
    {
        int Count { get; }
        Type GetElementType();
        object GetValueSlow(int index);
    }

    public interface ISeries<T> : ISeries
    {
        T this[int index] { get; }
    }

    public interface ITimeSeries : ISeries
    {
        /// <summary>
        /// Performs search for specified timestamp. In the worst case it'll be a binary search.
        /// </summary>
        /// <param name="timestamp">The timestamp.</param>
        /// <returns>The zero-based index of item, if item is found; otherwise, a negative number that is the bitwise complement of the index of the next element that is larger than item or, if there is no larger element, the bitwise complement of <see cref="ISeries.Count"/>.</returns>
        int BinarySearch(UtcDateTime timestamp);

        UtcDateTime GetTimestamp(int index);
    }

    public interface ITimeSeries<T> : ITimeSeries, ISeries<T>
    {
    }
}