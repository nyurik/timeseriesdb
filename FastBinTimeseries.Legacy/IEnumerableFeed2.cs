#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
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

using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public interface IEnumerableFeed<T> : IEnumerableFeed<UtcDateTime, T>
    {
//        /// <summary>
//        /// Returns function that can extract timestamp from a given value T
//        /// </summary>
//        Func<T, UtcDateTime> TimestampAccessor { get; }
//
//
//        /// <summary>
//        /// Enumerate all items one block at a time using an internal buffer.
//        /// </summary>
//        /// <param name="from">The index of the first element to read. Inclusive if going forward, exclusive when going backwards</param>
//        /// <param name="inReverse">Set to true if you want to enumerate backwards, from last to first</param>
//        /// <param name="bufferSize">Size of the read buffer. If 0, the buffer will start small and grow with time</param>
//        IEnumerable<ArraySegment<T>> StreamSegments(UtcDateTime from, bool inReverse = false, int bufferSize = 0);
    }
}