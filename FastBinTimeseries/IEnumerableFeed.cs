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
using JetBrains.Annotations;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public interface IEnumerableFeed : IGenericInvoker, IDisposable
    {
        /// <summary> Type of the items stored in this file </summary>
        Type ItemType { get; }

        /// <summary> User string stored in the header </summary>
        string Tag { get; }
    }

    public interface IEnumerableFeed<TInd, TVal> : IEnumerableFeed
        where TInd : IComparable<TInd>
    {
        /// <summary>
        /// Returns function that can extract TInd index from a given value T
        /// </summary>
        Func<TVal, TInd> IndexAccessor { get; }

        /// <summary>
        /// Returns true if this file is empty
        /// </summary>
        bool IsEmpty { get; }

        /// <summary>
        /// If available, returns the first index of the feed, or default(TInd) if empty
        /// </summary>
        TInd FirstIndex { get; }

        /// <summary>
        /// If available, returns the last index of the feed, or default(TInd) if empty
        /// </summary>
        TInd LastIndex { get; }

        /// <summary>
        /// False if more than one identical index is allowed in the feed, True otherwise
        /// </summary>
        bool UniqueIndexes { get; }

        /// <summary>
        /// Enumerate all items one block at a time using an internal buffer.
        /// </summary>
        /// <param name="fromInd">The index of the first element to read. Inclusive if going forward, exclusive when going backwards</param>
        /// <param name="inReverse">Set to true if you want to enumerate backwards, from last to first</param>
        /// <param name="bufferProvider">Provides buffers (or re-yields the same buffer) for each new result. Could be null for automatic</param>
        /// <param name="maxItemCount">Maximum number of items to return</param>
        IEnumerable<ArraySegment<TVal>> StreamSegments(TInd fromInd, bool inReverse = false,
                                                       IEnumerable<Buffer<TVal>> bufferProvider = null,
                                                       long maxItemCount = long.MaxValue);

        /// <summary>
        /// Add new items at the end of the existing file
        /// </summary>
        void AppendData([NotNull] IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncation = false);
    }

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