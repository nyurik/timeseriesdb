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

using System;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public interface IStoredUniformTimeseries : IStoredSeries
    {
        /// <summary>
        /// The timestamp of the first item in the file.
        /// </summary>
        UtcDateTime FirstTimestamp { get; }

        /// <summary>
        /// Represents the timestamp of the first value beyond the end of the existing data.
        /// (<see cref="IStoredSeries.GetItemCount"/> as a timestamp)
        /// </summary>
        UtcDateTime FirstUnavailableTimestamp { get; }

        /// <summary>
        /// Span of time each item represents.
        /// </summary>
        TimeSpan ItemTimeSpan { get; }

        /// <summary>
        /// Generic version of <see cref="BinUniformTimeseriesFile{T}.ReadData(UtcDateTime,UtcDateTime)"/>.
        /// Read data starting at <paramref name="fromInclusive"/>, up to, but not including <paramref name="toExclusive"/>,
        /// and return an <see cref="Array"/> object. 
        /// </summary>
        Array GenericReadData(UtcDateTime fromInclusive, UtcDateTime toExclusive);

        /// <summary>
        /// Read <paramref name="count"/> items starting at <paramref name="fromInclusive"/>.
        /// and return an <see cref="Array"/> object. 
        /// </summary>
        /// <param name="fromInclusive">Index of the item to start from.</param>
        /// <param name="count">The number of items to be read.</param>
        Array GenericReadData(UtcDateTime fromInclusive, int count);
    }
}