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
using NYurik.TimeSeriesDb.Common;

namespace NYurik.TimeSeriesDb
{
    [Obsolete("Uniform data files are no longer supported")]
    public interface IBinUniformTimeseriesFile : IBinaryFile, IStoredUniformTimeseries
    {
        /// <summary>
        /// Adjusts the date range to the ones that exist in the file
        /// </summary>
        /// <param name="fromInclusive">Will get rounded up to the next closest existing item.
        /// If no data beyond this point, date will only be adjusted to the next valid timestamp, and 0 is returned</param>
        /// <param name="toExclusive">Will get rounded up to the next closest existing item or first unavailable.
        /// If no data beyond this point, date will only be adjusted to the next valid timestamp, and 0 is returned</param>
        /// <returns>Number of items that exist between new range</returns>
        int AdjustRangeToExistingData(ref UtcDateTime fromInclusive, ref UtcDateTime toExclusive);

        /// <summary>
        /// Generic version of <see cref="BinUniformTimeseriesFile{T}.WriteData"/>.
        /// Write an array of items to the file.
        /// </summary>
        /// <param name="firstItemIndex">The index of the first value in the <paramref name="buffer"/> array.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        /// <param name="offset">The zero-based index of the first element in the range.</param>
        /// <param name="count">The number of elements in the range.</param>
        void GenericWriteData(UtcDateTime firstItemIndex, Array buffer, int offset, int count);

        /// <summary>
        /// Truncate existing file to the new date.
        /// </summary>
        /// <param name="newFirstUnavailableTimestamp">Must be less then or equal to 
        /// <see cref="IStoredUniformTimeseries.FirstUnavailableTimestamp"/></param>
        void TruncateFile(UtcDateTime newFirstUnavailableTimestamp);
    }
}