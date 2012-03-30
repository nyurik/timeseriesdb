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

namespace NYurik.TimeSeriesDb.Common
{
    static internal class FeedUtils
    {
        internal static bool IsDefault<TInd>(TInd value)
            where TInd : IComparable<TInd>
        {
            // For value types, it is safe to call IComparable.CompareTo(default) method
            // For refs or interfaces we should only check for null

            // ReSharper disable CompareNonConstrainedGenericWithNull
            return default(TInd) != null
                       ? value.CompareTo(default(TInd)) == 0
                       : value == null;
            // ReSharper restore CompareNonConstrainedGenericWithNull
        }

        public static void AssertPositiveIndex<TInd>(TInd value)
            where TInd : IComparable<TInd>
        {
            if (
                // ReSharper disable CompareNonConstrainedGenericWithNull
                default(TInd) != null
                // ReSharper restore CompareNonConstrainedGenericWithNull
                && value.CompareTo(default(TInd)) < 0)
            {
                throw new BinaryFileException(
                    "Value index '{0}' may not be less than the default '{1}' (negative indexes are not supported",
                    value, default(TInd));
            }
        }

    }
}