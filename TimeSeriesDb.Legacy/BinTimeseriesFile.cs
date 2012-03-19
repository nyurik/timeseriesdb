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
using System.IO;
using System.Reflection;
using JetBrains.Annotations;
using NYurik.TimeSeriesDb.Common;

namespace NYurik.TimeSeriesDb
{
    /// <summary>
    /// Helper non-generic class aids in creating a new instance of <see cref="BinTimeseriesFile{T}"/>.
    /// </summary>
    [Obsolete("Use BinSeriesFile<TInd,TVal> instead")]
    public static class BinTimeseriesFile
    {
        /// <summary>
        /// Uses reflection to create an instance of <see cref="BinTimeseriesFile{T}"/>.
        /// </summary>
        public static IBinTimeseriesFile GenericNew(Type itemType, string fileName, FieldInfo dateTimeFieldInfo = null)
        {
            return (IBinTimeseriesFile)
                   Activator.CreateInstance(
                       typeof (BinTimeseriesFile<>).MakeGenericType(itemType),
                       fileName, dateTimeFieldInfo);
        }
    }

    /// <summary>
    /// Object representing a binary-serialized timeseries file.
    /// This is a dummy wrapper to allow backward compatibility with the UtcDateTime index.
    /// </summary>
    [Obsolete("Use BinSeriesFile<TInd,TVal> instead")]
    public class BinTimeseriesFile<T> : BinSeriesFile<UtcDateTime, T>, IBinaryFile<T>, IBinTimeseriesFile,
                                        IEnumerableFeed<T>
    {
        /// <summary>
        /// Allow Activator non-public instantiation
        /// </summary>
        [UsedImplicitly]
        protected BinTimeseriesFile()
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        /// <param name="timestampFieldInfo">Field containing the UtcDateTime timestamp, or null to get default</param>
        public BinTimeseriesFile(string fileName, FieldInfo timestampFieldInfo = null)
            : base(fileName, timestampFieldInfo)
        {
        }

        #region IBinTimeseriesFile Members

        public FieldInfo TimestampFieldInfo
        {
            get { return IndexFieldInfo; }
            set { IndexFieldInfo = value; }
        }

        public UtcDateTime? FirstFileTS
        {
            get { return IsEmpty ? (UtcDateTime?) null : FirstIndex; }
        }

        public UtcDateTime? LastFileTS
        {
            get { return IsEmpty ? (UtcDateTime?) null : LastIndex; }
        }

        public bool UniqueTimestamps
        {
            get { return UniqueIndexes; }
            set { UniqueIndexes = value; }
        }

        public new long BinarySearch(UtcDateTime timestamp)
        {
            return base.BinarySearch(timestamp);
        }

        public long BinarySearch(UtcDateTime timestamp, bool findFirst)
        {
            if (!findFirst)
                throw new NotSupportedException(
                    "Find LAST is no longer supported. Consider switching to Streaming methods");
            return base.BinarySearch(timestamp);
        }

        public new void TruncateFile(long newCount)
        {
            base.TruncateFile(newCount);
        }

        #endregion
    }
}