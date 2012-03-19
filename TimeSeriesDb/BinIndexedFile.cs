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
using System.IO;
using JetBrains.Annotations;
using NYurik.TimeSeriesDb.Common;

namespace NYurik.TimeSeriesDb
{
    /// <summary>
    /// Very simple 0-based int64 index implementation
    /// </summary>
    public class BinIndexedFile<T> : BinaryFile<T>
    {
        #region Constructors

        /// <summary>
        /// Allow Activator non-public instantiation
        /// </summary>
        [UsedImplicitly]
        protected BinIndexedFile()
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.
        ///   If less than a day, the day must be evenly divisible by this value</param>
        public BinIndexedFile(string fileName)
            : base(fileName)
        {
        }

        #endregion

        // ReSharper disable StaticFieldInGenericType
        private static readonly Version Version10 = new Version(1, 0);
        // ReSharper restore StaticFieldInGenericType

        /// <summary>
        /// Enumerate items by block either in order or in reverse order, begining at the <paramref name="firstItemIdx"/>.
        /// </summary>
        /// <param name="firstItemIdx">The index of the first block to read (both forward and backward). Invalid values will be adjusted to existing data.</param>
        /// <param name="enumerateInReverse">Set to true to enumerate in reverse, false otherwise</param>
        /// <param name="bufferProvider">Provides buffers (or re-yields the same buffer) for each new result. Could be null for automatic</param>
        /// <param name="maxItemCount"></param>
        public IEnumerable<ArraySegment<T>> StreamSegments(
            long firstItemIdx, bool enumerateInReverse,
            IEnumerable<Buffer<T>> bufferProvider = null,
            long maxItemCount = long.MaxValue)
        {
            return PerformStreaming(firstItemIdx, enumerateInReverse, bufferProvider, maxItemCount);
        }

        /// <summary>
        /// Write segment stream to internal stream, optionally truncating the file so that <paramref name="firstItemIdx"/> would be the first written item.
        /// </summary>
        /// <param name="stream">The stream of array segments to write</param>
        /// <param name="firstItemIdx">The index of the first element in the stream. The file will be truncated if the value is less than or equal to Count</param>
        public void WriteStream(IEnumerable<ArraySegment<T>> stream, long firstItemIdx = long.MaxValue)
        {
            using (IEnumerator<ArraySegment<T>> streamEnmr = stream.GetEnumerator())
                if (streamEnmr.MoveNext())
                    PerformWriteStreaming(streamEnmr, firstItemIdx);
        }

        protected override Version Init(BinaryReader reader, Func<string, Type> typeResolver)
        {
            Version ver = reader.ReadVersion();
            if (ver != Version10)
                throw new IncompatibleVersionException(GetType(), ver);
            return ver;
        }

        protected override Version WriteCustomHeader(BinaryWriter writer)
        {
            writer.WriteVersion(Version10);
            return Version10;
        }
    }
}