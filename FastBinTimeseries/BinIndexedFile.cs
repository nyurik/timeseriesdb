using System;
using System.Collections.Generic;
using System.IO;
using NYurik.FastBinTimeseries.Serializers;

namespace NYurik.FastBinTimeseries
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

        private static readonly Version Version10 = new Version(1, 0);

        /// <summary>
        /// Read enough items to fill the <paramref name="buffer"/>, starting at <paramref name="firstItemIndex"/>.
        /// </summary>
        /// <param name="firstItemIndex">Index of the item to start from.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        public void ReadData(long firstItemIndex, ArraySegment<T> buffer)
        {
            PerformFileAccess(firstItemIndex, buffer, false);
        }

        /// <summary>
        /// Write an array of items to the file.
        /// </summary>
        /// <param name="firstItemIndex">The index of the first value in the <paramref name="buffer"/> array.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        public void WriteData(long firstItemIndex, ArraySegment<T> buffer)
        {
            PerformFileAccess(firstItemIndex, buffer, true);
        }

        /// <summary>
        /// Enumerate items by block either in order or in reverse order, begining at the <paramref name="firstItemIdx"/>.
        /// </summary>
        /// <param name="firstItemIdx">The index of the first block to read (both forward and backward). Invalid values will be adjusted to existing data.</param>
        /// <param name="enumerateInReverse">Set to true to enumerate in reverse, false otherwise</param>
        /// <param name="bufferSize">The size of the internal buffer to read data. Set to 0 to make internal buffer autogrow with time</param>
        public IEnumerable<ArraySegment<T>> StreamSegments(long firstItemIdx, bool enumerateInReverse, int bufferSize)
        {
            return PerformStreaming(firstItemIdx, enumerateInReverse, bufferSize);
        }

        protected override Version Init(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            var ver = reader.ReadVersion();
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