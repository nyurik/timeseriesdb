using System;
using System.Collections.Generic;
using System.IO;

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
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        ///   If less than a day, the day must be evenly divisible by this value</param>
        public BinIndexedFile(string fileName)
            : base(fileName)
        {
        }

        #endregion

        private static readonly Version CurrentVersion = new Version(1, 0);

        /// <summary>
        /// Read <paramref name="count"/> items starting at <paramref name="firstItemIndex"/>.
        /// </summary>
        /// <param name="firstItemIndex">Index of the item to start from.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        public void ReadData(long firstItemIndex, ArraySegment<T> buffer)
        {
            PerformRead(firstItemIndex, buffer);
        }

        /// <summary>
        /// Write an array of items to the file.
        /// </summary>
        /// <param name="firstItemIndex">The index of the first value in the <paramref name="buffer"/> array.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        public void WriteData(long firstItemIndex, ArraySegment<T> buffer)
        {
            PerformWrite(firstItemIndex, buffer);
        }

        protected override void ReadCustomHeader(BinaryReader stream, Version version, IDictionary<string, Type> typeMap)
        {
            if (version != CurrentVersion)
                FastBinFileUtils.ThrowUnknownVersion(version, GetType());
        }

        protected override Version WriteCustomHeader(BinaryWriter stream)
        {
            return CurrentVersion;
        }
    }
}