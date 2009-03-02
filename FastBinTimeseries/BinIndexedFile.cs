using System;
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
            : this(fileName, null)
        {
        }

        /// <summary>
        /// Create new timeseries file. If the file already exists, an <see cref="IOException"/> is thrown.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the file to create.</param>
        ///   If less than a day, the day must be evenly divisible by this value</param>
        /// <param name="customSerializer"></param>
        public BinIndexedFile(string fileName, IBinSerializer<T> customSerializer)
            : base(fileName, customSerializer)
        {
            WriteHeader(); // Initialize header
        }

        #endregion

        /// <summary>
        /// Read <paramref name="count"/> items starting at <paramref name="firstItemIndex"/>.
        /// </summary>
        /// <param name="firstItemIndex">Index of the item to start from.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        /// <param name="offset">The offset in buffer at which to begin copying items to the file.</param>
        /// <param name="count">The number of items to be read to the array.</param>
        public void ReadData(long firstItemIndex, T[] buffer, int offset, int count)
        {
            PerformFileAccess(firstItemIndex, buffer, offset, count, false);
        }

        /// <summary>
        /// Write an array of items to the file.
        /// </summary>
        /// <param name="firstItemIndex">The index of the first value in the <paramref name="buffer"/> array.</param>
        /// <param name="buffer">Array of values to be written into a file.</param>
        /// <param name="offset">The offset in buffer at which to begin copying items to the file.</param>
        /// <param name="count">The number of items to be written from array. No action is performed when the count is 0.</param>
        public void WriteData(long firstItemIndex, T[] buffer, int offset, int count)
        {
            PerformFileAccess(firstItemIndex, buffer, offset, count, true);
        }


        protected override void ReadCustomHeader(BinaryReader stream, Version version)
        {
            if(version != CurrentVersion)
                Utilities.ThrowUnknownVersion(version, GetType());
        }

        private static readonly Version CurrentVersion = new Version(1, 0);

        protected override Version WriteCustomHeader(BinaryWriter stream)
        {
            return CurrentVersion;
        }
    }
}