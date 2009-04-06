using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    public interface IBinSerializer
    {
        /// <summary>
        /// The size of each data element
        /// </summary>
        int TypeSize { get; }

        /// <summary>
        /// Will be set to true when this provider supports reading and writing from fast memory mapped files
        /// </summary>
        bool SupportsMemoryMappedFiles { get; }

        /// <summary>
        /// Optionaly initialize this serializer from a binary reader. Must match all actions by <see cref="WriteCustomHeader"/>
        /// </summary>
        void ReadCustomHeader(BinaryReader reader, Version version);

        /// <summary>
        /// Optionaly save this serializer's parameters to a binary writer. Must match all actions by <see cref="ReadCustomHeader"/>
        /// </summary>
        Version WriteCustomHeader(BinaryWriter writer);
    }

    public interface IBinSerializer<T> : IBinSerializer
    {
        /// <summary>
        /// Read or write <paramref name="count"/> items from/to <paramref name="fileStream"/> 
        /// into/from <paramref name="buffer"/> starting at <paramref name="offset"/>
        /// </summary>
        /// <param name="fileStream">FileStream already positioned at the point in the file from which to begin</param>
        /// <param name="buffer">Buffer to fill with/read from values</param>
        /// <param name="offset">The offset in buffer at which to start</param>
        /// <param name="count">Number of values to read/write</param>
        /// <param name="isWriting">True when buffer should be written into a file, false - when reading into the buffer</param>
        void ProcessFileStream(FileStream fileStream, T[] buffer, int offset, int count, bool isWriting);

        /// <summary>
        /// Read/Write <paramref name="count"/> items from/to the memory pointer <paramref name="memMapPtr"/> 
        /// into/from <paramref name="buffer"/> starting at <paramref name="offset"/>
        /// </summary>
        /// <param name="memMapPtr">A pointer to unmanaged memory mapped to the file</param>
        /// <param name="buffer">Buffer to fill with/read from values</param>
        /// <param name="offset">The offset in buffer at which to start</param>
        /// <param name="count">Number of values to read/write</param>
        /// <param name="isWriting">True when buffer should be written into a file, false - when reading into the buffer</param>
        void ProcessMemoryMap(IntPtr memMapPtr, T[] buffer, int offset, int count, bool isWriting);
    }
}