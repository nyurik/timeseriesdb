using System;
using System.Collections.Generic;
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
        void ReadCustomHeader(BinaryReader reader, Version version, IDictionary<string, Type> typeMap);

        /// <summary>
        /// Optionaly save this serializer's parameters to a binary writer. Must match all actions by <see cref="ReadCustomHeader"/>
        /// </summary>
        Version WriteCustomHeader(BinaryWriter writer);
    }

    public interface IBinSerializer<T> : IBinSerializer
    {
        /// <summary>
        /// Read or write all items from/to <paramref name="fileStream"/> 
        /// into/from <paramref name="buffer"/>.
        /// </summary>
        /// <param name="fileStream">FileStream already positioned at the point in the file from which to begin</param>
        /// <param name="buffer">Buffer to fill with/read from values</param>
        /// <param name="isWriting">True when buffer should be written into a file, false - when reading into the buffer</param>
        void ProcessFileStream(FileStream fileStream, ArraySegment<T> buffer, bool isWriting);

        /// <summary>
        /// Read/Write items from/to the memory pointer <paramref name="memMapPtr"/> into/from <paramref name="buffer"/>
        /// </summary>
        /// <param name="memMapPtr">A pointer to unmanaged memory mapped to the file</param>
        /// <param name="buffer">Buffer to fill with/read from values</param>
        /// <param name="isWriting">True when buffer should be written into a file, false - when reading into the buffer</param>
        void ProcessMemoryMap(IntPtr memMapPtr, ArraySegment<T> buffer, bool isWriting);

        /// <summary>
        /// Compare the elements of <paramref name="buffer1"/> with <paramref name="buffer2"/>, and returns true if they are the same
        /// </summary>
        bool CompareArrays(ArraySegment<T> buffer1, ArraySegment<T> buffer2);
    }
}