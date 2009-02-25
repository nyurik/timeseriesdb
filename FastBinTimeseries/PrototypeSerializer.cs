using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    internal struct PrototypeStruct
    {
        public DateTime Timestamp;
    }

    internal sealed class PrototypeSerializer : BuiltInTypeSerializer<PrototypeStruct>
    {
        public unsafe PrototypeSerializer()
            : base(sizeof (PrototypeStruct))
        {
        }

        public override unsafe void ProcessFileStream(FileStream fileHandle, PrototypeStruct[] buffer, int offset,
                                                      int count, bool isWriting)
        {
            fixed (void* bufPtr = &buffer[0])
            {
                ProcessFileStreamPtr(fileHandle, bufPtr, offset, count, isWriting);
            }
        }

        public override unsafe void ProcessMemoryMap(IntPtr memMapPtr, PrototypeStruct[] buffer, int offset,
                                                     int count, bool isWriting)
        {
            fixed (void* bufPtr = &buffer[0])
            {
                ProcessMemoryMapPtr(memMapPtr, bufPtr, offset, count, isWriting);
            }
        }
    }
}