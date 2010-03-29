using System;
using System.Collections.Generic;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    internal delegate void UnsafeActionDelegate<TStorage, TItem>(
        TStorage storage, TItem[] buffer, int offset, int count, bool isWriting);

    internal delegate bool UnsafeMemCompareDelegate<TItem>(
        TItem[] buffer1, int offset1, TItem[] buffer2, int offset2, int count);

    public class DefaultTypeSerializer<T> : IBinSerializer<T>
    {
        private static readonly Version CurrentVersion = new Version(1, 0);
        private readonly UnsafeMemCompareDelegate<T> _compareArrays;
        private readonly UnsafeActionDelegate<FileStream, T> _processFileStream;
        private readonly UnsafeActionDelegate<IntPtr, T> _processMemoryMap;
        private readonly int _typeSize;

        public DefaultTypeSerializer()
        {
            DynamicCodeFactory.BinSerializerInfo info = DynamicCodeFactory.Instance.CreateSerializer<T>();

            _typeSize = info.TypeSize;

            if (_typeSize <= 0)
                throw new InvalidOperationException("Struct size must be > 0");

            _processFileStream = (UnsafeActionDelegate<FileStream, T>)
                                 info.FileStreamMethod.CreateDelegate(
                                     typeof (UnsafeActionDelegate<,>).MakeGenericType(typeof (FileStream), typeof (T)),
                                     this);
            _processMemoryMap = (UnsafeActionDelegate<IntPtr, T>)
                                info.MemMapMethod.CreateDelegate(
                                    typeof (UnsafeActionDelegate<,>).MakeGenericType(typeof (IntPtr), typeof (T)),
                                    this);
            _compareArrays = (UnsafeMemCompareDelegate<T>)
                             info.MemCompareMethod.CreateDelegate(
                                 typeof (UnsafeMemCompareDelegate<>).MakeGenericType(typeof (T)), this);
        }

        #region IBinSerializer<T> Members

        public int TypeSize
        {
            get { return _typeSize; }
        }

        public bool SupportsMemoryMappedFiles
        {
            get { return true; }
        }

        public void ProcessFileStream(FileStream fileStream, ArraySegment<T> buffer, bool isWriting)
        {
            if (fileStream == null) throw new ArgumentNullException("fileStream");
            _processFileStream(fileStream, buffer.Array, buffer.Offset, buffer.Count, isWriting);
        }

        public void ProcessMemoryMap(IntPtr memMapPtr, ArraySegment<T> buffer, bool isWriting)
        {
            if (memMapPtr == IntPtr.Zero) throw new ArgumentNullException("memMapPtr");
            _processMemoryMap(memMapPtr, buffer.Array, buffer.Offset, buffer.Count, isWriting);
        }

        public bool BinaryArrayCompare(ArraySegment<T> buffer1, ArraySegment<T> buffer2)
        {
            if (buffer1.Array == null) throw new ArgumentNullException("buffer1");
            if (buffer2.Array == null) throw new ArgumentNullException("buffer2");

            // minor optimizations
            if (buffer1.Count != buffer2.Count) return false;
            if (buffer1.Count == 0) return true;

            return _compareArrays(buffer1.Array, buffer1.Offset, buffer2.Array, buffer2.Offset, buffer1.Count);
        }

        public void ReadCustomHeader(BinaryReader reader, Version version, IDictionary<string, Type> typeMap)
        {
            if (version == CurrentVersion)
            {
                // do nothing - in this version we do not validate the signature of the struct
            }
            else
                FastBinFileUtils.ThrowUnknownVersion(version, GetType());
        }

        public Version WriteCustomHeader(BinaryWriter writer)
        {
            return CurrentVersion;

            // in the next version - will record the type signature and compare it with T
            // var typeSignature = typeof(T).GenerateTypeSignature();
        }

        #endregion

// ReSharper disable UnusedMember.Local
        private unsafe void ProcessFileStreamPtr(FileStream fileStream, void* bufPtr, int offset, int count,
                                                 bool isWriting)
// ReSharper restore UnusedMember.Local
        {
            byte* byteBufPtr = (byte*) bufPtr + offset*_typeSize;
            int byteCount = count*_typeSize;

            uint bytesProcessed = isWriting
                                      ? NativeWinApis.WriteFile(fileStream, byteBufPtr, byteCount)
                                      : NativeWinApis.ReadFile(fileStream, byteBufPtr, byteCount);

            if (bytesProcessed != byteCount)
                throw new IOException(
                    String.Format("Unable to {0} {1} bytes - only {2} bytes were available",
                                  isWriting ? "write" : "read", byteCount, bytesProcessed));
        }

// ReSharper disable UnusedMember.Local
        private unsafe void ProcessMemoryMapPtr(IntPtr memMapPtr, void* bufPtr, int offset, int count, bool isWriting)
// ReSharper restore UnusedMember.Local
        {
            byte* byteBufPtr = (byte*) bufPtr + offset*_typeSize;
            int byteCount = count*_typeSize;

            byte* src = isWriting ? byteBufPtr : (byte*) memMapPtr;
            byte* dest = isWriting ? (byte*) memMapPtr : byteBufPtr;

            FastBinFileUtils.CopyMemory(dest, src, (uint) byteCount);
        }

// ReSharper disable UnusedMember.Local
        private unsafe bool CompareMemoryPtr(void* bufPtr1, int offset1, void* bufPtr2, int offset2, int count)
// ReSharper restore UnusedMember.Local
        {
            byte* byteBufPtr1 = (byte*) bufPtr1 + offset1*_typeSize;
            byte* byteBufPtr2 = (byte*) bufPtr2 + offset2*_typeSize;

            int byteCount = count*_typeSize;

            return FastBinFileUtils.CompareMemory(byteBufPtr1, byteBufPtr2, (uint) byteCount);
        }
    }
}