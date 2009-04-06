using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    internal delegate void UnsafeActionDelegate<TStorage, TItem>(
        TStorage storage, TItem[] buffer, int offset, int count, bool isWriting);

    internal class DefaultTypeSerializer<T> : IBinSerializer<T>
    {
        private static readonly Version CurrentVersion = new Version(1, 0);
        private readonly UnsafeActionDelegate<FileStream, T> m_processFileStream;
        private readonly UnsafeActionDelegate<IntPtr, T> m_processMemoryMap;
        private readonly int m_typeSize;

        public DefaultTypeSerializer()
        {
            DynamicCodeFactory.BinSerializerInfo info = DynamicCodeFactory.Instance.CreateSerializer<T>();

            m_typeSize = info.TypeSize;

            if (m_typeSize <= 0)
                throw new InvalidOperationException("Struct size must be > 0");

            m_processFileStream = (UnsafeActionDelegate<FileStream, T>)
                                  info.FileStreamMethod.CreateDelegate(
                                      typeof (UnsafeActionDelegate<,>).MakeGenericType(typeof (FileStream), typeof (T)),
                                      this);
            m_processMemoryMap = (UnsafeActionDelegate<IntPtr, T>)
                                 info.MemMapMethod.CreateDelegate(
                                     typeof (UnsafeActionDelegate<,>).MakeGenericType(typeof (IntPtr), typeof (T)),
                                     this);
        }

        #region IBinSerializer<T> Members

        public int TypeSize
        {
            get { return m_typeSize; }
        }

        public bool SupportsMemoryMappedFiles
        {
            get { return true; }
        }

        public void ProcessFileStream(FileStream fileStream, T[] buffer, int offset, int count, bool isWriting)
        {
            m_processFileStream(fileStream, buffer, offset, count, isWriting);
        }

        public void ProcessMemoryMap(IntPtr memMapPtr, T[] buffer, int offset, int count, bool isWriting)
        {
            m_processMemoryMap(memMapPtr, buffer, offset, count, isWriting);
        }

        public void ReadCustomHeader(BinaryReader reader, Version version)
        {
            if (version == CurrentVersion)
            {
                // do nothing - in this version we do not validate the signature of the struct
            }
            else
                Utilities.ThrowUnknownVersion(version, GetType());
        }

        public Version WriteCustomHeader(BinaryWriter writer)
        {
            return CurrentVersion;

            // in the next version - will record the type signature and compare it with T
            // var typeSignature = typeof(T).GenerateTypeSignature();
        }

        #endregion

// ReSharper disable UnusedMember.Local
        private unsafe void ProcessFileStreamPtr(FileStream fileStream, void* bufPtr, int offset, int count, bool isWriting)
// ReSharper restore UnusedMember.Local
        {
            byte* byteBufPtr = (byte*) bufPtr + offset*m_typeSize;
            int byteCount = count*m_typeSize;

            uint bytesProcessed = isWriting
                                      ? Win32Apis.WriteFile(fileStream, byteBufPtr, byteCount)
                                      : Win32Apis.ReadFile(fileStream, byteBufPtr, byteCount);

            if (bytesProcessed != byteCount)
                throw new IOException(
                    String.Format("Unable to {0} {1} bytes - only {2} bytes were available",
                                  isWriting ? "write" : "read", byteCount, bytesProcessed));
        }

// ReSharper disable UnusedMember.Local
        private unsafe void ProcessMemoryMapPtr(IntPtr memMapPtr, void* bufPtr, int offset, int count, bool isWriting)
// ReSharper restore UnusedMember.Local
        {
            byte* byteBufPtr = (byte*) bufPtr + offset*m_typeSize;
            int byteCount = count*m_typeSize;

            byte* src = isWriting ? byteBufPtr : (byte*) memMapPtr;
            byte* dest = isWriting ? (byte*) memMapPtr : byteBufPtr;

            Utilities.CopyMemory(dest, src, (uint) byteCount);
        }
    }
}