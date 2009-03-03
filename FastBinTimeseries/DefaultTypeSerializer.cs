using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    internal delegate void UnsafeActionDelegate<TStorage, TItem>(
        TStorage storage, TItem[] buffer, int offset, int count, bool isWriting);

    internal class DefaultTypeSerializer<T> : IBinSerializer<T>
    {
        private static readonly Version CurrentVersion = new Version(1, 0);
        private readonly int _typeSize;
        private readonly UnsafeActionDelegate<FileStream, T> processFileStream;
        private readonly UnsafeActionDelegate<IntPtr, T> processMemoryMap;

        public DefaultTypeSerializer()
        {
            var info = DynamicCodeFactory.Instance.CreateSerializer<T>();

            if (info.ItemSize <= 0)
                throw new InvalidOperationException("Struct size must be > 0");

            _typeSize = info.ItemSize;
            processFileStream = (UnsafeActionDelegate<FileStream, T>)
                                info.FileStreamMethod.CreateDelegate(
                                    typeof (UnsafeActionDelegate<,>).MakeGenericType(typeof (FileStream), typeof (T)),
                                    this);
            processMemoryMap = (UnsafeActionDelegate<IntPtr, T>)
                               info.MemMapMethod.CreateDelegate(
                                   typeof (UnsafeActionDelegate<,>).MakeGenericType(typeof (IntPtr), typeof (T)),
                                   this);
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

        public void ProcessFileStream(FileStream fileStream, T[] buffer, int offset, int count, bool isWriting)
        {
            processFileStream(fileStream, buffer, offset, count, isWriting);
        }

        public void ProcessMemoryMap(IntPtr memMapPtr, T[] buffer, int offset, int count, bool isWriting)
        {
            processMemoryMap(memMapPtr, buffer, offset, count, isWriting);
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

        protected unsafe void ProcessFileStreamPtr(FileStream fileStream, void* bufPtr, int offset, int count,
                                                   bool isWriting)
        {
            var byteBufPtr = (byte*) bufPtr + offset*_typeSize;
            var byteCount = count*_typeSize;

            var bytesProcessed = isWriting
                                     ? Win32Apis.WriteFile(fileStream, byteBufPtr, byteCount)
                                     : Win32Apis.ReadFile(fileStream, byteBufPtr, byteCount);

            if (bytesProcessed != byteCount)
                throw new IOException(
                    String.Format("Unable to {0} {1} bytes - only {2} bytes were available",
                                  isWriting ? "write" : "read", byteCount, bytesProcessed));
        }

        protected unsafe void ProcessMemoryMapPtr(IntPtr memMapPtr, void* bufPtr, int offset, int count, bool isWriting)
        {
            var byteBufPtr = (byte*) bufPtr + offset*_typeSize;
            var byteCount = count*_typeSize;

            var src = isWriting ? byteBufPtr : (byte*) memMapPtr;
            var dest = isWriting ? (byte*) memMapPtr : byteBufPtr;

            Utilities.CopyMemory(dest, src, (uint) byteCount);
        }
    }
}