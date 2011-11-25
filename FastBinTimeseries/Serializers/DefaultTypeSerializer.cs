using System;
using System.Collections.Generic;
using System.IO;
using JetBrains.Annotations;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries.Serializers;

namespace NYurik.FastBinTimeseries.Serializers
{
    internal delegate int UnsafeActionDelegate<in TStorage, in TItem>(
        TStorage storage, TItem[] buffer, int offset, int count, bool isWriting);

    internal delegate bool UnsafeMemCompareDelegate<in TItem>(
        TItem[] buffer1, int offset1, TItem[] buffer2, int offset2, int count);
}

// For legacy compatibility, keep DefaultTypeSerializer in the root namespace

namespace NYurik.FastBinTimeseries
{
    public class DefaultTypeSerializer<T> : Initializable, IBinSerializer<T>
    {
        // ReSharper disable StaticFieldInGenericType
        private static readonly Version Version10 = new Version(1, 0);
        private static readonly Version Version11 = new Version(1, 1);
        // ReSharper restore StaticFieldInGenericType

        private readonly UnsafeMemCompareDelegate<T> _compareArrays;
        private readonly UnsafeActionDelegate<FileStream, T> _processFileStream;
        private readonly UnsafeActionDelegate<IntPtr, T> _processMemoryPtr;

        private readonly int _typeSize;
        private Version _version;

        public DefaultTypeSerializer()
        {
            DynamicCodeFactory.BinSerializerInfo info = DynamicCodeFactory.Instance.Value.CreateSerializer<T>();

            _version = Version11;
            _typeSize = info.TypeSize;

            if (_typeSize <= 0)
                throw new InvalidOperationException("Struct size must be > 0");

            _processFileStream = (UnsafeActionDelegate<FileStream, T>)
                                 info.FileStreamMethod.CreateDelegate(
                                     typeof (UnsafeActionDelegate<,>).MakeGenericType(typeof (FileStream), typeof (T)),
                                     this);
            _processMemoryPtr = (UnsafeActionDelegate<IntPtr, T>)
                                info.MemPtrMethod.CreateDelegate(
                                    typeof (UnsafeActionDelegate<,>).MakeGenericType(typeof (IntPtr), typeof (T)),
                                    this);
            _compareArrays = (UnsafeMemCompareDelegate<T>)
                             info.MemCompareMethod.CreateDelegate(
                                 typeof (UnsafeMemCompareDelegate<>).MakeGenericType(typeof (T)), this);
        }

        #region IBinSerializer<T> Members

        public Version Version
        {
            get
            {
                ThrowOnNotInitialized();
                return _version;
            }
        }

        public int TypeSize
        {
            get
            {
                ThrowOnNotInitialized();
                return _typeSize;
            }
        }

        public Type ItemType
        {
            get { return typeof (T); }
        }

        public bool SupportsMemoryPtrOperations
        {
            get
            {
                ThrowOnNotInitialized();
                return true;
            }
        }

        public void InitNew(BinaryWriter writer)
        {
            ThrowOnInitialized();

            writer.WriteVersion(_version);

            if (_version >= Version11)
            {
                writer.Write(_typeSize);

                // in the next version - will record the type signature and compare it with T
                List<TypeExtensions.TypeInfo> sig = typeof (T).GenerateTypeSignature();
                writer.Write(sig.Count);
                foreach (TypeExtensions.TypeInfo s in sig)
                {
                    writer.Write(s.Level);
                    if (s.Type == null)
                        writer.Write("!" + s.FixedBufferSize);
                    else
                        writer.WriteType(s.Type);
                }
            }

            IsInitialized = true;
        }

        public void InitExisting(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            ThrowOnInitialized();

            _version = reader.ReadVersion();
            if (_version != Version10 && _version != Version11)
                throw new IncompatibleVersionException(GetType(), _version);

            if (_version >= Version11)
            {
                // Make sure the item size has not changed
                int itemSize = reader.ReadInt32();
                if (_typeSize != itemSize)
                    throw FastBinFileUtils.GetItemSizeChangedException(this, null, itemSize);

                int fileSigCount = reader.ReadInt32();

                var fileSig = new TypeExtensions.TypeInfo[fileSigCount];
                for (int i = 0; i < fileSigCount; i++)
                {
                    int level = reader.ReadInt32();

                    string typeName;
                    bool typeRemapped;
                    int fixedBufferSize;
                    Type type = reader.ReadType(typeMap, out typeName, out typeRemapped, out fixedBufferSize);

                    fileSig[i] =
                        type != null
                            ? new TypeExtensions.TypeInfo(level, type)
                            : new TypeExtensions.TypeInfo(level, fixedBufferSize);
                }

                if (typeMap == null)
                {
                    // For now only verify without the typemap, as it might become much more complex
                    List<TypeExtensions.TypeInfo> sig = typeof (T).GenerateTypeSignature();
                    if (sig.Count != fileSig.Length)
                        throw new SerializerException(
                            "Signature subtype count mismatch: expected={0}, found={1}",
                            sig.Count, fileSig.Length);

                    for (int i = 0; i < fileSig.Length; i++)
                        if (sig[i] != fileSig[i])
                            throw new SerializerException(
                                "Signature subtype[{0}] mismatch: expected={1}, found={2}",
                                i, sig[i], fileSig[i]);
                }
            }

            IsInitialized = true;
        }

        public int ProcessFileStream(FileStream fileStream, ArraySegment<T> buffer, bool isWriting)
        {
            ThrowOnNotInitialized();
            if (fileStream == null) throw new ArgumentNullException("fileStream");
            return _processFileStream(fileStream, buffer.Array, buffer.Offset, buffer.Count, isWriting);
        }

        public void ProcessMemoryPtr(IntPtr memPointer, ArraySegment<T> buffer, bool isWriting)
        {
            ThrowOnNotInitialized();
            if (memPointer == IntPtr.Zero) throw new ArgumentNullException("memPointer");
            _processMemoryPtr(memPointer, buffer.Array, buffer.Offset, buffer.Count, isWriting);
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

        #endregion

        public static DefaultTypeSerializer<T> CreateInitialized()
        {
            return new DefaultTypeSerializer<T> {IsInitialized = true};
        }

        [UsedImplicitly]
        private unsafe int ProcessFileStreamPtr(FileStream fileStream, void* bufPtr, int offset, int count,
                                                bool isWriting)
        {
            byte* byteBufPtr = (byte*) bufPtr + offset*_typeSize;
            int byteCount = count*_typeSize;

            var bytesProcessed = (int) (isWriting
                                            ? NativeWinApis.WriteFile(fileStream, byteBufPtr, byteCount)
                                            : NativeWinApis.ReadFile(fileStream, byteBufPtr, byteCount));

            if (isWriting && bytesProcessed != byteCount)
                throw new SerializerException(
                    "Unable to write {0} bytes - only {1} bytes were done",
                    byteCount, bytesProcessed);
            if (!isWriting && bytesProcessed%_typeSize != 0)
                throw new SerializerException(
                    "Incomplete items were detected while reading: {0} items ({1} bytes) requested, {2} bytes read",
                    count, byteCount, bytesProcessed);

            return bytesProcessed/_typeSize;
        }

        [UsedImplicitly]
        private unsafe int ProcessMemoryMapPtr(IntPtr memMapPtr, void* bufPtr, int offset, int count, bool isWriting)
        {
            byte* byteBufPtr = (byte*) bufPtr + offset*_typeSize;
            int byteCount = count*_typeSize;

            byte* src = isWriting ? byteBufPtr : (byte*) memMapPtr;
            byte* dest = isWriting ? (byte*) memMapPtr : byteBufPtr;

            FastBinFileUtils.CopyMemory(dest, src, (uint) byteCount);

            return count;
        }

        [UsedImplicitly]
        private unsafe bool CompareMemoryPtr(void* bufPtr1, int offset1, void* bufPtr2, int offset2, int count)
        {
            byte* byteBufPtr1 = (byte*) bufPtr1 + offset1*_typeSize;
            byte* byteBufPtr2 = (byte*) bufPtr2 + offset2*_typeSize;

            int byteCount = count*_typeSize;

            return FastBinFileUtils.CompareMemory(byteBufPtr1, byteBufPtr2, (uint) byteCount);
        }
    }

#if IncludePrototype
    // ReSharper disable MemberCanBeMadeStatic.Local
    internal unsafe class Prototype
    {
        private int ProcessFileStreamPtr(FileStream fileStream, void* bufPtr, int offset, int count,
                                                 bool isWriting)
        { return 0; }

        public int DynProcessFileStream(FileStream fileStream, DateTime[] bufPtr, int offset, int count,
                                                 bool isWriting)
        {
            fixed (DateTime* pt = &bufPtr[0])
            {
                return ProcessFileStreamPtr(fileStream, pt, offset, count, isWriting);
            }
        }
    }
#endif
}