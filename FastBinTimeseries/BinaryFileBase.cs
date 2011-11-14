using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile : IDisposable, IGenericInvoker
    {
        private const int FileSignature = 0xBF << 24 | (byte) 'a' << 16 | (byte) 'r' << 8 | (byte) 'Y';

        private const int MaxHeaderSize = 4*1024*1024;

        private static readonly Version BaseVersion10 = new Version(1, 0);
        private static readonly Version BaseVersion11 = new Version(1, 1);
        private static readonly Version BaseVersion12 = new Version(1, 2);

        private static readonly Version[] KnownVersions = {BaseVersion10, BaseVersion11, BaseVersion12};

        /// <summary> Used by any derived classes for their explicit locking requirements </summary>
        protected readonly object LockObj = new object();

        /// <summary> Base version for new files by default </summary>
        private Version _baseVersion = BaseVersion12;

        private bool _enableMemMappedAccessOnRead;
        private bool _enableMemMappedAccessOnWrite;
        private string _fileName;
        private int _headerSize;
        private bool _isDisposed;
        private bool _isInitialized;
        private Stream _stream;
        private string _tag = "";

        /// <summary> The version of the specific file implementation </summary>
        private Version _version;

        protected BinaryFile()
        {
        }

        protected BinaryFile(string fileName)
        {
            _fileName = fileName;
        }

        public bool IsFileStream
        {
            get { return BaseStream is FileStream; }
        }

        public Stream BaseStream
        {
            get
            {
                ThrowOnNotInitialized();
                return _stream;
            }
            set
            {
                if (value == null) throw new ArgumentNullException("value");
                ThrowOnInitialized();
                _stream = value;
            }
        }

        /// <summary> All memory mapping operations must align to this value (not the dwPageSize) </summary>
        public static int MinPageSize
        {
            get { return (int) NativeWinApis.SystemInfo.dwAllocationGranularity; }
        }

        /// <summary> Maximum number of bytes to read at once </summary>
        public static int MaxLargePageSize
        {
            get
            {
                switch (NativeWinApis.SystemInfo.ProcessorInfo.wProcessorArchitecture)
                {
                    case NativeWinApis.SYSTEM_INFO.ProcArch.PROCESSOR_ARCHITECTURE_INTEL:
                        return 4*1024*1024;

                    case NativeWinApis.SYSTEM_INFO.ProcArch.PROCESSOR_ARCHITECTURE_AMD64:
                    case NativeWinApis.SYSTEM_INFO.ProcArch.PROCESSOR_ARCHITECTURE_IA64:
                        return 16*1024*1024;

                    default:
                        return 4*1024*1024;
                }
            }
        }

        public abstract IBinSerializer NonGenericSerializer { get; }

        public long Count
        {
            get
            {
                ThrowOnNotInitialized();
                if (!BaseStream.CanSeek)
                    throw new NotSupportedException("Not supported for Stream.CanSeek == false");

                bool isAligned;
                return CalculateItemCountFromFilePosition(BaseStream.Length, out isAligned);
            }
        }

        public int ItemSize
        {
            get
            {
                ThrowOnNotInitialized();
                return NonGenericSerializer.TypeSize;
            }
        }

        public abstract Type ItemType { get; }

        public string Tag
        {
            get
            {
                ThrowOnDisposed();
                return _tag;
            }
            set
            {
                ThrowOnInitialized();
                if (value == null)
                    throw new ArgumentNullException("value");
                _tag = value;
            }
        }

        public bool IsEmpty
        {
            get { return Count == 0; }
        }

        public bool IsOpen
        {
            get { return IsInitialized && !IsDisposed; }
        }

        public bool EnableMemMappedAccessOnRead
        {
            get
            {
                ThrowOnNotInitialized();
                return _enableMemMappedAccessOnRead;
            }
            set
            {
                ThrowOnNotInitialized();
                if (_enableMemMappedAccessOnRead != value)
                {
                    if (value && !NonGenericSerializer.SupportsMemoryPtrOperations)
                        throw new NotSupportedException("Memory mapped files are not supported by the serializer");
                    _enableMemMappedAccessOnRead = value;
                }
            }
        }

        public bool EnableMemMappedAccessOnWrite
        {
            get
            {
                ThrowOnNotInitialized();
                return _enableMemMappedAccessOnWrite;
            }
            set
            {
                ThrowOnNotInitialized();
                if (_enableMemMappedAccessOnWrite != value)
                {
                    if (value && !NonGenericSerializer.SupportsMemoryPtrOperations)
                        throw new NotSupportedException("Memory mapped files are not supported by the serializer");
                    _enableMemMappedAccessOnWrite = value;
                }
            }
        }

        public string FileName
        {
            get { return _fileName; }
        }

        public Version BaseVersion
        {
            get { return _baseVersion; }
            set
            {
                ThrowOnInitialized();
                if (value == null) throw new ArgumentNullException("value");
                if (!KnownVersions.Contains(value))
                    throw new IncompatibleVersionException(GetType(), value);
                _baseVersion = value;
            }
        }

        /// <summary>
        /// Size of the file header, including the signature and the size itself
        /// </summary>
        public int HeaderSize
        {
            get
            {
                ThrowOnNotInitialized();
                return _headerSize;
            }
            private set
            {
                ThrowOnInitialized();

                if (value == _headerSize)
                    return;

                ValidateHeaderSize(value);

                _headerSize = value;
            }
        }

        public bool CanWrite
        {
            get { return BaseStream.CanWrite; }
        }

        public Version Version
        {
            get
            {
                ThrowOnNotInitialized();
                return _version;
            }
        }

        public bool IsDisposed
        {
            get { return _isDisposed; }
        }

        public bool IsInitialized
        {
            get { return _isInitialized; }
        }

        #region IDisposable Members

        void IDisposable.Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region IGenericInvoker Members

        public abstract TDst RunGenericMethod<TDst, TArg>(IGenericCallable<TDst, TArg> callable, TArg arg);

        #endregion

        #region Header Reading/Writing

        /// <summary>
        /// Open a binary file from a filestream, and start reading the file header.
        /// This method must match the <see cref="CreateHeader"/> method.
        /// </summary>
        /// <param name="stream">Stream from which to read the binary data</param>
        /// <param name="typeMap">
        /// An optional map that would override the type strings in the file with the given types.
        /// </param>
        public static BinaryFile Open(Stream stream, IDictionary<string, Type> typeMap)
        {
            if (stream == null) throw new ArgumentNullException("stream");

            bool canSeek = stream.CanSeek;
            if (canSeek)
                stream.Seek(0, SeekOrigin.Begin);

            // Get header signature & size
            int hdrSigSize = sizeof (int);
            int hdrSize = BitConverter.ToInt32(ReadIntoNewBuffer(stream, sizeof (int)), 0);
            if (hdrSize == FileSignature)
            {
                hdrSize = BitConverter.ToInt32(ReadIntoNewBuffer(stream, sizeof (int)), 0);
                hdrSigSize += sizeof (int);
            }

            ValidateHeaderSize(hdrSize);

            // Read the rest of the header and create a memory reader so that we won't accidently go too far on string reads
            var memReader = new BinaryReader(
                new MemoryStream(ReadIntoNewBuffer(stream, hdrSize - hdrSigSize), false));

            // Instantiate BinaryFile-inherited class this file was created with
            Version baseVersion = memReader.ReadVersion();

            BinaryFile inst;
            if (baseVersion == BaseVersion10 || baseVersion == BaseVersion11)
                inst = ReadHeaderV10(baseVersion, stream, memReader, hdrSize, typeMap);
            else if (baseVersion == BaseVersion12)
                inst = ReadHeaderV12(baseVersion, stream, memReader, hdrSize, typeMap);
            else
                throw new IncompatibleVersionException(typeof (BinaryFile), baseVersion);

            int typeSize = inst.NonGenericSerializer.TypeSize;
            if (typeSize <= 0)
                throw new ArgumentOutOfRangeException(
                    "TypeSize" + "", typeSize, "Element size given by the serializer must be > 0");

            inst._enableMemMappedAccessOnRead = false;
            inst._enableMemMappedAccessOnWrite = false;
            inst._isInitialized = true;

            return inst;
        }

        /// <summary>
        /// Serialize header info into a memory stream and return as a byte array.
        /// This method must match the reading sequence in the
        /// <see cref="Open(System.IO.Stream,System.Collections.Generic.IDictionary{string,System.Type})"/>.
        /// </summary>
        private ArraySegment<byte> CreateHeader()
        {
            var memStream = new MemoryStream();
            var memWriter = new BinaryWriter(memStream);

            //
            // Serialize header values
            //

            // Header size will be replaced by an actual length later
            memWriter.Write(FileSignature);
            memWriter.Write(0);
            memWriter.WriteVersion(BaseVersion);

            if (BaseVersion == BaseVersion10 || BaseVersion == BaseVersion11)
                WriteHeaderV10(memWriter);
            else if (BaseVersion == BaseVersion12)
                WriteHeaderV12(memWriter);
            else
                throw new IncompatibleVersionException(GetType(), BaseVersion);

            IBinSerializer srlzr = NonGenericSerializer;
            if (srlzr.TypeSize <= 0)
                throw new BinaryFileException(
                    "Serializer {0} reported incorrect type size {1} for type {2}",
                    srlzr.GetType().AssemblyQualifiedName, srlzr.TypeSize,
                    srlzr.ItemType.AssemblyQualifiedName);

            _enableMemMappedAccessOnRead = false;
            _enableMemMappedAccessOnWrite = false;

            // Header size must be dividable by the item size
            var headerSize =
                (int) FastBinFileUtils.RoundUpToMultiple(memWriter.BaseStream.Position, srlzr.TypeSize);
            if (memStream.Capacity < headerSize)
                memStream.Capacity = headerSize;

            // Override the header size value at the 5th byte of the header.
            // The first 4 bytes are taken up by the 4 byte signature
            memWriter.Seek(sizeof (int), SeekOrigin.Begin);
            memWriter.Write(headerSize);

            HeaderSize = headerSize;

            return new ArraySegment<byte>(memStream.GetBuffer(), 0, headerSize);
        }

        private static BinaryFile ReadHeaderV10(Version baseVersion, Stream stream, BinaryReader reader,
                                                int hdrSize, IDictionary<string, Type> typeMap)
        {
            var inst = reader.ReadTypeAndInstantiate<BinaryFile>(typeMap, true);

            // Read values in the same order as CreateHeader()
            // Serializer
            var serializer = reader.ReadTypeAndInstantiate<IBinSerializer>(typeMap, false);

            int itemSize = reader.ReadInt32();

            string tag = "";
            if (baseVersion > BaseVersion10)
                tag = reader.ReadString();

            inst.HeaderSize = hdrSize;
            inst.BaseVersion = baseVersion;
            inst._stream = stream;
            inst.Tag = tag;

            // Here we do it before finishing serializer instantiation due to design before v1.2
            inst.SetSerializer(serializer);

            inst._version = inst.Init(reader, typeMap);
            serializer.InitExisting(reader, typeMap);

            // Make sure the item size has not changed
            if (itemSize != serializer.TypeSize)
                throw FastBinFileUtils.GetItemSizeChangedException(serializer, tag, itemSize);

            return inst;
        }

        /// <summary>
        /// Write header versions v1.0 and v1.1
        /// </summary>
        /// <remarks>
        /// int32   HeaderSize
        /// Version BinFile base version
        /// string  BinaryFile...&lt;...> type name
        /// string  Serializer type name
        /// int32   ItemSize
        /// string  User-provided tag (non-null, v1.1 only)
        /// ...     BinFile custom header
        /// ...     Serializer custom header
        /// </remarks>
        private void WriteHeaderV10(BinaryWriter writer)
        {
            writer.WriteType(this);
            writer.WriteType(NonGenericSerializer);

            // Make sure the item size will not change
            writer.Write(NonGenericSerializer.TypeSize);

            // User tag
            if (BaseVersion > BaseVersion10)
                writer.Write(Tag);

            // Save versions and custom headers
            _version = WriteCustomHeader(writer);
            NonGenericSerializer.InitNew(writer);
        }

        private static BinaryFile ReadHeaderV12(Version baseVersion, Stream stream, BinaryReader reader,
                                                int hdrSize, IDictionary<string, Type> typeMap)
        {
            // Tag
            string tag = reader.ReadString();

            // Serializer
            var serializer = reader.ReadTypeAndInstantiate<IBinSerializer>(typeMap, false);
            serializer.InitExisting(reader, typeMap);

            // Make sure the item size has not changed
            int itemSize = reader.ReadInt32();
            if (itemSize != serializer.TypeSize)
                throw FastBinFileUtils.GetItemSizeChangedException(serializer, tag, itemSize);

            // BinaryFile
            var inst = reader.ReadTypeAndInstantiate<BinaryFile>(typeMap, true);
            inst.HeaderSize = hdrSize;
            inst.BaseVersion = baseVersion;
            inst._stream = stream;
            inst.Tag = tag;
            inst.SetSerializer(serializer);
            inst._version = inst.Init(reader, typeMap);

            return inst;
        }

        /// <summary>
        /// Write header versions v1.2
        /// </summary>
        /// <remarks>
        /// int32   HeaderSize
        /// Version BinFile base version
        /// string  User-provided tag (non-null)
        /// string  Serializer type name
        /// ...     Serializer custom header
        /// int32   ItemSize
        /// string  BinaryFile...&lt;...> type name
        /// ...     BinFile custom header 
        /// </remarks>
        private void WriteHeaderV12(BinaryWriter writer)
        {
            // User tag
            writer.Write(Tag);

            // Serializer
            writer.WriteType(NonGenericSerializer);
            NonGenericSerializer.InitNew(writer);

            // Make sure the item size will not change
            writer.Write(NonGenericSerializer.TypeSize);

            // Save versions and custom headers
            writer.WriteType(this);
            _version = WriteCustomHeader(writer);
        }

        #endregion

        private static void ValidateHeaderSize(int value)
        {
            const int minHeaderSize = sizeof (int)*2;
            if (value > MaxHeaderSize || value < minHeaderSize)
                throw new BinaryFileException(
                    "File header size {0} is not within allowed range {1}..{2}",
                    value, minHeaderSize, MaxHeaderSize);
        }

        public void Close()
        {
            ((IDisposable) this).Dispose();
        }

        /// <summary>
        /// Open existing binary timeseries file. A <see cref="FileNotFoundException"/> if the file does not exist.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the existing file to open.</param>
        /// <param name="canWrite">Should allow write operations</param>
        /// <param name="typeMap">An optional map that would override the type strings in the file with the given types.</param>
        /// <param name="bufferSize">Buffer size as used in <see cref="FileStream"/> constructor</param>
        /// <param name="fileOptions">Options as used in <see cref="FileStream"/> constructor</param>
        public static BinaryFile Open(string fileName, bool canWrite, IDictionary<string, Type> typeMap = null,
                                      int bufferSize = 0x1000, FileOptions fileOptions = FileOptions.None)
        {
            FileStream stream = null;
            try
            {
                stream = new FileStream(
                    fileName, FileMode.Open,
                    canWrite ? FileAccess.ReadWrite : FileAccess.Read,
                    canWrite ? FileShare.Read : FileShare.ReadWrite,
                    bufferSize, fileOptions);

                BinaryFile file = Open(stream, typeMap);
                file._fileName = fileName;

                return file;
            }
            catch
            {
                if (stream != null)
                {
                    try
                    {
                        stream.Dispose();
                    }
                        // ReSharper disable EmptyGeneralCatchClause
                    catch
                    {
                        // Silent fail in order to report the original exception
                    }
                    // ReSharper restore EmptyGeneralCatchClause
                }

                throw;
            }
        }

        /// <summary> This method must be called for all new files (object created with the constructor) before usage. </summary>
        public void InitializeNewFile()
        {
            ThrowOnDisposed();
            if (IsInitialized)
                throw new InvalidOperationException(
                    "InitializeNewFile() can only be called once for new files before performing any other operations");

            string path = FileName;
            if (File.Exists(path))
                throw new IOException(string.Format("File {0} already exists", path));

            string dir = Path.GetDirectoryName(path);
            if(dir == null)
                throw new IOException(string.Format("Filename '{0}' is not valid", path));

            if (dir != "" && !Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            ArraySegment<byte> header = CreateHeader();

            // This call does not change the state, so no need to invalidate this object
            // This call must be left outside the following try-catch block, 
            // because file-already-exists exception would cause the deletion of that file.
            var s = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);

            try
            {
                s.Write(header.Array, header.Offset, header.Count);
                s.Flush();
                BaseStream = s;
                _isInitialized = true;
            }
            catch (Exception ex)
            {
                // on error, delete the newly created file and pass on the exception
                try
                {
                    Dispose(true); // invalidate object state
                    File.Delete(path);
                }
                catch (Exception ex2)
                {
                    throw new CombinedException("Failed to clean up after failed header writing", ex, ex2);
                }

                throw;
            }
        }

        protected void ThrowOnNotInitialized()
        {
            ThrowOnDisposed();
            if (!_isInitialized)
                throw new InvalidOperationException(
                    "InitializeNewFile() must be called before performing any operations on the new file");
        }

        protected void ThrowOnInitialized()
        {
            ThrowOnDisposed();
            if (_isInitialized)
                throw new InvalidOperationException(
                    "This call is only allowed for new files before InitializeNewFile() was called");
        }

        protected void ThrowOnDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(GetType().FullName, "The file has been closed");
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    Stream streamTmp = _stream;
                    _stream = null;
                    if (streamTmp != null)
                        streamTmp.Close();
                }
                else
                    _stream = null;

                _isDisposed = true;
            }
        }

        ~BinaryFile()
        {
            Dispose(false);
        }

        protected abstract void SetSerializer(IBinSerializer nonGenericSerializer);

        private static byte[] ReadIntoNewBuffer(Stream stream, int bufferSize)
        {
            var headerBuffer = new byte[bufferSize];
            int bytesRead = EnsureStreamRead(stream, new ArraySegment<byte>(headerBuffer));
            if (bytesRead < bufferSize)
                throw new IOException(
                    String.Format("Unable to read a block of size {0}: only {1} bytes were available", bufferSize,
                                  bytesRead));
            return headerBuffer;
        }

        /// <summary> Size of the file header expressed as a number of items </summary>
        protected int CalculateHeaderSizeAsItemCount()
        {
            return _headerSize/NonGenericSerializer.TypeSize;
        }

        /// <summary> Calculates the number of items that would make up the given file size </summary>
        protected long CalculateItemCountFromFilePosition(long position, out bool isAligned)
        {
            int typeSize = NonGenericSerializer.TypeSize;
            isAligned = position%typeSize == 0;
            return position/typeSize - CalculateHeaderSizeAsItemCount();
        }

        /// <summary> Calculate file position from an item index </summary>
        protected long ItemIdxToOffset(long itemIdx)
        {
            long adjIndex = itemIdx + CalculateHeaderSizeAsItemCount();
            return adjIndex*NonGenericSerializer.TypeSize;
        }

        public override string ToString()
        {
            var fileStream = _stream as FileStream;
            return string.Format("{0} file {1} of type {2}{3}",
                                 IsDisposed ? "Disposed" : (IsInitialized ? "Open" : "Uninitialized"),
                                 _stream == null
                                     ? "(unknown)"
                                     : (fileStream == null ? _stream.ToString() : fileStream.Name),
                                 GetType().FullName,
                                 fileStream != null && IsOpen && fileStream.CanSeek
                                     ? string.Format(" with {0} items", Count)
                                     : "");
        }

        /// <summary> Override to read custom header info. Must match the <see cref="WriteCustomHeader"/>. </summary>
        protected abstract Version Init(BinaryReader reader, IDictionary<string, Type> typeMap);

        /// <summary> Override to write custom header info. Must match the <see cref="Init"/>. </summary>
        /// <returns> Return the version number of the header. </returns>
        protected abstract Version WriteCustomHeader(BinaryWriter writer);

        /// <summary> Shrink file to the new size. </summary>
        /// <param name="newCount">Number of items the file should contain after this operation</param>
        protected void PerformTruncateFile(long newCount)
        {
            ThrowOnNotInitialized();
            if (newCount < 0 || newCount > Count)
                throw new ArgumentOutOfRangeException("newCount", newCount, "Must be >= 0 and <= Count");

            // Optimize empty requests
            if (Count == newCount)
                return;

            BaseStream.SetLength(ItemIdxToOffset(newCount));
            BaseStream.Flush();

            // Just in case, hope this will never happen
            if (newCount != Count)
                throw new IOException(
                    string.Format(
                        "Internal error: the new file should have had {0} items, but was calculated to have {1}",
                        newCount, Count));
        }

        public static IBinSerializer<T> GetDefaultSerializer<T>()
        {
            Type typeT = typeof (T);
            BinarySerializerAttribute[] attr = typeT.GetCustomAttributes<BinarySerializerAttribute>(false);
            if (attr.Length >= 1)
            {
                Type typeSer = attr[0].BinSerializerType;
                if (typeSer.IsGenericTypeDefinition)
                    typeSer = typeSer.MakeGenericType(typeT);

                var ser = Activator.CreateInstance(typeSer) as IBinSerializer<T>;
                if (ser == null)
                    throw new ArgumentException(
                        string.Format("Custom binary serializer for type {0} does not implement IBinSerializer<{0}>",
                                      typeT.Name));
                return ser;
            }

            // Initialize default serializer
            return new DefaultTypeSerializer<T>();
        }

        protected static int EnsureStreamRead(Stream stream, ArraySegment<byte> buffer)
        {
            int offset = buffer.Offset;
            int count = buffer.Count;

            while (true)
            {
                int c = stream.Read(buffer.Array, offset, count);
                if (c == count)
                    return buffer.Count;
                if (c == 0)
                    return offset - buffer.Offset;

                offset += c;
                count -= c;
            }
        }
    }
}