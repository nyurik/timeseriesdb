using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile : IDisposable
    {
        private const int BytesInHeaderSize = sizeof (int);
        private const int MaxHeaderSize = 4*1024*1024;

        private static readonly Version BaseVersion10 = new Version(1, 0);
        private static readonly Version BaseVersion11 = new Version(1, 1);
        private static readonly Version BaseVersion12 = new Version(1, 2);

        private static readonly Version[] KnownVersions = {BaseVersion10, BaseVersion11, BaseVersion12};

        /// <summary> Base version for new files by default </summary>
        private Version _baseVersion = BaseVersion12;

        private bool _canWrite;
        private bool _enableMemMappedAccessOnRead;
        private bool _enableMemMappedAccessOnWrite;
        private string _fileName;
        private FileStream _fileStream;
        private Version _fileVersion;
        private int _headerSize;
        private bool _isDisposed;
        private bool _isInitialized;
        private string _tag = "";

        #region Fields accessed from the derived BinaryFile<T> class

        // ReSharper disable InconsistentNaming
        internal long m_count;
        internal int m_itemSize;
        // ReSharper restore InconsistentNaming

        #endregion

        protected BinaryFile()
        {
        }

        protected BinaryFile(string fileName)
        {
            _canWrite = true;
            _fileName = fileName;
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

        protected FileStream FileStream
        {
            get
            {
                ThrowOnNotInitialized();
                return _fileStream;
            }
        }

        public abstract IBinSerializer NonGenericSerializer { get; }

        public long Count
        {
            get
            {
                ThrowOnNotInitialized();
                return m_count;
            }
        }

        public int ItemSize
        {
            get
            {
                ThrowOnNotInitialized();
                return m_itemSize;
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
            get { return _enableMemMappedAccessOnRead; }
            set
            {
                if (_enableMemMappedAccessOnRead != value)
                {
                    if (!NonGenericSerializer.SupportsMemoryMappedFiles && value)
                        throw new NotSupportedException("Memory mapped files are not supported by the serializer");
                    _enableMemMappedAccessOnRead = value;
                }
            }
        }

        public bool EnableMemMappedAccessOnWrite
        {
            get { return _enableMemMappedAccessOnWrite; }
            set
            {
                if (_enableMemMappedAccessOnWrite != value)
                {
                    if (!NonGenericSerializer.SupportsMemoryMappedFiles && value)
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
                    throw FastBinFileUtils.GetUnknownVersionException(value, GetType());
                _baseVersion = value;
            }
        }

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

                if (value > MaxHeaderSize || value < BytesInHeaderSize)
                    throw new IOException(
                        String.Format("File header size {0} is not within allowed range {1}..{2}",
                                      value, BytesInHeaderSize, MaxHeaderSize));

                _headerSize = value;
            }
        }

        public bool CanWrite
        {
            get
            {
                ThrowOnDisposed();
                return _canWrite;
            }
        }

        public Version FileVersion
        {
            get
            {
                ThrowOnNotInitialized();
                return _fileVersion;
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

        public void Close()
        {
            ((IDisposable) this).Dispose();
        }

        /// <summary>
        /// Open existing binary timeseries file. A <see cref="FileNotFoundException"/> if the file does not exist.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the existing file to open.</param>
        /// <param name="canWrite">Should allow write operations</param>
        public static BinaryFile Open(string fileName, bool canWrite)
        {
            return Open(fileName, canWrite, null);
        }

        /// <summary>
        /// Open existing binary timeseries file. A <see cref="FileNotFoundException"/> if the file does not exist.
        /// </summary>
        /// <param name="fileName">A relative or absolute path for the existing file to open.</param>
        /// <param name="canWrite">Should allow write operations</param>
        /// <param name="typeMap">An optional map that would override the type strings in the file with the given types.</param>
        public static BinaryFile Open(string fileName, bool canWrite, IDictionary<string, Type> typeMap)
        {
            FileStream stream = null;
            try
            {
                stream = new FileStream(
                    fileName, FileMode.Open, canWrite ? FileAccess.ReadWrite : FileAccess.Read,
                    canWrite ? FileShare.Read : FileShare.ReadWrite);

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

            if (File.Exists(FileName))
                throw new IOException(string.Format("File {0} already exists", FileName));

            ArraySegment<byte> header = WriteHeader();

            // This call does not change the state, so no need to invalidate this object
            // This call must be left outside the following try-catch block, 
            // because file-already-exists exception would cause the deletion of that file.
            _fileStream = new FileStream(FileName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);

            try
            {
                _fileStream.Write(header.Array, header.Offset, header.Count);
                _fileStream.Flush();
                _isInitialized = true;
            }
            catch (Exception ex)
            {
                // on error, delete the newly created file and pass on the exception
                try
                {
                    Dispose(true); // invalidate object state
                    File.Delete(FileName);
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
                    FileStream streamTmp = _fileStream;
                    _fileStream = null;
                    if (streamTmp != null)
                        streamTmp.Close();
                }
                else
                    _fileStream = null;

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
            int bytesRead = stream.Read(headerBuffer, 0, bufferSize);
            if (bytesRead < bufferSize)
                throw new IOException(
                    String.Format("Unable to read a block of size {0}: only {1} bytes were available", bufferSize,
                                  bytesRead));
            return headerBuffer;
        }

        /// <summary> Size of the file header expressed as a number of items </summary>
        protected int CalculateHeaderSizeAsItemCount()
        {
            return _headerSize/m_itemSize;
        }

        /// <summary> Calculates the number of items that would make up the given file size </summary>
        protected long CalculateItemCountFromFilePosition(long position)
        {
            long items = position/m_itemSize;
            items -= CalculateHeaderSizeAsItemCount();

            if (position != ItemIdxToOffset(items))
                throw new IOException(
                    String.Format(
                        "Calculated file size should be {0}, but the size on disk is {1} ({2})",
                        ItemIdxToOffset(items), position, ToString()));

            return items;
        }

        /// <summary> Calculate file position from an item index </summary>
        protected long ItemIdxToOffset(long itemIdx)
        {
            long adjIndex = itemIdx + CalculateHeaderSizeAsItemCount();
            return adjIndex*m_itemSize;
        }

        public override string ToString()
        {
            return string.Format("{0} file {1} of type {2}{3}",
                                 IsDisposed ? "Disposed" : (IsInitialized ? "Open" : "Uninitialized"),
                                 _fileStream == null ? "(unknown)" : _fileStream.Name,
                                 GetType().FullName,
                                 IsOpen ? string.Format(" with {0} items", Count) : "");
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

            FileStream.SetLength(ItemIdxToOffset(newCount));
            FileStream.Flush();
            m_count = CalculateItemCountFromFilePosition(FileStream.Length);

            // Just in case, hope this will never happen
            if (newCount != m_count)
                throw new IOException(
                    string.Format(
                        "Internal error: the new file should have had {0} items, but was calculated to have {1}",
                        newCount, m_count));
        }

        /// <summary>
        /// Calls a factory method without explicitly specifying the type of the sub-item.
        /// </summary>
        public abstract TDst CreateWrappedObject<TDst>(IWrapperFactory factory);

        #region Header Reading/Writing

        /// <summary>
        /// Open a binary file from a filestream, and start reading the file header.
        /// This method must match the <see cref="BinaryFile.WriteHeader"/> method.
        /// </summary>
        /// <param name="stream">Stream from which to read the binary data</param>
        /// <param name="typeMap">
        /// An optional map that would override the type strings in the file with the given types.
        /// </param>
        public static BinaryFile Open(FileStream stream, IDictionary<string, Type> typeMap)
        {
            if (stream == null) throw new ArgumentNullException("stream");

            stream.Seek(0, SeekOrigin.Begin);

            // Get header size
            int hdrSize = BitConverter.ToInt32(ReadIntoNewBuffer(stream, BytesInHeaderSize), 0);

            // Read the rest of the header and create a memory reader so that we won't accidently go too far on string reads
            var memReader = new BinaryReader(
                new MemoryStream(ReadIntoNewBuffer(stream, hdrSize - BytesInHeaderSize), false));

            // Instantiate BinaryFile-inherited class this file was created with
            Version baseVersion = memReader.ReadVersion();

            BinaryFile inst;
            if (baseVersion == BaseVersion10 || baseVersion == BaseVersion11)
                inst = ReadHeaderV10(baseVersion, stream, memReader, hdrSize, typeMap);
            else if (baseVersion == BaseVersion12)
                inst = ReadHeaderV12(baseVersion, stream, memReader, hdrSize, typeMap);
            else
                throw FastBinFileUtils.GetUnknownVersionException(baseVersion, typeof (BinaryFile));

            inst.EnableMemMappedAccessOnRead = inst.NonGenericSerializer.SupportsMemoryMappedFiles;
            inst.EnableMemMappedAccessOnWrite = inst.NonGenericSerializer.SupportsMemoryMappedFiles;
            inst.m_count = inst.CalculateItemCountFromFilePosition(inst._fileStream.Length);
            inst._isInitialized = true;

            return inst;
        }

        /// <summary>
        /// Write the header info into the begining of the file.
        /// This method must match the reading sequence in the
        /// <see cref="Open(System.IO.FileStream,System.Collections.Generic.IDictionary{string,System.Type})"/>.
        /// </summary>
        private ArraySegment<byte> WriteHeader()
        {
            var memStream = new MemoryStream();
            var memWriter = new BinaryWriter(memStream);

            //
            // Serialize header values
            //

            // Header size will be replaced by an actual length later
            memWriter.Write(0);
            memWriter.WriteVersion(BaseVersion);

            if (BaseVersion == BaseVersion10 || BaseVersion == BaseVersion11)
                WriteHeaderV10(memWriter);
            else if (BaseVersion == BaseVersion12)
                WriteHeaderV12(memWriter);
            else
                throw FastBinFileUtils.GetUnknownVersionException(BaseVersion, GetType());

            // Header size must be dividable by the item size
            var headerSize = (int) FastBinFileUtils.RoundUpToMultiple(memWriter.BaseStream.Position, m_itemSize);
            if (memStream.Capacity < headerSize)
                memStream.Capacity = headerSize;

            // Override the header size value at the first position of the header
            memWriter.Seek(0, SeekOrigin.Begin);
            memWriter.Write(headerSize);

            HeaderSize = headerSize;

            return new ArraySegment<byte>(memStream.GetBuffer(), 0, headerSize);
        }

        private static BinaryFile ReadHeaderV10(Version baseVersion, FileStream stream, BinaryReader reader,
                                                int hdrSize, IDictionary<string, Type> typeMap)
        {
            var inst = reader.ReadTypeAndInstantiate<BinaryFile>(typeMap, true);

            // Read values in the same order as WriteHeader()
            // Serializer
            var serializer = reader.ReadTypeAndInstantiate<IBinSerializer>(typeMap, false);

            int itemSize = reader.ReadInt32();
            // Make sure the item size has not changed
            int serializerTypeSize = serializer.TypeSize;
            if (itemSize != serializerTypeSize)
                throw new InvalidOperationException(
                    string.Format(
                        "The file of type {0} was created with itemSize={1}, but now the itemSize={2}",
                        inst.GetType().FullName, itemSize, serializerTypeSize));

            string tag = "";
            if (baseVersion > BaseVersion10)
                tag = reader.ReadString();

            inst.HeaderSize = hdrSize;
            inst.BaseVersion = baseVersion;
            inst._fileStream = stream;
            inst._canWrite = stream.CanWrite;
            inst.SetSerializer(serializer);
            inst.m_itemSize = serializerTypeSize;
            inst.Tag = tag;

            inst._fileVersion = inst.Init(reader, typeMap);
            serializer.Init(reader, typeMap);

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
            writer.Write(m_itemSize);

            // User tag
            if (BaseVersion > BaseVersion10)
                writer.Write(Tag);

            // Save versions and custom headers
            _fileVersion = WriteCustomHeader(writer);
            NonGenericSerializer.WriteCustomHeader(writer);
        }

        private static BinaryFile ReadHeaderV12(Version baseVersion, FileStream stream, BinaryReader reader,
                                                int hdrSize, IDictionary<string, Type> typeMap)
        {
            // Tag
            string tag = reader.ReadString();

            // Serializer
            var serializer = reader.ReadTypeAndInstantiate<IBinSerializer>(typeMap, false);
            serializer.Init(reader, typeMap);

            // Make sure the item size has not changed
            int itemSize = reader.ReadInt32();
            int serializerTypeSize = serializer.TypeSize;
            if (itemSize != serializerTypeSize)
                throw new InvalidOperationException(
                    string.Format(
                        "The file with tag {0} (serializer {1} {2}) was created with itemSize={3}, but now the itemSize={4}",
                        tag, serializer.GetType().AssemblyQualifiedName, serializer.Version,
                        itemSize, serializerTypeSize));

            // BinaryFile
            var inst = reader.ReadTypeAndInstantiate<BinaryFile>(typeMap, true);
            inst.HeaderSize = hdrSize;
            inst.BaseVersion = baseVersion;
            inst._fileStream = stream;
            inst._canWrite = stream.CanWrite;
            inst.SetSerializer(serializer);
            inst.m_itemSize = serializerTypeSize;
            inst.Tag = tag;
            inst._fileVersion = inst.Init(reader, typeMap);

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
            NonGenericSerializer.WriteCustomHeader(writer);

            // Make sure the item size will not change
            writer.Write(m_itemSize);

            // Save versions and custom headers
            writer.WriteType(this);
            _fileVersion = WriteCustomHeader(writer);
        }

        #endregion
    }

    /// <summary>
    /// This interface is used to easily create wrapper objects without referencing the generic subtype
    /// </summary>
    public interface IWrapperFactory
    {
        TDst Create<TSrc, TDst, TSubType>(TSrc source);
    }
}