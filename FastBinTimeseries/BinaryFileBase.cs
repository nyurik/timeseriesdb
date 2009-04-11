using System;
using System.Collections.Generic;
using System.IO;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile : IBinaryFile
    {
        protected const int BytesInHeaderSize = sizeof (int);
        protected const int MaxHeaderSize = 4*1024*1024;
        protected const int MinReqSizeToUseMapView = 4*1024; // 4 KB

        protected static readonly Version BaseVersion_1_0_NoTag = new Version(1, 0);
        protected static readonly Version BaseVersion_Current = new Version(1, 1);

        private static readonly Version s_dummyVersion = new Version(int.MaxValue, int.MaxValue, int.MaxValue,
                                                                     int.MaxValue);

        private readonly string _fileName;
        private Version _baseVersion;
        private bool _canWrite;
        private bool _enableMemoryMappedFileAccess;
        private Version _fileVersion;
        private int _headerSize;
        private bool _isDisposed;
        private bool _isInitialized;
        private Version _serializerVersion;
        private string _tag = "";
        
        // These fields are accessed from the derived BinaryFile<T> class.
        protected internal long m_count;
        protected internal FileStream m_fileStream;
        protected internal int m_itemSize;

        protected BinaryFile()
        {
        }

        protected BinaryFile(string fileName)
        {
            _canWrite = true;
            _fileName = fileName;
        }

        /// <summary>
        /// All memory mapping operations must align to this value (not the dwPageSize)
        /// </summary>
        public static int MinPageSize
        {
            get { return (int) NativeWinApis.SystemInfo.dwAllocationGranularity; }
        }

        /// <summary>
        /// Maximum number of bytes to read at once
        /// </summary>
        public static int MinLargePageSize
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
                return m_fileStream;
            }
        }

        #region IBinaryFile Members

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

        public bool EnableMemoryMappedFileAccess
        {
            get { return _enableMemoryMappedFileAccess; }
            set
            {
                if (_enableMemoryMappedFileAccess != value)
                {
                    if (!NonGenericSerializer.SupportsMemoryMappedFiles && value)
                        throw new NotSupportedException("Memory mapped files are not supported by the serializer");
                    _enableMemoryMappedFileAccess = value;
                }
            }
        }

        public string FileName
        {
            get { return _fileName; }
        }

        public Version BaseVersion
        {
            get
            {
                ThrowOnNotInitialized();
                return _baseVersion;
            }
            protected internal set
            {
                ThrowOnInitialized();
                if (value == null) throw new ArgumentNullException("value");
                if (value != BaseVersion_Current && value != BaseVersion_1_0_NoTag)
                    FastBinFileUtils.ThrowUnknownVersion(value, typeof (BinaryFile));
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
            protected internal set
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

        public Version SerializerVersion
        {
            get
            {
                ThrowOnNotInitialized();
                return _serializerVersion;
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

        public void Close()
        {
            ((IDisposable) this).Dispose();
        }

        void IDisposable.Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

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
                return Open(stream, typeMap);
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
            Version baseVersion = FastBinFileUtils.ReadVersion(memReader);

            string classTypeName = memReader.ReadString();
            Type classType;
            if (typeMap == null || !typeMap.TryGetValue(classTypeName, out classType))
                classType = TypeUtils.GetTypeFromAnyAssemblyVersion(classTypeName);
            if (classType == null)
                throw new InvalidOperationException("Unable to find class type " + classTypeName);
            var inst = (BinaryFile) Activator.CreateInstance(classType, true);

            inst.HeaderSize = hdrSize;
            inst.BaseVersion = baseVersion;
            inst.m_fileStream = stream;

            // Read values in the same order as WriteHeader()
            // Serializer
            string serializerTypeName = memReader.ReadString();
            Type serializerType;
            if (typeMap == null || !typeMap.TryGetValue(serializerTypeName, out serializerType))
                serializerType = TypeUtils.GetTypeFromAnyAssemblyVersion(serializerTypeName);
            if (serializerType == null)
                throw new InvalidOperationException("Unable to find serializer type " + serializerTypeName);

            inst._canWrite = stream.CanWrite;
            var serializer = (IBinSerializer) Activator.CreateInstance(serializerType);
            inst.SetSerializer(serializer);

            inst.EnableMemoryMappedFileAccess = serializer.SupportsMemoryMappedFiles;

            // Make sure the item size has not changed
            int serializerTypeSize = serializer.TypeSize;
            int itemSize = memReader.ReadInt32();
            if (itemSize != serializerTypeSize)
                throw new InvalidOperationException(
                    string.Format(
                        "The file of type {0} was created with itemSize={1}, but now the itemSize={2}",
                        inst.GetType().FullName, itemSize, inst.ItemSize));
            inst.m_itemSize = serializerTypeSize;

            if (inst._baseVersion > BaseVersion_1_0_NoTag)
                inst.Tag = memReader.ReadString();

            inst._fileVersion = FastBinFileUtils.ReadVersion(memReader);
            inst.ReadCustomHeader(memReader, inst._fileVersion, typeMap);

            inst._serializerVersion = FastBinFileUtils.ReadVersion(memReader);
            serializer.ReadCustomHeader(memReader, inst._serializerVersion, typeMap);

            inst.m_count = inst.CalculateItemCountFromFilePosition(inst.m_fileStream.Length);

            inst._isInitialized = true;

            return inst;
        }

        /// <summary>
        /// This method must be called for all new files (object created with the constructor) before usage.
        /// </summary>
        public void InitializeNewFile()
        {
            ThrowOnDisposed();
            if (IsInitialized)
                throw new InvalidOperationException(
                    "InitializeNewFile() can only be called once for new files before performing any other operations");

            // This call does not change the state, so no need to invalidate this object
            // This call must be left outside the following try-catch block, 
            // because file-already-exists exception would cause the deletion of that file.
            m_fileStream = new FileStream(FileName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);

            try
            {
                WriteHeader();
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
                    FileStream streamTmp = m_fileStream;
                    m_fileStream = null;
                    if (streamTmp != null)
                        streamTmp.Close();
                }
                else
                    m_fileStream = null;

                _isDisposed = true;
            }
        }

        ~BinaryFile()
        {
            Dispose(false);
        }

        protected internal abstract void SetSerializer(IBinSerializer nonGenericSerializer);

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

        /// <summary>Size of the file header expressed as a number of items</summary>
        protected int CalculateHeaderSizeAsItemCount()
        {
            return _headerSize/m_itemSize;
        }

        /// <summary>Calculates the number of items that would make up the given file size</summary>
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

        protected long ItemIdxToOffset(long itemIdx)
        {
            long adjIndex = itemIdx + CalculateHeaderSizeAsItemCount();
            return adjIndex*m_itemSize;
        }

        public override string ToString()
        {
            return string.Format("{0} file {1} of type {2}{3}",
                                 IsDisposed ? "Disposed" : (IsInitialized ? "Open" : "Uninitialized"),
                                 m_fileStream == null ? "(unknown)" : m_fileStream.Name,
                                 GetType().FullName,
                                 IsOpen ? string.Format(" with {0} items", Count) : "");
        }

        /// <summary>
        /// Write the header info into the begining of the file.
        /// This method must match the reading sequence in the
        /// <see cref="Open(System.IO.FileStream,System.Collections.Generic.IDictionary{string,System.Type})"/>.
        /// </summary>
        /// <remarks>
        ///            ***  Header structure: ***
        /// int32   HeaderSize
        /// Version BinFile main version
        /// string  BinaryFile...<...> type name
        /// string  Serializer type name
        /// int32   ItemSize
        /// string  User-provided tag (non-null)
        /// Version BinFile custom header version
        /// ...     BinFile custom header
        /// Version Serializer version
        /// ...     Serializer custom header
        /// </remarks>
        private void WriteHeader()
        {
            var memStream = new MemoryStream();
            var memWriter = new BinaryWriter(memStream);

            //
            // Serialize header values
            //

            // Header size will be replaced by an actual length later
            memWriter.Write(0);
            FastBinFileUtils.WriteVersion(memWriter, BaseVersion_Current);
            BaseVersion = BaseVersion_Current;

            memWriter.Write(GetType().AssemblyQualifiedName);
            memWriter.Write(NonGenericSerializer.GetType().AssemblyQualifiedName);

            // Make sure the item size will not change
            memWriter.Write(m_itemSize);

            // User tag
            memWriter.Write(Tag);

            // Save versions and custom headers
            _fileVersion = WriteHeaderWithVersion(memWriter, WriteCustomHeader);
            _serializerVersion = WriteHeaderWithVersion(memWriter, NonGenericSerializer.WriteCustomHeader);

            // Header size must be dividable by the item size
            var headerSize = (int) FastBinFileUtils.RoundUpToMultiple(memWriter.BaseStream.Position, m_itemSize);

            // Override the header size value at the first position of the header
            memWriter.Seek(0, SeekOrigin.Begin);
            memWriter.Write(headerSize);

            HeaderSize = headerSize;

            if (m_fileStream.Position != 0)
                throw new InvalidOperationException("Expected to be at the stream position 0");
            m_fileStream.Write(memStream.GetBuffer(), 0, headerSize);
            m_fileStream.Flush();
        }

        /// <summary>
        /// Write version plus custom header generated by the writeHeaderMethod into the stream
        /// </summary>
        private static Version WriteHeaderWithVersion(BinaryWriter memWriter,
                                                      Func<BinaryWriter, Version> writeHeaderMethod)
        {
            // Record original postition and write dummy version
            long versionPos = memWriter.BaseStream.Position;
            FastBinFileUtils.WriteVersion(memWriter, s_dummyVersion);

            // Write real version and save final position
            Version version = writeHeaderMethod(memWriter);
            long latestPos = memWriter.BaseStream.Position;

            // Seek back, rerecord the proper version instead of the dummy one, and move back to the end
            memWriter.BaseStream.Seek(versionPos, SeekOrigin.Begin);
            FastBinFileUtils.WriteVersion(memWriter, version);
            memWriter.BaseStream.Seek(latestPos, SeekOrigin.Begin);

            return version;
        }

        /// <summary>
        /// Override to read custom header info. Must match the <see cref="WriteCustomHeader"/>.
        /// </summary>
        protected abstract void ReadCustomHeader(BinaryReader stream, Version version, IDictionary<string, Type> typeMap);

        /// <summary>
        /// Override to write custom header info. Must match the <see cref="ReadCustomHeader"/>.
        /// Return the version number of the header.
        /// </summary>
        protected abstract Version WriteCustomHeader(BinaryWriter stream);
    }
}