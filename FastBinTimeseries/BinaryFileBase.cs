using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile : IBinaryFile
    {
        protected const int BytesInHeaderSize = sizeof (int);
        protected const int MaxHeaderSize = 4*1024*1024;
        protected const int MinReqSizeToUseMapView = 4*1024; // 4 KB

        protected static readonly Version BaseVersion_1_0_NoTag = new Version(1, 0);
        protected static readonly Version BaseVersion_Current = new Version(1, 1);

        private readonly string m_fileName;
        private Version m_baseVersion;
        private bool m_canWrite;
        protected internal long m_count;
        private bool m_enableMemoryMappedFileAccess;
        protected internal FileStream m_fileStream;
        private Version m_fileVersion;
        private int m_headerSize;
        private bool m_isDisposed;
        private bool m_isInitialized;
        protected internal int m_itemSize;
        private Version m_serializerVersion;
        private string m_tag = "";
        private readonly static Version s_dummyVersion = new Version(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue);

        protected BinaryFile()
        {
        }

        protected BinaryFile(string fileName)
        {
            m_canWrite = true;
            m_fileName = fileName;
        }

        /// <summary>
        /// All memory mapping operations must align to this value (not the dwPageSize)
        /// </summary>
        public static int MinPageSize
        {
            get { return (int) Win32Apis.SystemInfo.dwAllocationGranularity; }
        }

        /// <summary>
        /// Maximum number of bytes to read at once
        /// </summary>
        public static int MinLargePageSize
        {
            get
            {
                switch (Win32Apis.SystemInfo.ProcessorInfo.wProcessorArchitecture)
                {
                    case Win32Apis.SYSTEM_INFO.ProcArch.PROCESSOR_ARCHITECTURE_INTEL:
                        return 4*1024*1024;

                    case Win32Apis.SYSTEM_INFO.ProcArch.PROCESSOR_ARCHITECTURE_AMD64:
                    case Win32Apis.SYSTEM_INFO.ProcArch.PROCESSOR_ARCHITECTURE_IA64:
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

        public string Tag
        {
            get
            {
                ThrowOnDisposed();
                return m_tag;
            }
            set
            {
                ThrowOnInitialized();
                if (value == null)
                    throw new ArgumentNullException("value");
                m_tag = value;
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
            get { return m_enableMemoryMappedFileAccess; }
            set
            {
                if (m_enableMemoryMappedFileAccess != value)
                {
                    if (!NonGenericSerializer.SupportsMemoryMappedFiles && value)
                        throw new NotSupportedException("Memory mapped files are not supported by the serializer");
                    m_enableMemoryMappedFileAccess = value;
                }
            }
        }

        public string FileName
        {
            get { return m_fileName; }
        }

        public Version BaseVersion
        {
            get
            {
                ThrowOnNotInitialized();
                return m_baseVersion;
            }
            protected internal set
            {
                ThrowOnInitialized();
                if (value == null) throw new ArgumentNullException("value");
                if (value != BaseVersion_Current && value != BaseVersion_1_0_NoTag)
                    Utilities.ThrowUnknownVersion(value, typeof (BinaryFile));
                m_baseVersion = value;
            }
        }

        public int HeaderSize
        {
            get
            {
                ThrowOnNotInitialized();
                return m_headerSize;
            }
            protected internal set
            {
                ThrowOnInitialized();

                if (value == m_headerSize)
                    return;

                if (value > MaxHeaderSize || value < BytesInHeaderSize)
                    throw new IOException(
                        String.Format("File header size {0} is not within allowed range {1}..{2}",
                                      value, BytesInHeaderSize, MaxHeaderSize));

                m_headerSize = value;
            }
        }

        public bool CanWrite
        {
            get
            {
                ThrowOnDisposed();
                return m_canWrite;
            }
        }

        public Version SerializerVersion
        {
            get
            {
                ThrowOnNotInitialized();
                return m_serializerVersion;
            }
        }

        public Version FileVersion
        {
            get
            {
                ThrowOnNotInitialized();
                return m_fileVersion;
            }
        }

        public bool IsDisposed
        {
            get { return m_isDisposed; }
        }

        public bool IsInitialized
        {
            get { return m_isInitialized; }
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
            FileStream stream = null;
            try
            {
                stream = new FileStream(
                    fileName, FileMode.Open, canWrite ? FileAccess.ReadWrite : FileAccess.Read,
                    canWrite ? FileShare.Read : FileShare.ReadWrite);
                return Open(stream);
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
        public static BinaryFile Open(FileStream stream)
        {
            if (stream == null) throw new ArgumentNullException("stream");

            stream.Seek(0, SeekOrigin.Begin);

            // Get header size
            int hdrSize = BitConverter.ToInt32(ReadIntoNewBuffer(stream, BytesInHeaderSize), 0);

            // Read the rest of the header and create a memory reader so that we won't accidently go too far on string reads
            var memReader = new BinaryReader(
                new MemoryStream(ReadIntoNewBuffer(stream, hdrSize - BytesInHeaderSize), false));

            // Instantiate BinaryFile-inherited class this file was created with
            Version baseVersion = Utilities.ReadVersion(memReader);
            string classTypeName = memReader.ReadString();
            Type classType = Utilities.GetTypeFromAnyAssemblyVersion(classTypeName);
            if (classType == null)
                throw new InvalidOperationException("Unable to find class type " + classTypeName);
            var inst = (BinaryFile) Activator.CreateInstance(classType, true);

            inst.HeaderSize = hdrSize;
            inst.BaseVersion = baseVersion;
            inst.m_fileStream = stream;

            // Read values in the same order as WriteHeader()
            // Serializer
            string serializerTypeName = memReader.ReadString();
            Type serializerType = Utilities.GetTypeFromAnyAssemblyVersion(serializerTypeName);
            if (serializerType == null)
                throw new InvalidOperationException("Unable to find serializer type " + serializerTypeName);

            inst.m_canWrite = stream.CanWrite;
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

            if (inst.m_baseVersion > BaseVersion_1_0_NoTag)
                inst.Tag = memReader.ReadString();

            inst.m_fileVersion = Utilities.ReadVersion(memReader);
            inst.ReadCustomHeader(memReader, inst.m_fileVersion);

            inst.m_serializerVersion = Utilities.ReadVersion(memReader);
            serializer.ReadCustomHeader(memReader, inst.m_serializerVersion);

            inst.m_count = inst.CalculateItemCountFromFilePosition(inst.m_fileStream.Length);

            inst.m_isInitialized = true;

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
                m_isInitialized = true;
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
            if (!m_isInitialized)
                throw new InvalidOperationException(
                    "InitializeNewFile() must be called before performing any operations on the new file");
        }

        protected void ThrowOnInitialized()
        {
            ThrowOnDisposed();
            if (m_isInitialized)
                throw new InvalidOperationException(
                    "This call is only allowed for new files before InitializeNewFile() was called");
        }

        protected void ThrowOnDisposed()
        {
            if (m_isDisposed)
                throw new ObjectDisposedException(GetType().FullName, "The file has been closed");
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!m_isDisposed)
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

                m_isDisposed = true;
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
            return m_headerSize/m_itemSize;
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
        /// <see cref="Open(System.IO.FileStream)"/>.
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
            Utilities.WriteVersion(memWriter, BaseVersion_Current);
            BaseVersion = BaseVersion_Current;

            memWriter.Write(GetType().AssemblyQualifiedName);
            memWriter.Write(NonGenericSerializer.GetType().AssemblyQualifiedName);

            // Make sure the item size will not change
            memWriter.Write(m_itemSize);

            // User tag
            memWriter.Write(Tag);

            // Save versions and custom headers
            m_fileVersion = WriteHeaderWithVersion(memWriter, WriteCustomHeader);
            m_serializerVersion = WriteHeaderWithVersion(memWriter, NonGenericSerializer.WriteCustomHeader);

            // Header size must be dividable by the item size
            var headerSize = (int) Utilities.RoundUpToMultiple(memWriter.BaseStream.Position, m_itemSize);

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
            Utilities.WriteVersion(memWriter, s_dummyVersion);

            // Write real version and save final position
            Version version = writeHeaderMethod(memWriter);
            long latestPos = memWriter.BaseStream.Position;

            // Seek back, rerecord the proper version instead of the dummy one, and move back to the end
            memWriter.BaseStream.Seek(versionPos, SeekOrigin.Begin);
            Utilities.WriteVersion(memWriter, version);
            memWriter.BaseStream.Seek(latestPos, SeekOrigin.Begin);

            return version;
        }

        /// <summary>
        /// Override to read custom header info. Must match the <see cref="WriteCustomHeader"/>.
        /// </summary>
        protected abstract void ReadCustomHeader(BinaryReader stream, Version version);

        /// <summary>
        /// Override to write custom header info. Must match the <see cref="ReadCustomHeader"/>.
        /// Return the version number of the header.
        /// </summary>
        protected abstract Version WriteCustomHeader(BinaryWriter stream);
    }
}