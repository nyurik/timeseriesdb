using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile : IDisposable
    {
        protected const int BytesInHeaderSize = sizeof (int);
        protected const int MaxHeaderSize = 4*1024*1024;
        protected const int MinReqSizeToUseMapView = 4*1024; // 4 KB

        protected static readonly Version BaseVersion_1_0_NoTag = new Version(1, 0);
        protected static readonly Version BaseVersion_Current = new Version(1, 1);

        protected internal Version _baseVersion;
        protected internal FileStream _fileStream;
        protected internal int _headerSize;
        private bool _isDisposed;
        protected internal bool _isInitialized;

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
                    Utilities.ThrowUnknownVersion(value, typeof (BinaryFile));
                _baseVersion = value;
            }
        }

        /// <summary>Size of the file header in bytes</summary>
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
                return _fileStream;
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

        public void Close()
        {
            ((IDisposable) this).Dispose();
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
        /// This method together with the <see cref="Open(System.Type,System.IO.BinaryReader,bool)"/>
        /// must match the <see cref="BinaryFile{T}.WriteHeader"/> method.
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
            inst._fileStream = stream;

            // Read values in the same order as WriteHeader()
            // Serializer
            string serializerTypeName = memReader.ReadString();
            Type serializerType = Utilities.GetTypeFromAnyAssemblyVersion(serializerTypeName);
            if (serializerType == null)
                throw new InvalidOperationException("Unable to find serializer type " + serializerTypeName);

            inst.Open(serializerType, memReader, stream.CanWrite);

            inst._isInitialized = true;

            return inst;
        }

        protected internal abstract void Open(Type serializerType, BinaryReader memReader, bool canWrite);

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
    }
}