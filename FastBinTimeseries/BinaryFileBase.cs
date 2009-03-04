using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile : IDisposable
    {
        protected const int MaxHeaderSize = 64*1024;
        protected const int MinReqSizeToUseMapView = 4*1024; // 4 KB
        protected static readonly Version BaseCurrentVersion = new Version(1, 0);
        protected static readonly int BytesInHeaderSize = sizeof (int);

        protected internal FileStream m_fileStream;
        protected internal bool m_isDisposed;
        private int m_headerSize;

        protected BinaryFile()
        {
            BaseVersion = BaseCurrentVersion;
        }

        public Version BaseVersion { get; private set; }

        /// <summary>Size of the file header in bytes</summary>
        public int HeaderSize
        {
            get { return m_headerSize; }
            protected set
            {
                if (value == m_headerSize)
                    return;

                if (value > MaxHeaderSize || value < BytesInHeaderSize)
                    throw new IOException(
                        String.Format("File header size {0} is not within allowed range {1}..{2}",
                                      value, BytesInHeaderSize, MaxHeaderSize));

                m_headerSize = value;
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
                ThrowOnInvalidState();
                return m_fileStream;
            }
        }

        #region IDisposable Members

        void IDisposable.Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        protected void ThrowOnInvalidState()
        {
            ThrowOnDisposed();
            if (m_fileStream == null)
                throw new InvalidOperationException(
                    "You must call InitializeNewFile() before performing any operations on the new file");
        }

        protected void ThrowOnDisposed()
        {
            if (m_isDisposed)
                throw new ObjectDisposedException(GetType().FullName, "The file has been closed");
        }

        public void Close()
        {
            ((IDisposable) this).Dispose();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!m_isDisposed)
            {
                if (disposing)
                {
                    var streamTmp = m_fileStream;
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
        /// This method together with the <see cref="Init"/>
        /// must match the <see cref="BinaryFile{T}.WriteHeader"/> method.
        /// </summary>
        public static BinaryFile Open(FileStream stream)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            stream.Seek(0, SeekOrigin.Begin);

            // Get header size
            var hdrSize = BitConverter.ToInt32(ReadIntoNewBuffer(stream, BytesInHeaderSize), 0);

            // Read the rest of the header and create a memory reader so that we won't accidently go too far on string reads
            var memReader = new BinaryReader(
                new MemoryStream(ReadIntoNewBuffer(stream, hdrSize - BytesInHeaderSize), false));

            // Instantiate BinaryFile-inherited class this file was created with
            var baseVersion = Utilities.ReadVersion(memReader);

            // Any older versions of the header should be processed here
            if (baseVersion != BaseCurrentVersion)
                Utilities.ThrowUnknownVersion(baseVersion, typeof (BinaryFile));


            var classTypeName = memReader.ReadString();
            var classType = Utilities.GetTypeFromAnyAssemblyVersion(classTypeName);
            if (classType == null)
                throw new InvalidOperationException("Unable to find class type " + classTypeName);
            var inst = (BinaryFile) Activator.CreateInstance(classType, true);

            inst.HeaderSize = hdrSize;
            inst.BaseVersion = baseVersion;
            inst.m_fileStream = stream;

            // Read values in the same order as WriteHeader()
            // Serializer
            var serializerTypeName = memReader.ReadString();
            var serializerType = Utilities.GetTypeFromAnyAssemblyVersion(serializerTypeName);
            if (serializerType == null)
                throw new InvalidOperationException("Unable to find serializer type " + serializerTypeName);

            inst.Init(serializerType, memReader);

            return inst;
        }

        protected internal abstract void Init(Type serializerType, BinaryReader memReader);

        private static byte[] ReadIntoNewBuffer(Stream stream, int bufferSize)
        {
            var headerBuffer = new byte[bufferSize];
            var bytesRead = stream.Read(headerBuffer, 0, bufferSize);
            if (bytesRead < bufferSize)
                throw new IOException(
                    String.Format("Unable to read a block of size {0}: only {1} bytes were available", bufferSize,
                                  bytesRead));
            return headerBuffer;
        }
    }
}