using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile
    {
        protected const int DefaultCustomTypeHeaderSize = 2*1024; // 2 KB
        protected const int DefaultPageSize = 64*1024; // 64 KB - todo: optimal?
        protected const int HeaderSizeByteCount = 4; // sizeof(int)
        protected const int MaxHeaderSize = 64*1024;
        protected const int MaxItemsPerRequest = Int32.MaxValue;
        protected const int MaxMapViewSize = 16*1024*1024; // 8 MB - large page size to optimize TLB use
        protected const int MinPageSize = 8*1024; // 8 KB - smallest value on IA64 systems
        protected const int MinReqSizeToUseMapView = 4*1024; // 4 KB

        protected FileStream _fileStream;
        private int _fileHeaderSize;
        private int _pageSize;

        /// <summary>The size of a data page in bytes.</summary>
        public int PageSize
        {
            get { return _pageSize; }
            protected set
            {
                if (value == 0)
                    _pageSize = DefaultPageSize;
                else
                {
                    if (value < MinPageSize || value%MinPageSize != 0)
                        throw new ArgumentOutOfRangeException(
                            String.Format("PageSize must be greater or equal then and divisible by {0}",
                                          MinPageSize));
                    _pageSize = value;
                }
            }
        }

        /// <summary>Size of the file header in bytes</summary>
        public int FileHeaderSize
        {
            get { return _fileHeaderSize; }
            protected set
            {
                if (value == _fileHeaderSize)
                    return;

                if (value > MaxHeaderSize || value < HeaderSizeByteCount)
                    throw new IOException(
                        String.Format("File header size {0} is not within allowed range {1}..{2}",
                                      value, HeaderSizeByteCount, MaxHeaderSize));

                _fileHeaderSize = value;
            }
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
                    fileName, FileMode.Open, canWrite ? FileAccess.ReadWrite : FileAccess.Read, FileShare.None);
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

        /// <summary>Open a binary file from a filestream</summary>
        public static BinaryFile Open(FileStream stream)
        {
            if(stream == null)
                throw new ArgumentNullException("stream");

            stream.Seek(0, SeekOrigin.Begin);

            // Get header size
            var hdrSize = BitConverter.ToInt32(ReadIntoNewBuffer(stream, HeaderSizeByteCount), 0);

            // Read the rest of the header and create a memory reader so that we won't accidently go too far on string reads
            var memReader = new BinaryReader(
                new MemoryStream(ReadIntoNewBuffer(stream, hdrSize - HeaderSizeByteCount), false));

            // Instantiate BinaryFile-inherited class this file was created with
            var classTypeName = memReader.ReadString();
            var classType = Utilities.GetTypeFromAnyAssemblyVersion(classTypeName);
            if (classType == null)
                throw new ArgumentException("Unable to find class type " + classTypeName);
            var inst = (BinaryFile) Activator.CreateInstance(classType, true);

            inst.FileHeaderSize = hdrSize;
            inst._fileStream = stream;

            // Read values in the same order as WriteHeader()
            inst.PageSize = memReader.ReadInt32();

            // Serializer
            var serializerTypeName = memReader.ReadString();
            var serializerType = Utilities.GetTypeFromAnyAssemblyVersion(serializerTypeName);
            if (serializerType == null)
                throw new ArgumentException("Unable to find serializer type " + serializerTypeName);

            inst.Init(serializerType, memReader);

            return inst;
        }

        protected abstract void Init(Type serializerType, BinaryReader memReader);

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