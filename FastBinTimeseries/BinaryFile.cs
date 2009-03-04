using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile<T> : BinaryFile
    {
        protected const bool Read = false;
        protected const bool Write = true;
        private readonly string m_fileName;
        private long _count;

        private bool _enableMemoryMappedFileAccess;

        /// <summary>
        /// Must override this constructor to allow Activator non-public instantiation
        /// </summary>
        protected BinaryFile()
        {
        }

        /// <summary>
        /// Create a new binary file. Must call <seealso cref="InitializeNewFile"/> to finish file creation.
        /// </summary>
        /// <param name="fileName">file path</param>
        /// <param name="customSerializer">optional custom serializer type</param>
        protected BinaryFile(string fileName, IBinSerializer<T> customSerializer)
        {
            Serializer = customSerializer ?? new DefaultTypeSerializer<T>();
            InitFromSerializer();
            CanWrite = true;
            m_fileName = fileName;
        }

        public Version SerializerVersion { get; private set; }
        public Version FileVersion { get; private set; }

        /// <summary>Total number of items in the file</summary>
        public long Count
        {
            get { return _count; }
            private set
            {
                if (value < 0)
                    throw new IOException(String.Format("Item count of {0} is invalid. Must be => 0.", value));
                _count = value;
            }
        }

        protected IBinSerializer<T> Serializer { get; private set; }

        /// <summary>Size of the file header expressed as a number of items</summary>
        public int HeaderSizeAsItemCount
        {
            get { return HeaderSize/ItemSize; }
        }

        /// <summary>The size of each item of data in bytes</summary>
        public int ItemSize { get; private set; }

        /// <summary>Was file open for writing</summary>
        public bool CanWrite { get; private set; }

        public bool IsEmpty
        {
            get { return Count == 0; }
        }

        public bool EnableMemoryMappedFileAccess
        {
            get { return _enableMemoryMappedFileAccess; }
            set
            {
                if (_enableMemoryMappedFileAccess != value)
                {
                    if (!Serializer.SupportsMemoryMappedFiles && value)
                        throw new NotSupportedException("Memory mapped files are not supported by the serializer");
                    _enableMemoryMappedFileAccess = value;
                }
            }
        }

        public void InitializeNewFile()
        {
            ThrowOnDisposed();
            if (m_fileStream != null)
                throw new InvalidOperationException(
                    "InitializeNewFile() can only be called for new files before performing any other operations");

            m_fileStream = new FileStream(m_fileName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);
            try
            {
                WriteHeader();
            }
            catch (Exception ex)
            {
                // on error, delete the newly created file and pass on the exception
                try
                {
                    m_fileStream.Close();
                    File.Delete(m_fileName);
                }
                catch (Exception ex2)
                {
                    throw new CombinedException("Failed to clean up after failed header writing", ex, ex2);
                }

                throw;
            }
        }

        private void InitFromSerializer()
        {
            var size = Serializer.TypeSize;
            if (size <= 0)
                throw new ArgumentOutOfRangeException("typeSize" + "", size,
                                                      "Element size given by the serializer must be > 0");
            ItemSize = size;
            _enableMemoryMappedFileAccess = Serializer.SupportsMemoryMappedFiles;
        }

        /// <summary>
        /// Continue reading the file header - now in the type-specific object.
        /// This method together with the <see cref="BinaryFile.Open(FileStream)"/>> 
        /// must match the <see cref="WriteHeader"/> method.
        /// </summary>
        protected internal override sealed void Init(Type serializerType, BinaryReader memReader)
        {
            Serializer = (IBinSerializer<T>) Activator.CreateInstance(serializerType);
            InitFromSerializer();

            // Make sure the item size has not changed
            var itemSize = memReader.ReadInt32();
            if (itemSize != ItemSize)
                throw new InvalidOperationException(
                    string.Format(
                        "The file of type {0} was created with itemsize={1}, but now the expected size is {2}",
                        GetType().FullName, itemSize, ItemSize));

            FileVersion = Utilities.ReadVersion(memReader);
            ReadCustomHeader(memReader, FileVersion);

            SerializerVersion = Utilities.ReadVersion(memReader);
            Serializer.ReadCustomHeader(memReader, SerializerVersion);

            Count = CalculateItemCountFromFilePosition(FileStream.Length);
        }

        /// <summary>
        /// Write the header info into the begining of the file.
        /// This method must match the reading sequence in the
        /// <see cref="BinaryFile.Open(FileStream)"/> and <see cref="Init"/>.
        /// </summary>
        /// <remarks>
        ///            ***  Header structure: ***
        /// int32   HeaderSize
        /// Version BinFile main version
        /// string  BinaryFile...<...> type name
        /// string  Serializer type name
        /// int32   ItemSize
        /// Version BinFile custom header version
        /// ...     BinFile custom header
        /// Version Serializer version
        /// ...     Serializer custom header
        /// </remarks>
        protected void WriteHeader()
        {
            if (Count != 0)
                throw new InvalidOperationException("Should only be called for an empty file");

            // Perform all the writing into a memory buffer to avoid file corruption
            var newHeaderSize = HeaderSize;
            var headerBuffer = Count == 0 ? new byte[MaxHeaderSize] : new byte[newHeaderSize];
            var memWriter = new BinaryWriter(new MemoryStream(headerBuffer));

            //
            // Serialize header values
            //

            // for an empty file, this will be replaced by an actual length later
            memWriter.Write(newHeaderSize);
            Utilities.WriteVersion(memWriter, BaseVersion);
            memWriter.Write(GetType().AssemblyQualifiedName);
            memWriter.Write(Serializer.GetType().AssemblyQualifiedName);

            // Make sure the item size will not change
            memWriter.Write(ItemSize);

            // Save versions and custom headers
            FileVersion = WriteHeaderWithVersion(memWriter, WriteCustomHeader);
            SerializerVersion = WriteHeaderWithVersion(memWriter, Serializer.WriteCustomHeader);

            // Header size must be dividable by the item size
            newHeaderSize = (int) Utilities.RoundUpToMultiple(memWriter.BaseStream.Position, ItemSize);

            // Override the header size value at the first position of the header
            memWriter.Seek(0, SeekOrigin.Begin);
            memWriter.Write(newHeaderSize);

            //Count = newItemCount;
            HeaderSize = newHeaderSize;

            FileStream.Seek(0, SeekOrigin.Begin);
            FileStream.Write(headerBuffer, 0, newHeaderSize);
            FileStream.Flush();
        }

        /// <summary>
        /// Write version plus custom header generated by the writeHeaderMethod into the stream
        /// </summary>
        private static Version WriteHeaderWithVersion(BinaryWriter memWriter,
                                                      Func<BinaryWriter, Version> writeHeaderMethod)
        {
            // Record original postition and write dummy version
            var versionPos = memWriter.BaseStream.Position;
            Utilities.WriteVersion(memWriter, new Version(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue));

            // Write real version and save final position
            var version = writeHeaderMethod(memWriter);
            var latestPos = memWriter.BaseStream.Position;

            // Seek back, rerecord the proper version instead of the dummy one, and move back to the end
            memWriter.BaseStream.Seek(versionPos, SeekOrigin.Begin);
            Utilities.WriteVersion(memWriter, version);
            memWriter.BaseStream.Seek(latestPos, SeekOrigin.Begin);

            return version;
        }

        protected abstract void ReadCustomHeader(BinaryReader stream, Version version);

        /// <summary>
        /// Override to write custom header info. Must match the <see cref="ReadCustomHeader"/>.
        /// Return the version number of the header.
        /// </summary>
        protected abstract Version WriteCustomHeader(BinaryWriter stream);


        /// <summary>Calculates the number of items that would make up the given file size</summary>
        private long CalculateItemCountFromFilePosition(long position)
        {
            var items = position/ItemSize;
            items -= HeaderSizeAsItemCount;

            if (position != ItemIdxToOffset(items))
                throw new IOException(
                    String.Format(
                        "Calculated file size should be {0}, but the size on disk is {1} ({2})",
                        ItemIdxToOffset(items), position, ToString()));

            return items;
        }

        private long ItemIdxToOffset(long itemIdx)
        {
            var adjIndex = itemIdx + HeaderSizeAsItemCount;
            return adjIndex*ItemSize;
        }

        /// Access file using FileStream object
        private void ProcessFileNoMMF(long firstItemIdx, T[] buffer, int bufOffset, int bufCount, bool isWriting)
        {
            var fileOffset = ItemIdxToOffset(firstItemIdx);

            FileStream.Seek(fileOffset, SeekOrigin.Begin);
            Serializer.ProcessFileStream(FileStream, buffer, bufOffset, bufCount, isWriting);

            var expectedStreamPos = fileOffset + bufCount*ItemSize;
            if (expectedStreamPos != FileStream.Position)
                throw new InvalidOperationException(
                    String.Format(
                        "Possible loss of data or file corruption detected.\n" +
                        "Unexpected position in the data stream: after {0} {1} items, position should have moved " +
                        "from 0x{2:X} to 0x{3:X}, but instead is now at 0x{4:X}.",
                        isWriting ? "writing" : "reading",
                        bufCount, fileOffset, expectedStreamPos, FileStream.Position));
        }

        private void ProcessFileMMF(long firstItemIdx, T[] buffer, int bufOffset, int bufCount, bool isWriting)
        {
            SafeMapHandle hMap = null;
            try
            {
                var offsetToStopAt = ItemIdxToOffset(firstItemIdx + bufCount);
                var itemIdx = firstItemIdx;

                // Grow file if needed
                var fileSize = FileStream.Length;
                if (isWriting && offsetToStopAt > fileSize)
                    fileSize = offsetToStopAt;
                hMap = Win32Apis.CreateFileMapping(
                    FileStream, fileSize,
                    isWriting ? FileMapProtection.PageReadWrite : FileMapProtection.PageReadOnly);

                while (itemIdx < firstItemIdx + bufCount)
                {
                    SafeMapViewHandle ptrMapViewBaseAddr = null;
                    try
                    {
                        var itemIdxOffset = ItemIdxToOffset(itemIdx);
                        var mapViewFileOffset = Utilities.RoundDownToMultiple(itemIdxOffset, MinPageSize);

                        var mapViewSize = offsetToStopAt - mapViewFileOffset;
                        var itemsToProcessThisRun = firstItemIdx + bufCount - itemIdx;
                        if (mapViewSize > MinLargePageSize)
                        {
                            mapViewSize = MinLargePageSize;
                            itemsToProcessThisRun = (mapViewFileOffset + mapViewSize)/ItemSize - itemIdx -
                                                    HeaderSizeAsItemCount;
                        }

                        // The size of the new map view.
                        ptrMapViewBaseAddr = Win32Apis.MapViewOfFile(
                            hMap, mapViewFileOffset, mapViewSize,
                            isWriting ? FileMapAccess.Write : FileMapAccess.Read);

                        var totalItemsDone = itemIdx - firstItemIdx;
                        var bufItemOffset = bufOffset + totalItemsDone;

                        // Access file using memory-mapped pages
                        Serializer.ProcessMemoryMap(
                            (IntPtr) (ptrMapViewBaseAddr.Address + itemIdxOffset - mapViewFileOffset),
                            buffer, (int) bufItemOffset, (int) itemsToProcessThisRun, isWriting);

                        itemIdx += itemsToProcessThisRun;
                    }
                    finally
                    {
                        if (ptrMapViewBaseAddr != null)
                            ptrMapViewBaseAddr.Dispose();
                    }
                }
            }
            finally
            {
                if (hMap != null)
                    hMap.Dispose();
            }
        }

        protected void PerformFileAccess(long firstItemIdx, T[] buffer, int bufOffset, int bufCount, bool isWriting)
        {
            ThrowOnInvalidState();
            Utilities.ValidateArrayParams(buffer, bufOffset, bufCount);

            // Optimize out empty requests - any first index would work when count==0
            if (bufCount == 0)
                return;

            if (firstItemIdx < 0 || firstItemIdx > Count || (firstItemIdx == Count && !isWriting))
                throw new ArgumentOutOfRangeException("firstItemIdx", firstItemIdx, "Must be >= 0 and < Count");

            if (!isWriting && firstItemIdx + bufCount > Count)
                throw new ArgumentOutOfRangeException(
                    "bufCount", bufCount,
                    "There is not enough data to fulfill this request. FirstItemIndex + BufferCount > Count");

            if (Count == 0 && isWriting)
            {
                // New file, rewrite header
                WriteHeader();
            }

            var useMemMapping = EnableMemoryMappedFileAccess
                                && ItemIdxToOffset(bufCount) > MinReqSizeToUseMapView;

            if (useMemMapping)
                ProcessFileMMF(firstItemIdx, buffer, bufOffset, bufCount, isWriting);
            else
                ProcessFileNoMMF(firstItemIdx, buffer, bufOffset, bufCount, isWriting);

            if (isWriting)
            {
                if (!useMemMapping)
                    FileStream.Flush();

                var newCount = CalculateItemCountFromFilePosition(FileStream.Length);
                if (Count < newCount)
                    Count = newCount;
            }
        }

        public override string ToString()
        {
            return string.Format("File {0} with {1} items", FileStream.Name, Count);
        }
    }
}