using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile<T> : BinaryFile
    {
        #region Fields and Properties

        private long _count;

        /// <summary>Size of each stored value in bytes</summary>
        private int _itemSize;

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

        /// <summary>The size of the padding in bytes at the end of each page</summary>
        public int PagePadding
        {
            get { return PageSize%ItemSize; }
        }

        /// <summary>The size of each item of data in bytes</summary>
        public int ItemSize
        {
            get
            {
                if (_itemSize == 0)
                {
                    var size = Serializer.TypeSize;
                    if (size <= 0)
                        throw new ArgumentOutOfRangeException("typeSize" + "", size, "Element size must be > 0");
                    if (size > PageSize)
                        throw new ArgumentOutOfRangeException("typeSize" + "", size,
                                                              "Element size must be less than the page size " +
                                                              PageSize);
                    _itemSize = size;
                }
                return _itemSize;
            }
        }

        /// <summary>Number of items that would fit in a page</summary>
        public int ItemsPerPage
        {
            get { return PageSize/ItemSize; }
        }

        /// <summary>Was file open for writing</summary>
        public bool CanWrite { get; private set; }

        public bool IsEmpty
        {
            get { return Count == 0; }
        }

        private int MaxPagesPerMapView
        {
            get { return MaxMapViewSize/PageSize; }
        }

        #endregion

        #region Instance Creating and Header Handling

        private bool m_enableMemoryMappedFileAccess;

        /// <summary>
        /// Must override this constructor to allow Activator non-public instantiation
        /// </summary>
        protected BinaryFile()
        {
        }

        /// <summary>
        /// Create a new binary file. Will fail if file already exists.
        /// Note to inheritors: derived constructor must call WriteHeader();
        /// </summary>
        /// <param name="fileName">file path</param>
        /// <param name="customSerializer">optional custom serializer type</param>
        protected BinaryFile(string fileName, IBinSerializer<T> customSerializer)
        {
            Serializer = customSerializer ?? new DefaultTypeSerializer<T>();
            m_enableMemoryMappedFileAccess = Serializer.SupportsMemoryMappedFiles;
            PageSize = Serializer.PageSize;
            CanWrite = true;
            m_fileStream = new FileStream(fileName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);
        }

        public bool EnableMemoryMappedFileAccess
        {
            get { return m_enableMemoryMappedFileAccess; }
            set
            {
                if (m_enableMemoryMappedFileAccess != value)
                {
                    if (!Serializer.SupportsMemoryMappedFiles && value)
                        throw new NotSupportedException("Memory mapped files are not supported by the serializer");
                    m_enableMemoryMappedFileAccess = value;
                }
            }
        }

        /// <summary>
        /// Continue reading the file header - now in the type-specific object.
        /// This method together with the <see cref="BinaryFile.Open(FileStream)"/>> 
        /// must match the <see cref="WriteHeader"/> method.
        /// </summary>
        protected internal override sealed void Init(Type serializerType, BinaryReader memReader)
        {
            Serializer = (IBinSerializer<T>) Activator.CreateInstance(serializerType);
            m_enableMemoryMappedFileAccess = Serializer.SupportsMemoryMappedFiles;

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

            Count = CalculateItemCountFromFilePosition(m_fileStream.Length);
            ValidateHeaderSize(HeaderSize);
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
        /// int32   PageSize
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
            memWriter.Write(PageSize);
            memWriter.Write(Serializer.GetType().AssemblyQualifiedName);

            // Make sure the item size will not change
            memWriter.Write(ItemSize);

            // Save versions and custom headers
            FileVersion = WriteHeaderWithVersion(memWriter, WriteCustomHeader);
            SerializerVersion = WriteHeaderWithVersion(memWriter, Serializer.WriteCustomHeader);

            // Header size must be dividable by the item size
            newHeaderSize = (int) Utilities.RoundUpToMultiple(memWriter.BaseStream.Position, ItemSize);
            ValidateHeaderSize(newHeaderSize);

            // Override the header size value at the first position of the header
            memWriter.Seek(0, SeekOrigin.Begin);
            memWriter.Write(newHeaderSize);

            //Count = newItemCount;
            HeaderSize = newHeaderSize;

            m_fileStream.Seek(0, SeekOrigin.Begin);
            m_fileStream.Write(headerBuffer, 0, newHeaderSize);
            m_fileStream.Flush();
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

        #endregion

        #region Private Implementation

        /// <summary>Calculates the number of items that would make up the given file size</summary>
        private long CalculateItemCountFromFilePosition(long position)
        {
            var items = position/PageSize*ItemsPerPage; // Full pages
            items += (position%PageSize)/ItemSize; // Items on the last page
            items -= HeaderSizeAsItemCount;

            if (position != ItemCountToFileLength(items))
                throw new IOException(
                    String.Format(
                        "Calculated file size should be {0}, but the size on disk is {1} ({2})",
                        ItemCountToFileLength(items), position, ToString()));

            return items;
        }

        /// <summary>
        /// Calculate the size of file that would fit given number of items.
        /// </summary>
        private long ItemCountToFileLength(long count)
        {
            var offsetToStopAt = ItemIdxToOffset(count);
            if (offsetToStopAt%PageSize == 0 && PagePadding != 0)
                offsetToStopAt -= PagePadding;
            return offsetToStopAt;
        }

        private long ItemIdxToOffset(long itemIdx)
        {
            //  Number of whole pages before item * pageSize   +   item index on its page * itemSize
            var adjIndex = itemIdx + HeaderSizeAsItemCount;
            return adjIndex/ItemsPerPage*PageSize +
                   adjIndex%ItemsPerPage*ItemSize;
        }

        private long ItemIdxToPage(long itemIdx)
        {
            return (itemIdx + HeaderSizeAsItemCount)/ItemsPerPage;
        }

        /// <summary> Validate file size matches with the header data </summary>
        private void ValidateHeaderSize(int newHeaderSize)
        {
            if (newHeaderSize > PageSize)
                throw new InvalidOperationException(
                    String.Format("File header size {0} must be less than or equal to the page size {1}",
                                  newHeaderSize, PageSize));
        }

        #endregion

        protected void ProcessFileByPage(long firstItemIdx, T[] buffer, int bufOffset, int bufCount, bool isWriting)
        {
            ThrowOnDisposed();
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

            var hasPadding = PagePadding != 0;

            var useMemMapping = Serializer.SupportsMemoryMappedFiles
                                && ItemIdxToOffset(bufCount) > MinReqSizeToUseMapView;

            var offsetToStopAt = ItemCountToFileLength(firstItemIdx + bufCount);

            var numPagesToProcess = (ItemIdxToPage(firstItemIdx + bufCount) - ItemIdxToPage(firstItemIdx) + 1);
            var pagesPerRun = hasPadding ? 1 : (useMemMapping ? MaxPagesPerMapView : numPagesToProcess);
            var itemsPerRun = pagesPerRun*ItemsPerPage;
            var runsPerGroup = hasPadding ? (useMemMapping ? MaxPagesPerMapView : numPagesToProcess) : 1;
            var groupCount = Decimal.Ceiling((decimal) numPagesToProcess/runsPerGroup/pagesPerRun);
            var itemIdx = firstItemIdx;

            SafeMapHandle hMap = null;

            try
            {
                if (useMemMapping)
                {
                    var fileSize = m_fileStream.Length;

                    // Grow file if needed
                    if (isWriting && offsetToStopAt > fileSize)
                        fileSize = offsetToStopAt;

                    hMap = Win32Apis.CreateFileMapping(
                        m_fileStream, fileSize,
                        isWriting ? FileMapProtection.PageReadWrite : FileMapProtection.PageReadOnly);
                }

                for (long group = 0; group < groupCount; group++)
                {
                    SafeMapViewHandle ptrMapViewBaseAddr = null;
                    try
                    {
                        long mapViewFileOffset = 0;
                        if (useMemMapping)
                        {
                            // Round down to nearest page
                            mapViewFileOffset = (ItemIdxToOffset(itemIdx)/PageSize)*PageSize;

                            // Cannot exceed file length (at least for files opened as read-only)
                            var mapViewSize = Math.Min(offsetToStopAt - mapViewFileOffset, MaxMapViewSize);

                            // The size of the new map view.
                            ptrMapViewBaseAddr = Win32Apis.MapViewOfFile(
                                hMap, mapViewFileOffset, mapViewSize,
                                isWriting ? FileMapAccess.Write : FileMapAccess.Read);
                        }

                        for (long run = 0; run < runsPerGroup; run++)
                        {
                            var fileOffset = ItemIdxToOffset(itemIdx);
                            var totalItemsDone = itemIdx - firstItemIdx;
                            var bufItemOffset = bufOffset + totalItemsDone;

                            var itemsToProcessThisRun = Math.Min(
                                bufCount - totalItemsDone,
                                itemsPerRun - ((itemIdx + HeaderSizeAsItemCount)%itemsPerRun));

                            if (useMemMapping)
                            {
                                // Access file using memory-mapped pages
                                Serializer.ProcessMemoryMap(
                                    (IntPtr) (ptrMapViewBaseAddr.Address + fileOffset - mapViewFileOffset),
                                    buffer, (int) bufItemOffset, (int) itemsToProcessThisRun, isWriting);
                            }
                            else
                            {
                                // Access file using FileStream object
                                m_fileStream.Seek(fileOffset, SeekOrigin.Begin);
                                Serializer.ProcessFileStream(m_fileStream, buffer, (int) bufItemOffset,
                                                             (int) itemsToProcessThisRun, isWriting);

                                var expectedStreamPos = fileOffset + itemsToProcessThisRun*ItemSize;
                                if (expectedStreamPos != m_fileStream.Position)
                                    throw new InvalidOperationException(
                                        String.Format(
                                            "Possible loss of data or file corruption detected.\n" +
                                            "Unexpected position in the data stream: after {0} {1} items, position should have moved " +
                                            "from 0x{2:X} to 0x{3:X}, but instead is now at 0x{4:X}.",
                                            isWriting ? "writing" : "reading",
                                            itemsToProcessThisRun, fileOffset, expectedStreamPos, m_fileStream.Position));

                                if (hasPadding && (bufCount - (totalItemsDone + itemsToProcessThisRun)) > 0)
                                    m_fileStream.Seek(PagePadding, SeekOrigin.Current);
                            }

                            itemIdx += itemsToProcessThisRun;
                        }
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

            if (isWriting)
            {
                if (!useMemMapping)
                    m_fileStream.Flush();

                var newCount = CalculateItemCountFromFilePosition(m_fileStream.Length);
                if (Count < newCount)
                    Count = newCount;
            }
        }

        protected void ThrowOnDisposed()
        {
            if (m_fileStream == null)
                throw new ObjectDisposedException(GetType().FullName, "The file has been closed");
        }

        protected static void ValidateMaxRequestSize(long itemCount)
        {
            if (itemCount < 0)
                throw new ArgumentOutOfRangeException(
                    "itemCount", itemCount, "<0");
            if (itemCount > MaxItemsPerRequest)
                throw new ArgumentOutOfRangeException(
                    "itemCount", itemCount,
                    String.Format("Cannot get more than {0} items at once", MaxItemsPerRequest));
        }

        public override string ToString()
        {
            return string.Format("File {0} with {1} items", m_fileStream.Name, Count);
        }
    }
}