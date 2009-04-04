using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile<T> : BinaryFile
    {
        protected const bool Read = false;
        protected const bool Write = true;

        private readonly string _fileName;
        private bool _canWrite;
        private long _count;
        private bool _enableMemoryMappedFileAccess;
        private Version _fileVersion;
        private int _itemSize;
        private IBinSerializer<T> _serializer;
        private Version _serializerVersion;
        private string _tag = "";

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
        protected BinaryFile(string fileName)
        {
            _canWrite = true;
            _fileName = fileName;
        }

        public Version SerializerVersion
        {
            get
            {
                ThrowOnNotInitialized();
                return _serializerVersion;
            }
            private set
            {
                if (value == null) throw new ArgumentNullException("value");
                _serializerVersion = value;
            }
        }

        public Version FileVersion
        {
            get
            {
                ThrowOnNotInitialized();
                return _fileVersion;
            }
            private set
            {
                if (value == null) throw new ArgumentNullException("value");
                _fileVersion = value;
            }
        }

        /// <summary>Total number of items in the file</summary>
        public long Count
        {
            get
            {
                ThrowOnNotInitialized();
                return _count;
            }
            private set
            {
                if (value < 0)
                    throw new IOException(String.Format("Item count of {0} is invalid. Must be => 0.", value));
                _count = value;
            }
        }

        public IBinSerializer<T> Serializer
        {
            get
            {
                if (_serializer == null)
                    _serializer = new DefaultTypeSerializer<T>();
                return _serializer;
            }
            set
            {
                ThrowOnInitialized();
                _serializer = value;
            }
        }

        /// <summary>The size of each item of data in bytes</summary>
        public int ItemSize
        {
            get
            {
                ThrowOnNotInitialized();
                return _itemSize;
            }
            private set
            {
                if (value <= 0)
                    throw new ArgumentOutOfRangeException("typeSize" + "", value,
                                                          "Element size given by the serializer must be > 0");
                _itemSize = value;
            }
        }

        /// <summary>Was file open for writing</summary>
        public bool CanWrite
        {
            get
            {
                ThrowOnDisposed();
                return _canWrite;
            }
        }

        /// <summary>User string stored in the header</summary>
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

        public bool IsOpen
        {
            get { return IsInitialized && !IsDisposed; }
        }

        #region Calculation Utilites

        /// <summary>Size of the file header expressed as a number of items</summary>
        private int CalculateHeaderSizeAsItemCount()
        {
            return _headerSize/_itemSize;
        }

        /// <summary>Calculates the number of items that would make up the given file size</summary>
        private long CalculateItemCountFromFilePosition(long position)
        {
            long items = position/_itemSize;
            items -= CalculateHeaderSizeAsItemCount();

            if (position != ItemIdxToOffset(items))
                throw new IOException(
                    String.Format(
                        "Calculated file size should be {0}, but the size on disk is {1} ({2})",
                        ItemIdxToOffset(items), position, ToString()));

            return items;
        }

        private long ItemIdxToOffset(long itemIdx)
        {
            long adjIndex = itemIdx + CalculateHeaderSizeAsItemCount();
            return adjIndex*_itemSize;
        }

        #endregion

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
            _fileStream = new FileStream(_fileName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);

            try
            {
                ItemSize = Serializer.TypeSize;
                EnableMemoryMappedFileAccess = Serializer.SupportsMemoryMappedFiles;
                WriteHeader();
                _isInitialized = true;
            }
            catch (Exception ex)
            {
                // on error, delete the newly created file and pass on the exception
                try
                {
                    Dispose(true); // invalidate object state
                    File.Delete(_fileName);
                }
                catch (Exception ex2)
                {
                    throw new CombinedException("Failed to clean up after failed header writing", ex, ex2);
                }

                throw;
            }
        }

        /// <summary>
        /// Continue reading the file header - now in the type-specific object.
        /// This method together with the <see cref="BinaryFile.Open(FileStream)"/>> 
        /// must match the <see cref="WriteHeader"/> method.
        /// </summary>
        protected internal override sealed void Open(Type serializerType, BinaryReader memReader, bool canWrite)
        {
            _canWrite = canWrite;
            _serializer = (IBinSerializer<T>) Activator.CreateInstance(serializerType);
            EnableMemoryMappedFileAccess = Serializer.SupportsMemoryMappedFiles;

            // Make sure the item size has not changed
            int serializerTypeSize = Serializer.TypeSize;
            int itemSize = memReader.ReadInt32();
            if (itemSize != serializerTypeSize)
                throw new InvalidOperationException(
                    string.Format(
                        "The file of type {0} was created with itemSize={1}, but now the itemSize={2}",
                        GetType().FullName, itemSize, ItemSize));
            ItemSize = serializerTypeSize;

            if (_baseVersion > BaseVersion_1_0_NoTag)
                Tag = memReader.ReadString();

            FileVersion = Utilities.ReadVersion(memReader);
            ReadCustomHeader(memReader, _fileVersion);

            SerializerVersion = Utilities.ReadVersion(memReader);
            Serializer.ReadCustomHeader(memReader, _serializerVersion);

            Count = CalculateItemCountFromFilePosition(_fileStream.Length);
        }

        /// <summary>
        /// Write the header info into the begining of the file.
        /// This method must match the reading sequence in the
        /// <see cref="BinaryFile.Open(FileStream)"/> and <see cref="Open"/>.
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
            memWriter.Write(Serializer.GetType().AssemblyQualifiedName);

            // Make sure the item size will not change
            memWriter.Write(_itemSize);

            // User tag
            memWriter.Write(Tag);

            // Save versions and custom headers
            FileVersion = WriteHeaderWithVersion(memWriter, WriteCustomHeader);
            SerializerVersion = WriteHeaderWithVersion(memWriter, Serializer.WriteCustomHeader);

            // Header size must be dividable by the item size
            var headerSize = (int) Utilities.RoundUpToMultiple(memWriter.BaseStream.Position, _itemSize);

            // Override the header size value at the first position of the header
            memWriter.Seek(0, SeekOrigin.Begin);
            memWriter.Write(headerSize);

            HeaderSize = headerSize;

            if (_fileStream.Position != 0)
                throw new InvalidOperationException("Expected to be at the stream position 0");
            _fileStream.Write(memStream.GetBuffer(), 0, headerSize);
            _fileStream.Flush();
        }

        /// <summary>
        /// Write version plus custom header generated by the writeHeaderMethod into the stream
        /// </summary>
        private static Version WriteHeaderWithVersion(BinaryWriter memWriter,
                                                      Func<BinaryWriter, Version> writeHeaderMethod)
        {
            // Record original postition and write dummy version
            long versionPos = memWriter.BaseStream.Position;
            Utilities.WriteVersion(memWriter, new Version(int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue));

            // Write real version and save final position
            Version version = writeHeaderMethod(memWriter);
            long latestPos = memWriter.BaseStream.Position;

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

        protected void PerformFileAccess(long firstItemIdx, T[] buffer, int bufOffset, int bufCount, bool isWriting)
        {
            ThrowOnNotInitialized();
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

            bool useMemMapping = EnableMemoryMappedFileAccess
                                 && ItemIdxToOffset(bufCount) > MinReqSizeToUseMapView;

            if (useMemMapping)
                ProcessFileMMF(firstItemIdx, buffer, bufOffset, bufCount, isWriting);
            else
                ProcessFileNoMMF(firstItemIdx, buffer, bufOffset, bufCount, isWriting);

            if (isWriting)
            {
                if (!useMemMapping)
                    FileStream.Flush();

                long newCount = CalculateItemCountFromFilePosition(FileStream.Length);
                if (Count < newCount)
                    Count = newCount;
            }
        }

        /// Access file using FileStream object
        private void ProcessFileNoMMF(long firstItemIdx, T[] buffer, int bufOffset, int bufCount, bool isWriting)
        {
            long fileOffset = ItemIdxToOffset(firstItemIdx);

            FileStream.Seek(fileOffset, SeekOrigin.Begin);
            Serializer.ProcessFileStream(FileStream, buffer, bufOffset, bufCount, isWriting);

            long expectedStreamPos = fileOffset + bufCount*ItemSize;
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
                long offsetToStopAt = ItemIdxToOffset(firstItemIdx + bufCount);
                long itemIdx = firstItemIdx;

                // Grow file if needed
                long fileSize = FileStream.Length;
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
                        long itemIdxOffset = ItemIdxToOffset(itemIdx);
                        long mapViewFileOffset = Utilities.RoundDownToMultiple(itemIdxOffset, MinPageSize);

                        long mapViewSize = offsetToStopAt - mapViewFileOffset;
                        long itemsToProcessThisRun = firstItemIdx + bufCount - itemIdx;
                        if (mapViewSize > MinLargePageSize)
                        {
                            mapViewSize = MinLargePageSize;
                            itemsToProcessThisRun = (mapViewFileOffset + mapViewSize)/ItemSize - itemIdx -
                                                    CalculateHeaderSizeAsItemCount();
                        }

                        // The size of the new map view.
                        ptrMapViewBaseAddr = Win32Apis.MapViewOfFile(
                            hMap, mapViewFileOffset, mapViewSize,
                            isWriting ? FileMapAccess.Write : FileMapAccess.Read);

                        long totalItemsDone = itemIdx - firstItemIdx;
                        long bufItemOffset = bufOffset + totalItemsDone;

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

        public override string ToString()
        {
            return string.Format("{0} file {1} of type {2}{3}",
                                 IsDisposed ? "Disposed" : (IsInitialized ? "Open" : "Uninitialized"),
                                 _fileStream == null ? "(unknown)" : _fileStream.Name,
                                 GetType().FullName,
                                 IsOpen ? string.Format(" with {0} items", Count) : "");
        }
    }
}