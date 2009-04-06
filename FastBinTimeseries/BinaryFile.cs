using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile<T> : BinaryFile
    {
        private IBinSerializer<T> m_serializer;

        /// <summary>
        /// Must override this constructor to allow Activator non-public instantiation
        /// </summary>
        protected BinaryFile()
        {
        }

        /// <summary>
        /// Create a new binary file. Must call <seealso cref="BinaryFile.InitializeNewFile"/> to finish file creation.
        /// </summary>
        /// <param name="fileName">file path</param>
        protected BinaryFile(string fileName)
            : base(fileName)
        {
            // Initialize default serializer
            Serializer = new DefaultTypeSerializer<T>();
        }

        public IBinSerializer<T> Serializer
        {
            get
            {
                if (m_serializer == null)
                    throw new InvalidOperationException("Serializer is not initialized");
                return m_serializer;
            }
            set
            {
                ThrowOnInitialized();
                if (value == null)
                    throw new ArgumentNullException("value");

                int itemSize = value.TypeSize;
                if (itemSize <= 0)
                    throw new ArgumentOutOfRangeException(
                        "typeSize" + "", itemSize, "Element size given by the serializer must be > 0");

                m_serializer = value;
                m_itemSize = itemSize;
                EnableMemoryMappedFileAccess = value.SupportsMemoryMappedFiles;
            }
        }

        public override sealed IBinSerializer NonGenericSerializer
        {
            get { return Serializer; }
        }

        protected internal override sealed void SetSerializer(IBinSerializer nonGenericSerializer)
        {
            m_serializer = (IBinSerializer<T>) nonGenericSerializer;
        }

        protected void Read(long firstItemIdx, T[] buffer, int bufOffset, int bufCount)
        {
            PerformFileAccess(firstItemIdx, buffer, bufOffset, bufCount, false);
        }

        protected void Write(long firstItemIdx, T[] buffer, int bufOffset, int bufCount)
        {
            PerformFileAccess(firstItemIdx, buffer, bufOffset, bufCount, true);
        }

        private void PerformFileAccess(long firstItemIdx, T[] buffer, int bufOffset, int bufCount, bool isWriting)
        {
            ThrowOnNotInitialized();
            Utilities.ValidateArrayParams(buffer, bufOffset, bufCount);

            if (firstItemIdx < 0 || firstItemIdx > Count)
                throw new ArgumentOutOfRangeException("firstItemIdx", firstItemIdx, "Must be >= 0 and <= Count");

            if (!isWriting && firstItemIdx + bufCount > Count)
                throw new ArgumentOutOfRangeException(
                    "bufCount", bufCount,
                    "There is not enough data to fulfill this request. FirstItemIndex + BufferCount > Count");

            // Optimize out empty requests
            if (bufCount == 0)
                return;

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
                    m_count = newCount;
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
                long idxToStopAt = firstItemIdx + bufCount;
                long offsetToStopAt = ItemIdxToOffset(idxToStopAt);
                long idxCurrent = firstItemIdx;

                // Grow file if needed
                long fileSize = FileStream.Length;
                if (isWriting && offsetToStopAt > fileSize)
                    fileSize = offsetToStopAt;
                hMap = Win32Apis.CreateFileMapping(
                    FileStream, fileSize,
                    isWriting ? FileMapProtection.PageReadWrite : FileMapProtection.PageReadOnly);

                while (idxCurrent < idxToStopAt)
                {
                    SafeMapViewHandle ptrMapViewBaseAddr = null;
                    try
                    {
                        long offsetCurrent = ItemIdxToOffset(idxCurrent);
                        long mapViewFileOffset = Utilities.RoundDownToMultiple(offsetCurrent, MinPageSize);

                        long mapViewSize = offsetToStopAt - mapViewFileOffset;
                        long itemsToProcessThisRun = idxToStopAt - idxCurrent;
                        if (mapViewSize > MinLargePageSize)
                        {
                            mapViewSize = MinLargePageSize;
                            itemsToProcessThisRun = (mapViewFileOffset + mapViewSize)/ItemSize - idxCurrent -
                                                    CalculateHeaderSizeAsItemCount();
                        }

                        // The size of the new map view.
                        ptrMapViewBaseAddr = Win32Apis.MapViewOfFile(
                            hMap, mapViewFileOffset, mapViewSize,
                            isWriting ? FileMapAccess.Write : FileMapAccess.Read);

                        long totalItemsDone = idxCurrent - firstItemIdx;
                        long bufItemOffset = bufOffset + totalItemsDone;

                        // Access file using memory-mapped pages
                        Serializer.ProcessMemoryMap(
                            (IntPtr) (ptrMapViewBaseAddr.Address + offsetCurrent - mapViewFileOffset),
                            buffer, (int) bufItemOffset, (int) itemsToProcessThisRun, isWriting);

                        idxCurrent += itemsToProcessThisRun;
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
    }
}