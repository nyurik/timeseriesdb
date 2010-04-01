using System;
using System.Collections.Generic;
using System.IO;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile<T> : BinaryFile, IBinaryFile
    {
        private const int MinReqSizeToUseMapView = 4 * 1024; // 4 KB

        private IBinSerializer<T> _serializer;

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
            Type typeT = typeof (T);
            BinarySerializerAttribute[] attr = typeT.GetCustomAttributes<BinarySerializerAttribute>(false);
            if (attr.Length >= 1)
            {
                Type typeSer = attr[0].BinSerializerType;
                if (typeSer.IsGenericTypeDefinition)
                    typeSer = typeSer.MakeGenericType(typeT);

                var ser = Activator.CreateInstance(typeSer) as IBinSerializer<T>;
                if (ser == null)
                    throw new ArgumentException(
                        string.Format("Custom binary serializer for type {0} does not implement IBinSerializer<{0}>",
                                      typeT.Name));
                Serializer = ser;
            }
            else
            {
                // Initialize default serializer
                Serializer = new DefaultTypeSerializer<T>();
            }
        }

        public IBinSerializer<T> Serializer
        {
            get
            {
                ThrowOnDisposed();
                return _serializer;
            }
            set
            {
                ThrowOnInitialized();
                if (value == null)
                    throw new ArgumentNullException("value");
                _serializer = value;
            }
        }

        #region IBinaryFile Members

        public override sealed IBinSerializer NonGenericSerializer
        {
            get { return Serializer; }
        }

        long IStoredSeries.GetItemCount()
        {
            return Count;
        }

        public override sealed Type ItemType
        {
            get { return typeof (T); }
        }

        Array IStoredSeries.GenericReadData(long firstItemIdx, int count)
        {
            if (firstItemIdx < 0 || firstItemIdx > Count)
                throw new ArgumentOutOfRangeException(
                    "firstItemIdx", firstItemIdx, string.Format("Accepted range [0:{0}]", Count));
            if (count < 0)
                throw new ArgumentOutOfRangeException("count", count, "Must be non-negative");

            var result = new T[(int) Math.Min(Count - firstItemIdx, count)];

            PerformRead(firstItemIdx, new ArraySegment<T>(result));

            return result;
        }

        #endregion

        /// <summary> Used by <see cref="BinaryFile.Open(FileStream,System.Collections.Generic.IDictionary{string,System.Type})"/> when opening an existing file </summary>
        protected override sealed void SetSerializer(IBinSerializer nonGenericSerializer)
        {
            _serializer = (IBinSerializer<T>) nonGenericSerializer;
        }

        protected void PerformRead(long firstItemIdx, ArraySegment<T> buffer)
        {
            PerformFileAccess(firstItemIdx, buffer, false);
        }

        protected void PerformWrite(long firstItemIdx, ArraySegment<T> buffer)
        {
            PerformFileAccess(firstItemIdx, buffer, true);
        }

        private void PerformFileAccess(long firstItemIdx, ArraySegment<T> buffer, bool isWriting)
        {
            ThrowOnNotInitialized();
            if (firstItemIdx < 0 || firstItemIdx > Count)
                throw new ArgumentOutOfRangeException("firstItemIdx", firstItemIdx, "Must be >= 0 and <= Count");

            if (!isWriting && firstItemIdx + buffer.Count > Count)
                throw new ArgumentOutOfRangeException(
                    "buffer", buffer.Count,
                    "There is not enough data to fulfill this request. FirstItemIndex + Buffer.Count > Count");

            // Optimize empty requests
            if (buffer.Count == 0)
                return;

            bool useMemMapping = (
                                     (isWriting && EnableMemMappedAccessOnWrite)
                                     ||
                                     (!isWriting && EnableMemMappedAccessOnRead)
                                 )
                                 && ItemIdxToOffset(buffer.Count) > MinReqSizeToUseMapView;

            if (useMemMapping)
                ProcessFileMmf(firstItemIdx, buffer, isWriting);
            else
                ProcessFileNoMmf(firstItemIdx, buffer, isWriting);

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
        private void ProcessFileNoMmf(long firstItemIdx, ArraySegment<T> buffer, bool isWriting)
        {
            long fileOffset = ItemIdxToOffset(firstItemIdx);

            FileStream.Seek(fileOffset, SeekOrigin.Begin);
            Serializer.ProcessFileStream(FileStream, buffer, isWriting);

            long expectedStreamPos = fileOffset + buffer.Count*ItemSize;
            if (expectedStreamPos != FileStream.Position)
                throw new InvalidOperationException(
                    String.Format(
                        "Possible loss of data or file corruption detected.\n" +
                        "Unexpected position in the data stream: after {0} {1} items, position should have moved " +
                        "from 0x{2:X} to 0x{3:X}, but instead is now at 0x{4:X}.",
                        isWriting ? "writing" : "reading",
                        buffer.Count, fileOffset, expectedStreamPos, FileStream.Position));
        }

        private void ProcessFileMmf(long firstItemIdx, ArraySegment<T> buffer, bool isWriting)
        {
            SafeMapHandle hMap = null;
            try
            {
                long idxToStopAt = firstItemIdx + buffer.Count;
                long offsetToStopAt = ItemIdxToOffset(idxToStopAt);
                long idxCurrent = firstItemIdx;

                // Grow file if needed
                long fileSize = FileStream.Length;
                if (isWriting && offsetToStopAt > fileSize)
                    fileSize = offsetToStopAt;
                hMap = NativeWinApis.CreateFileMapping(
                    FileStream, fileSize,
                    isWriting ? FileMapProtection.PageReadWrite : FileMapProtection.PageReadOnly);

                while (idxCurrent < idxToStopAt)
                {
                    SafeMapViewHandle ptrMapViewBaseAddr = null;
                    try
                    {
                        long offsetCurrent = ItemIdxToOffset(idxCurrent);
                        long mapViewFileOffset = FastBinFileUtils.RoundDownToMultiple(offsetCurrent, MinPageSize);

                        long mapViewSize = offsetToStopAt - mapViewFileOffset;
                        long itemsToProcessThisRun = idxToStopAt - idxCurrent;
                        if (mapViewSize > MaxLargePageSize)
                        {
                            mapViewSize = MaxLargePageSize;
                            itemsToProcessThisRun = (mapViewFileOffset + mapViewSize)/ItemSize - idxCurrent -
                                                    CalculateHeaderSizeAsItemCount();
                        }

                        // The size of the new map view.
                        ptrMapViewBaseAddr = NativeWinApis.MapViewOfFile(
                            hMap, mapViewFileOffset, mapViewSize,
                            isWriting ? FileMapAccess.Write : FileMapAccess.Read);

                        long totalItemsDone = idxCurrent - firstItemIdx;
                        long bufItemOffset = buffer.Offset + totalItemsDone;

                        // Access file using memory-mapped pages
                        Serializer.ProcessMemoryMap(
                            (IntPtr) (ptrMapViewBaseAddr.Address + offsetCurrent - mapViewFileOffset),
                            new ArraySegment<T>(buffer.Array, (int) bufItemOffset, (int) itemsToProcessThisRun),
                            isWriting);

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

        public override TDst CreateWrappedObject<TDst>(IWrapperFactory factory)
        {
            return factory.Create<BinaryFile<T>, TDst, T>(this);
        }

        /// <summary>
        /// Enumerate items by block either in order or in reverse order, begining at the <paramref name="firstItemIdx"/>.
        /// </summary>
        /// <param name="firstItemIdx">The index of the first block to read (both forward and backward). Invalid values will be adjusted to existing data.</param>
        /// <param name="enumerateInReverse">Set to true to enumerate in reverse, false otherwise</param>
        /// <param name="bufferSize">The size of the internal buffer to read data. Set to 0 to make internal buffer autogrow with time</param>
        protected IEnumerable<ArraySegment<T>> PerformStreaming(long firstItemIdx, bool enumerateInReverse,
                                                                int bufferSize)
        {
            if (bufferSize < 0)
                throw new ArgumentOutOfRangeException("bufferSize", bufferSize, "Must be >= 0");

            long idx;
            if (enumerateInReverse)
            {
                idx = Math.Min(firstItemIdx, Count - 1);
                if (idx < 0)
                    yield break;
            }
            else
            {
                idx = Math.Max(firstItemIdx, 0);
                if (idx >= Count)
                    yield break;
            }

            bool autogrow = bufferSize == 0;
            var buffer = new T[autogrow ? 16*MinPageSize/ItemSize : bufferSize];
            int iterations = 0;

            while (true)
            {
                long itemsLeft = enumerateInReverse ? idx + 1 : Count - idx;

                if (itemsLeft <= 0)
                    yield break;

                if (autogrow && iterations > 10)
                {
                    // switch to larger blocks
                    buffer = new T[Math.Min(MaxLargePageSize/ItemSize, itemsLeft)];
                    autogrow = false;
                }

                var readSize = (int) Math.Min(itemsLeft, buffer.Length);
                var block = new ArraySegment<T>(buffer, 0, readSize);

                if (enumerateInReverse)
                {
                    PerformRead(idx - readSize + 1, block);
                    yield return block;
                    idx = idx - readSize;
                }
                else
                {
                    PerformRead(idx, block);
                    yield return block;
                    idx = idx + readSize;
                }

                iterations++;
            }
        }
    }
}