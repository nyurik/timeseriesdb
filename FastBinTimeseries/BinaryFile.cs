using System;
using System.Collections.Generic;
using System.IO;
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
            Serializer = GetDefaultSerializer<T>();
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

            PerformFileAccess(firstItemIdx, new ArraySegment<T>(result), false);

            return result;
        }

        #endregion

        /// <summary> Used by <see cref="BinaryFile.Open(Stream,System.Collections.Generic.IDictionary{string,System.Type})"/> when opening an existing file </summary>
        protected override sealed void SetSerializer(IBinSerializer nonGenericSerializer)
        {
            _serializer = (IBinSerializer<T>) nonGenericSerializer;
        }

        protected int PerformFileAccess(long firstItemIdx, ArraySegment<T> buffer, bool isWriting)
        {
            ThrowOnNotInitialized();

            var canSeek = BaseStream.CanSeek;
            
            if (!canSeek && firstItemIdx != 0)
                throw new ArgumentOutOfRangeException("firstItemIdx", firstItemIdx,
                                                      "Must be 0 when the base stream is not seekable");

            if (canSeek && (firstItemIdx < 0 || firstItemIdx > Count))
                throw new ArgumentOutOfRangeException("firstItemIdx", firstItemIdx, "Must be >= 0 and <= Count");

            // Optimize empty requests
            if (buffer.Count == 0)
                return 0;

            int ret;
            if (IsFileStream
                && ((isWriting && EnableMemMappedAccessOnWrite) || (!isWriting && EnableMemMappedAccessOnRead))
                && ItemIdxToOffset(buffer.Count) > MinReqSizeToUseMapView)
            {
                ret = ProcessMemoryMappedFile(firstItemIdx, buffer, isWriting);
            }
            else
            {
                ret = ProcessStreamSlow(firstItemIdx, buffer, isWriting);
                if (isWriting)
                    BaseStream.Flush();
            }

            return ret;
        }

        /// Access file using Stream object
        private int ProcessStreamSlow(long firstItemIdx, ArraySegment<T> buffer, bool isWriting)
        {
            var canSeek = BaseStream.CanSeek;
            long fileOffset = 0;

            if (canSeek)
            {
                fileOffset = ItemIdxToOffset(firstItemIdx);
                BaseStream.Seek(fileOffset, SeekOrigin.Begin);
            }

            var fs = BaseStream as FileStream;
            int count = fs != null
                             ? Serializer.ProcessFileStream(fs, buffer, isWriting)
                             : ProcessStreamSlow(buffer, isWriting);

            if (canSeek)
            {
                long expectedStreamPos = fileOffset + buffer.Count*ItemSize;
                if (expectedStreamPos != BaseStream.Position)
                    throw new InvalidOperationException(
                        String.Format(
                            "Possible loss of data or file corruption detected.\n" +
                            "Unexpected position in the data stream: after {0} {1} items, position should have moved " +
                            "from 0x{2:X} to 0x{3:X}, but instead is now at 0x{4:X}.",
                            isWriting ? "writing" : "reading",
                            buffer.Count, fileOffset, expectedStreamPos, BaseStream.Position));
            }

            return count;
        }

        private int ProcessStreamSlow(ArraySegment<T> buffer, bool isWriting)
        {
            const int maxBufferSize = 512*1024; // 512 KB

            ThrowOnNotInitialized();

            int offset = buffer.Offset;
            int count = buffer.Count;

            var itemSize = ItemSize;
            int tempBufSize = Math.Min((int) FastBinFileUtils.RoundUpToMultiple(maxBufferSize, itemSize), count*itemSize);
            var tempBuf = new byte[tempBufSize];
            int tempSize = tempBuf.Length/itemSize;

            while (count > 0)
            {
                int opSize = tempSize < count ? tempSize : count;
                int opByteSize = opSize*itemSize;
                int readBytes = 0;

                if (!isWriting)
                {
                    readBytes = EnsureStreamRead(BaseStream, new ArraySegment<byte>(tempBuf, 0, opByteSize));
                    if (readBytes%itemSize != 0)
                        throw new SerializerException("Unexpected end of stream. Received {0} bytes", readBytes);
                    opSize = readBytes/itemSize;
                }

                unsafe
                {
                    fixed (byte* p = &tempBuf[0])
                    {
                        Serializer.ProcessMemoryMap(
                            (IntPtr) p,
                            new ArraySegment<T>(buffer.Array, offset, opSize),
                            isWriting);
                    }
                }

                if (isWriting)
                {
                    // Write into the stream
                    BaseStream.Write(tempBuf, 0, opByteSize);
                }
                else if (readBytes != opByteSize)
                {
                    // Finish early - the stream is done
                    return buffer.Count - count + opSize;
                }

                offset += opSize;
                count -= opSize;
            }

            return buffer.Count;
        }

        private int ProcessMemoryMappedFile(long firstItemIdx, ArraySegment<T> buffer, bool isWriting)
        {
            SafeMapHandle hMap = null;
            try
            {
                long fileSize = BaseStream.Length;
                long fileCount = CalculateItemCountFromFilePosition(fileSize);
                long idxToStopAt = firstItemIdx + buffer.Count;

                if (!isWriting && idxToStopAt > fileCount)
                    idxToStopAt = fileCount;
                
                long offsetToStopAt = ItemIdxToOffset(idxToStopAt);
                long idxCurrent = firstItemIdx;

                // Grow file if needed
                if (isWriting && offsetToStopAt > fileSize)
                    fileSize = offsetToStopAt;

                hMap = NativeWinApis.CreateFileMapping(
                    (FileStream) BaseStream, fileSize,
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

                return (int) (idxCurrent - firstItemIdx);
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
            ThrowOnNotInitialized();
            if (bufferSize < 0)
                throw new ArgumentOutOfRangeException("bufferSize", bufferSize, "Must be >= 0");

            long idx;
            var canSeek = BaseStream.CanSeek;
            if (enumerateInReverse)
            {
                if (!canSeek)
                    throw new NotSupportedException("Reverse enumeration is not supported when Stream.CanSeek == false");
                idx = Math.Min(firstItemIdx, Count - 1);
                if (idx < 0)
                    yield break;
            }
            else
            {
                if(!canSeek && firstItemIdx != 0)
                    throw new ArgumentOutOfRangeException("firstItemIdx", firstItemIdx,
                                                          "Must be 0 when the base stream is not seekable");

                idx = Math.Max(firstItemIdx, 0);
                if (canSeek && idx >= Count)
                    yield break;
            }

            bool autogrow = bufferSize == 0;
            var buffer = new T[autogrow ? 16*MinPageSize/ItemSize : bufferSize];
            int iterations = 0;

            while (true)
            {
                long itemsLeft = !canSeek ? long.MaxValue : (enumerateInReverse ? idx + 1 : Count - idx);

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
                    var read = PerformFileAccess(idx - readSize + 1, block, false);
                    if (read != block.Count)
                        throw new SerializerException(
                            "Unexpected number of items read during reverse traversal. {0} was expected, {1} returned",
                            block.Count, read);

                    yield return block;
                    idx = idx - readSize;
                }
                else
                {
                    var read = PerformFileAccess(idx, block, false);
                    if(read == 0)
                        yield break;

                    yield return read < block.Count ? new ArraySegment<T>(block.Array, 0, read) : block;
                    
                    if(canSeek)
                        idx += readSize;
                }

                iterations++;
            }
        }
    }
}