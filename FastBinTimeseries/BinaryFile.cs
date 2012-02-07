#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of FastBinTimeseries library
 * 
 *  FastBinTimeseries is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  FastBinTimeseries is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with FastBinTimeseries.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.Collections.Generic;
using System.IO;
using NYurik.FastBinTimeseries.Serializers;

namespace NYurik.FastBinTimeseries
{
    public abstract class BinaryFile<T> : BinaryFile, IBinaryFile
    {
        private const int MinReqSizeToUseMapView = 4*1024; // 4 KB
        private BufferProvider<T> _buffer;
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

        long IBinaryFile.GetItemCount()
        {
            return Count;
        }

        public override sealed Type ItemType
        {
            get { return typeof (T); }
        }

        public override TDst RunGenericMethod<TDst, TArg>(IGenericCallable<TDst, TArg> callable, TArg arg)
        {
            return callable.Run<T>(this, arg);
        }

        #endregion

        /// <summary> Used by <see cref="BinaryFile.Open(System.IO.Stream,System.Func{string,System.Type})"/> when opening an existing file </summary>
        protected override sealed void SetSerializer(IBinSerializer nonGenericSerializer)
        {
            _serializer = (IBinSerializer<T>) nonGenericSerializer;
        }

        [Obsolete("Use streaming methods instead")]
        protected internal int PerformFileAccess(long firstItemIdx, ArraySegment<T> buffer, bool isWriting)
        {
            ThrowOnNotInitialized();

            bool canSeek = BaseStream.CanSeek;

            if (!canSeek && firstItemIdx != 0)
                throw new ArgumentOutOfRangeException(
                    "firstItemIdx", firstItemIdx,
                    "Must be 0 when the base stream is not seekable");

            if (canSeek && (firstItemIdx < 0 || firstItemIdx > GetCount()))
                throw new ArgumentOutOfRangeException("firstItemIdx", firstItemIdx, "Must be >= 0 and <= Count");

            // Optimize empty requests
            if (buffer.Count == 0)
                return 0;

            int ret;
            if (IsFileStream
                && ((isWriting && EnableMemMappedAccessOnWrite) || (!isWriting && EnableMemMappedAccessOnRead))
                && ItemIdxToOffset(buffer.Count) > MinReqSizeToUseMapView)
            {
                ret = ProcessMemoryMappedFile(firstItemIdx, buffer, isWriting, BaseStream.Length);
            }
            else
            {
                ret = ProcessStream(firstItemIdx, buffer, isWriting);
                if (isWriting)
                    BaseStream.Flush();
            }

            return ret;
        }

        /// Access file using Stream object
        private int ProcessStream(long firstItemIdx, ArraySegment<T> buffer, bool isWriting)
        {
            bool canSeek = BaseStream.CanSeek;
            long fileOffset = 0;

            if (canSeek)
            {
                fileOffset = ItemIdxToOffset(firstItemIdx);
                BaseStream.Seek(fileOffset, SeekOrigin.Begin);
            }

            var fs = BaseStream as FileStream;
            int count = fs != null
                            ? Serializer.ProcessFileStream(fs, buffer, isWriting)
                            : ProcessManagedStream(buffer, isWriting);

            if (canSeek)
            {
                long expectedStreamPos = fileOffset + buffer.Count*ItemSize;
                if (expectedStreamPos != BaseStream.Position)
                    throw new BinaryFileException(
                        "Possible loss of data or file corruption detected.\n" +
                        "Unexpected position in the data stream: after {0} {1} items, position should have moved " +
                        "from 0x{2:X} to 0x{3:X}, but instead is now at 0x{4:X}.",
                        isWriting ? "writing" : "reading",
                        buffer.Count, fileOffset, expectedStreamPos, BaseStream.Position);
            }

            return count;
        }

        /// Use additional byte buffer for stream operations. Slowest method.
        private int ProcessManagedStream(ArraySegment<T> buffer, bool isWriting)
        {
            const int maxBufferSize = 512*1024; // 512 KB

            int offset = buffer.Offset;
            int count = buffer.Count;

            int itemSize = ItemSize;
            int tempBufSize = Math.Min(FastBinFileUtils.RoundUpToMultiple(maxBufferSize, itemSize), count*itemSize);
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
                        Serializer.ProcessMemoryPtr(
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

        private int ProcessMemoryMappedFile(long firstItemIdx, ArraySegment<T> buffer, bool isWriting, long fileSize)
        {
            SafeMapHandle hMap = null;
            try
            {
                bool isAligned;
                long fileCount = CalculateItemCountFromFilePosition(fileSize, out isAligned);
                if (!isAligned && isWriting)
                    throw new BinaryFileException(
                        "Cannot write to a file when its length does not align to item size ({0})",
                        ToString());

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
                        Serializer.ProcessMemoryPtr(
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

        /// <summary>
        /// Enumerate items by block either in order or in reverse order, begining at the <paramref name="firstItemIdx"/>.
        /// </summary>
        /// <param name="firstItemIdx">The index of the first block to read (both forward and backward). Invalid values will be adjusted to existing data.</param>
        /// <param name="enumerateInReverse">Set to true to enumerate in reverse, false otherwise</param>
        /// <param name="bufferProvider">Provides buffers (or re-yields the same buffer) for each new result. Could be null for automatic</param>
        /// <param name="maxItemCount">Maximum number of items to return</param>
        /// <param name="cachedCount">Use if <see cref="BinaryFile.GetCount"/> was called right before (avoids additional kernel call)</param>
        protected internal IEnumerable<ArraySegment<T>> PerformStreaming(
            long firstItemIdx, bool enumerateInReverse, IEnumerable<Buffer<T>> bufferProvider = null,
            long maxItemCount = long.MaxValue, long cachedCount = -1)
        {
            ThrowOnNotInitialized();
            if (maxItemCount < 0)
                throw new ArgumentOutOfRangeException("maxItemCount", maxItemCount, "Must be >= 0");
            if (maxItemCount == 0)
                yield break;

            bool canSeek = BaseStream.CanSeek;
            long fileCount;
            long fileSize;

            if (canSeek)
            {
                if (cachedCount < 0)
                {
                    fileSize = BaseStream.Length;
                    bool isAligned;
                    fileCount = CalculateItemCountFromFilePosition(fileSize, out isAligned);
                }
                else
                {
                    fileSize = ItemIdxToOffset(cachedCount);
                    fileCount = cachedCount;

                    // TODO: delete
                    bool isAligned;
                    if (fileCount != CalculateItemCountFromFilePosition(fileSize, out isAligned))
                        throw new Exception();
                }
            }
            else
            {
                fileCount = 0;
                fileSize = 0;
            }

            long idx;
            if (enumerateInReverse)
            {
                if (!canSeek)
                    throw new NotSupportedException("Reverse enumeration is not supported when Stream.CanSeek == false");
                idx = Math.Min(firstItemIdx, fileCount - 1);
                if (idx < 0)
                    yield break;
            }
            else
            {
                if (!canSeek && firstItemIdx != 0)
                    throw new ArgumentOutOfRangeException(
                        "firstItemIdx", firstItemIdx,
                        "Must be 0 when the base stream is not seekable");

                idx = Math.Max(firstItemIdx, 0);
                if (canSeek && idx >= fileCount)
                    yield break;
            }

            IEnumerable<Buffer<T>> buffers =
                bufferProvider
                ?? (_buffer ?? (_buffer = new BufferProvider<T>()))
                       .YieldMaxGrowingBuffer(maxItemCount, 16*MinPageSize/ItemSize, 5, MaxLargePageSize/ItemSize);

            bool? useMemMappedAccess = null;

            // buffer provider should be an infinite loop
            foreach (var buffer in buffers)
            {
                if (buffer.Count == 0)
                    throw new BinaryFileException("BufferProvider returned an empty buffer");

                long itemsLeft = !canSeek ? long.MaxValue : (enumerateInReverse ? idx + 1 : fileCount - idx);
                if (itemsLeft <= 0)
                    yield break;

                int readSize = itemsLeft < buffer.Count ? (int) itemsLeft : buffer.Count;
                if (readSize > maxItemCount)
                    readSize = (int) maxItemCount;
                buffer.Count = readSize;

                long readBlockFrom = enumerateInReverse ? idx - readSize + 1 : idx;

                if (useMemMappedAccess == null)
                    useMemMappedAccess = UseMemoryMappedAccess(readSize, false);

                int read = PerformUnsafeBlockAccess(
                    readBlockFrom, false, buffer.AsArraySegment(), fileSize,
                    useMemMappedAccess.Value);

                if (enumerateInReverse)
                {
                    if (read != readSize)
                        throw new SerializerException(
                            "Unexpected number of items read during reverse traversal. {0} was expected, {1} returned",
                            readSize, read);

                    yield return buffer.AsArraySegment();
                    idx = idx - readSize;
                }
                else
                {
                    if (read == 0)
                        yield break;

                    yield return new ArraySegment<T>(buffer.Array, 0, read);

                    if (canSeek)
                        idx += readSize;
                }

                if (maxItemCount < long.MaxValue)
                {
                    maxItemCount -= readSize;
                    if (maxItemCount <= 0)
                        yield break;
                }
            }
        }

        protected int PerformUnsafeBlockAccess(long firstItemIdx, bool isWriting, ArraySegment<T> buffer, long fileSize,
                                               bool useMemMappedAccess)
        {
            return useMemMappedAccess
                       ? ProcessMemoryMappedFile(firstItemIdx, buffer, isWriting, fileSize)
                       : ProcessStream(firstItemIdx, buffer, isWriting);
        }

        /// Return true if it is recomended to use memory mapped access for blocks of the given size
        protected bool UseMemoryMappedAccess(int blockSize, bool isWriting)
        {
            return IsFileStream
                   && EnableMemMappedAccessOnRead
                   && ItemIdxToOffset(blockSize) > MinReqSizeToUseMapView;
        }

        /// <summary>
        /// Write segment stream to internal stream, optionally truncating the file so that <paramref name="firstItemIdx"/> would be the first written item.
        /// </summary>
        /// <param name="streamEnmr">The stream of array segments to write, with a single MoveNext() already performed (returned true)</param>
        /// <param name="firstItemIdx">The index of the first element in the stream. The file will be truncated if the value is less than or equal to Count</param>
        protected void PerformWriteStreaming(IEnumerator<ArraySegment<T>> streamEnmr, long firstItemIdx = long.MaxValue)
        {
            ThrowOnNotInitialized();
            if (firstItemIdx < long.MaxValue)
                PerformTruncateFile(firstItemIdx);

            bool canSeek = BaseStream.CanSeek;
            long fileSize = 0, itemIdx = 0;
            if (canSeek)
            {
                fileSize = BaseStream.Length;
                bool isAligned;
                itemIdx = CalculateItemCountFromFilePosition(fileSize, out isAligned);
            }

            bool? useMemMappedAccess = null;

            // Have to call Count on every access 
            do
            {
                ArraySegment<T> seg = streamEnmr.Current;
                if (seg.Count == 0)
                    continue;

                if (useMemMappedAccess == null)
                    useMemMappedAccess = UseMemoryMappedAccess(seg.Count, true);

                int read = PerformUnsafeBlockAccess(itemIdx, true, seg, fileSize, useMemMappedAccess.Value);

                if (canSeek)
                {
                    itemIdx += read;
                    fileSize += read*ItemSize;
                }
            } while (streamEnmr.MoveNext());

            if (useMemMappedAccess == false)
                BaseStream.Flush();
        }
    }
}