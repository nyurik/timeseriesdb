#region COPYRIGHT
/*
 *     Copyright 2009-2011 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
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

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public interface IBinBlockSerializer : IBinSerializer
    {
        int ItemCount { get; set; }
    }

    public class BinBlockSerializer<TObject, THeader, TItem> : Initializable, IBinBlockSerializer,
                                                               IBinSerializer<TObject>
        where TObject : class, IBinBlock<THeader, TItem>, new()
        where THeader : struct
        where TItem : struct
    {
        // ReSharper disable StaticFieldInGenericType
        private static readonly Version Version10 = new Version(1, 0);
        // ReSharper restore StaticFieldInGenericType

        private IBinSerializer<TItem> _dataSerializer;
        private IBinSerializer<THeader> _headerSerializer;
        private int _itemCount;
        private int _itemTypeSize;
        private Version _version = Version10;

        public BinBlockSerializer()
        {
            _headerSerializer = new DefaultTypeSerializer<THeader>();
            _dataSerializer = new DefaultTypeSerializer<TItem>();
            ItemCount = 100;
        }

        #region IBinBlockSerializer Members

        public int ItemCount
        {
            get { return _itemCount; }
            set
            {
                ThrowOnInitialized();
                if (value <= 0)
                    throw new ArgumentOutOfRangeException("value", value, "Must be >= 0");
                _itemCount = value;
            }
        }

        public Version Version
        {
            get
            {
                ThrowOnNotInitialized();
                return _version;
            }
        }

        public int TypeSize
        {
            get
            {
                ThrowOnNotInitialized();
                return _itemTypeSize;
            }
        }

        public bool SupportsMemoryPtrOperations
        {
            get
            {
                ThrowOnNotInitialized();
                return true;
            }
        }

        public Type ItemType
        {
            get { return typeof (TObject); }
        }

        public void InitNew(BinaryWriter writer)
        {
            ThrowOnInitialized();

            writer.WriteVersion(_version);

            writer.WriteType(_headerSerializer.GetType());
            _headerSerializer.InitNew(writer);

            writer.WriteType(_dataSerializer.GetType());
            _dataSerializer.InitNew(writer);

            writer.Write(ItemCount);

            _itemTypeSize = CalculateTypeSize();
            writer.Write(_itemTypeSize);

            IsInitialized = true;
        }

        public void InitExisting(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            ThrowOnInitialized();

            _version = reader.ReadVersion();
            if (_version != Version10)
                throw new IncompatibleVersionException(GetType(), _version);

            _headerSerializer = reader.ReadTypeAndInstantiate<IBinSerializer<THeader>>(typeMap, false);
            _headerSerializer.InitExisting(reader, typeMap);

            _dataSerializer = reader.ReadTypeAndInstantiate<IBinSerializer<TItem>>(typeMap, false);
            _dataSerializer.InitExisting(reader, typeMap);

            ItemCount = reader.ReadInt32();

            _itemTypeSize = reader.ReadInt32();
            if (CalculateTypeSize() != _itemTypeSize)
                throw FastBinFileUtils.GetItemSizeChangedException(this, null, _itemTypeSize);

            IsInitialized = true;
        }

        #endregion

        #region IBinSerializer<TObject> Members

        public int ProcessFileStream(FileStream fileStream, ArraySegment<TObject> buffer, bool isWriting)
        {
            ThrowOnNotInitialized();
            if (fileStream == null) throw new ArgumentNullException("fileStream");
            return Process(fileStream, IntPtr.Zero, buffer, isWriting);
        }

        public void ProcessMemoryPtr(IntPtr memPointer, ArraySegment<TObject> buffer, bool isWriting)
        {
            ThrowOnNotInitialized();
            if (memPointer == IntPtr.Zero) throw new ArgumentNullException("memPointer");
            Process(null, memPointer, buffer, isWriting);
        }

        public bool BinaryArrayCompare(ArraySegment<TObject> buffer1, ArraySegment<TObject> buffer2)
        {
            if (buffer1.Array == null) throw new ArgumentNullException("buffer1");
            if (buffer2.Array == null) throw new ArgumentNullException("buffer2");

            // minor optimizations
            if (buffer1.Count != buffer2.Count) return false;
            if (buffer1.Count == 0) return true;

            var hdrArray1 = new THeader[1];
            var hdrSegment1 = new ArraySegment<THeader>(hdrArray1);
            var hdrArray2 = new THeader[1];
            var hdrSegment2 = new ArraySegment<THeader>(hdrArray2);

            for (int i = 0; i < buffer1.Count; i++)
            {
                TObject block1 = buffer1.Array[i + buffer1.Offset];
                TObject block2 = buffer1.Array[i + buffer2.Offset];

                hdrArray1[0] = block1.Header;
                hdrArray2[0] = block2.Header;

                TItem[] items1 = block1.Items;
                TItem[] items2 = block2.Items;

                if (!ReferenceEquals(items1, items2))
                {
                    if (items1 == null || items2 == null || items1.Length != items2.Length)
                        return false;
                    if (
                        !_dataSerializer.BinaryArrayCompare(
                            new ArraySegment<TItem>(items1),
                            new ArraySegment<TItem>(items2)))
                        return false;
                }

                if (!_headerSerializer.BinaryArrayCompare(hdrSegment1, hdrSegment2))
                    return false;
            }

            return true;
        }

        #endregion

        private int CalculateTypeSize()
        {
            return _headerSerializer.TypeSize + _dataSerializer.TypeSize*ItemCount;
        }

        private int Process(FileStream fileStream, IntPtr memPtr, ArraySegment<TObject> buffer,
                            bool isWriting)
        {
            bool useMmf = fileStream == null;
            var hdrArray = new THeader[1];
            var hdrSegment = new ArraySegment<THeader>(hdrArray);

            int last = buffer.Offset + buffer.Count;
            for (int i = buffer.Offset; i < last; i++)
            {
                TObject block = buffer.Array[i];

                if (isWriting)
                    hdrArray[0] = block.Header;
                else if (block == null)
                    buffer.Array[i] = block = new TObject();

                TItem[] items = block.Items;
                if (items == null)
                {
                    if (isWriting)
                        throw new SerializerException("Buffer element #{0} Items property is null", i);
                    block.Items = items = new TItem[ItemCount];
                }
                else
                {
                    if (items.Length != ItemCount)
                        throw new SerializerException(
                            "Buffer element #{0} Items property is set to an array of wrong size {1}. {2} was expected",
                            i, items.Length, ItemCount);
                }

                // Serialize header struct
                if (useMmf)
                {
                    _headerSerializer.ProcessMemoryPtr(memPtr, hdrSegment, isWriting);
                    memPtr = (IntPtr) (memPtr.ToInt64() + _headerSerializer.TypeSize);
                }
                else
                {
                    int cnt = _headerSerializer.ProcessFileStream(fileStream, hdrSegment, isWriting);
                    if (!isWriting && cnt == 0)
                        return i - buffer.Offset;
                }

                if (!isWriting)
                    block.Header = hdrArray[0];

                // Serialize items array
                if (useMmf)
                {
                    _dataSerializer.ProcessMemoryPtr(memPtr, new ArraySegment<TItem>(items), isWriting);
                    memPtr = (IntPtr) (memPtr.ToInt64() + _dataSerializer.TypeSize*ItemCount);
                }
                else
                {
                    int cnt = _dataSerializer.ProcessFileStream(fileStream, new ArraySegment<TItem>(items), isWriting);
                    if (!isWriting && cnt < ItemCount)
                        return i - buffer.Offset; // We cave incomplete file, ignore this item
                }
            }

            return buffer.Count;
        }
    }
}