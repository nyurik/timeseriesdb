using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public interface IBinBlock<THeader, TItem>
    {
        THeader Header { get; set; }
        TItem[] Items { get; }
    }

    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class BinaryBlockAttribute : Attribute
    {
        public BinaryBlockAttribute(int itemCount)
        {
            ItemCount = itemCount;
        }

        public int ItemCount { get; private set; }
    }

    public class BinBlockSerializer<TObject, THeader, TItem> : IBinSerializer<TObject>
        where TObject : class, IBinBlock<THeader, TItem>, new()
        where THeader : struct
        where TItem : struct
    {
        private static readonly Version Version10 = new Version(1, 0);
        private static readonly int ItemCount;
        private readonly int _itemTypeSize;
        private IBinSerializer<TItem> _dataSerializer;
        private IBinSerializer<THeader> _headerSerializer;
        private Version _version = Version10;

        static BinBlockSerializer()
        {
            BinaryBlockAttribute attr = typeof (TObject)
                .GetCustomAttributes<BinaryBlockAttribute>(false)
                .FirstOrDefault();
            if (attr == null)
                throw new InvalidOperationException(
                    string.Format("Class {0} must have a [{1}] attribute",
                                  typeof (TObject).Name, typeof (BinaryBlockAttribute).Name));

            ItemCount = attr.ItemCount;
        }

        public BinBlockSerializer()
        {
            _headerSerializer = new DefaultTypeSerializer<THeader>();
            _dataSerializer = new DefaultTypeSerializer<TItem>();
            _itemTypeSize = _headerSerializer.TypeSize + _dataSerializer.TypeSize*ItemCount;
        }

        #region IBinSerializer<TObject> Members

        public Version Version
        {
            get { return _version; }
        }

        public int TypeSize
        {
            get { return _itemTypeSize; }
        }

        public bool SupportsMemoryMappedFiles
        {
            get { return true; }
        }

        public void Init(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            var ver = reader.ReadVersion();
            if (ver != Version10)
                throw FastBinFileUtils.GetUnknownVersionException(ver, GetType());

            _version = ver;

            _headerSerializer = reader.ReadTypeAndInstantiate<IBinSerializer<THeader>>(typeMap, false);
            _headerSerializer.Init(reader, typeMap);

            _dataSerializer = reader.ReadTypeAndInstantiate<IBinSerializer<TItem>>(typeMap, false);
            _dataSerializer.Init(reader, typeMap);
        }

        public void WriteCustomHeader(BinaryWriter writer)
        {
            writer.WriteVersion(_version);

            writer.WriteType(_headerSerializer);
            _headerSerializer.WriteCustomHeader(writer);

            writer.WriteType(_dataSerializer);
            _dataSerializer.WriteCustomHeader(writer);
        }

        public void ProcessFileStream(FileStream fileStream, ArraySegment<TObject> buffer, bool isWriting)
        {
            if (fileStream == null) throw new ArgumentNullException("fileStream");
            Process(fileStream, IntPtr.Zero, buffer, isWriting);
        }

        public void ProcessMemoryMap(IntPtr memMapPtr, ArraySegment<TObject> buffer, bool isWriting)
        {
            if (memMapPtr == IntPtr.Zero) throw new ArgumentNullException("memMapPtr");
            Process(null, memMapPtr, buffer, isWriting);
        }

        public bool BinaryArrayCompare(ArraySegment<TObject> buffer1, ArraySegment<TObject> buffer2)
        {
            throw new NotSupportedException();
        }

        #endregion

        private void Process(FileStream fileStream, IntPtr memMapPtr, ArraySegment<TObject> buffer,
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
                if (items == null || items.Length != ItemCount)
                    throw new InvalidOperationException(
                        string.Format("Class {0} must always have Items property set to an array of size {1}",
                                      typeof (TObject).Name, ItemCount));

                // Serialize header struct
                if (useMmf)
                {
                    _headerSerializer.ProcessMemoryMap(memMapPtr, hdrSegment, isWriting);
                    memMapPtr = (IntPtr) (memMapPtr.ToInt64() + _headerSerializer.TypeSize);
                }
                else
                    _headerSerializer.ProcessFileStream(fileStream, hdrSegment, isWriting);

                if (!isWriting)
                    block.Header = hdrArray[0];

                // Serialize items array
                if (useMmf)
                {
                    _dataSerializer.ProcessMemoryMap(memMapPtr, new ArraySegment<TItem>(items), isWriting);
                    memMapPtr = (IntPtr) (memMapPtr.ToInt64() + _dataSerializer.TypeSize*ItemCount);
                }
                else
                    _dataSerializer.ProcessFileStream(fileStream, new ArraySegment<TItem>(items), isWriting);
            }
        }
    }
}