using System;
using System.Collections.Generic;
using System.IO;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class DeltaSerializer<T> : Initializable, IBinSerializer<T>
    {
        // Always allocate a bit extra memory to avoid array bounds checks
        private const int BytePadding = 10;

        // ReSharper disable StaticFieldInGenericType
        private static readonly Version Version10 = new Version(1, 0);
        // ReSharper restore StaticFieldInGenericType

        private int _blockSize;
        private IBinSerializer<byte> _dataSerializer;
        private Version _version = Version10;


        public DeltaSerializer()
        {
            BlockSize = 128*1024;
            _dataSerializer = new DefaultTypeSerializer<byte>();
        }

        public int BlockSize
        {
            get { return _blockSize; }
            set
            {
                ThrowOnInitialized();
                if (value <= 0)
                    throw new ArgumentOutOfRangeException("value", value, "Must be >= 0");
                _blockSize = value;
            }
        }

        #region IBinSerializer<TObject> Members

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
                return _blockSize;
            }
        }

        public Type ItemType
        {
            get { return typeof (T); }
        }

        public bool SupportsMemoryPtrOperations
        {
            get
            {
                ThrowOnNotInitialized();
                return true;
            }
        }

        public void InitNew(BinaryWriter writer)
        {
            ThrowOnInitialized();

            writer.WriteVersion(_version);

            writer.WriteType(_dataSerializer);
            _dataSerializer.InitNew(writer);

            writer.Write(BlockSize);

            IsInitialized = true;
        }

        public void InitExisting(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            ThrowOnInitialized();

            _version = reader.ReadVersion();
            if (_version != Version10)
                throw new IncompatibleVersionException(GetType(), _version);

            _dataSerializer = reader.ReadTypeAndInstantiate<IBinSerializer<byte>>(typeMap, false);
            _dataSerializer.InitExisting(reader, typeMap);

            BlockSize = reader.ReadInt32();

            IsInitialized = true;
        }

        public int ProcessFileStream(FileStream fileStream, ArraySegment<T> buffer, bool isWriting)
        {
            throw new NotImplementedException();
        }

        public void ProcessMemoryPtr(IntPtr memPointer, ArraySegment<T> buffer, bool isWriting)
        {
            throw new NotImplementedException();
        }

        public bool BinaryArrayCompare(ArraySegment<T> buffer1, ArraySegment<T> buffer2)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}