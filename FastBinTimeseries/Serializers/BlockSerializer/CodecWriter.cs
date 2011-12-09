using System;
using System.Security.Cryptography;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class CodecWriter : CodecBase, IDisposable
    {
        /// All buffers are created slightly bigger than needed so that WriteOperations do not need to check for buffer end for every byte
        private const int PaddingSize = MaxBytesFor64;

        public readonly byte[] Buffer;
        public readonly int BufferSize;
        private readonly HashAlgorithm _hashAlgorithm;
        private int _count;

        /// <summary> Create writing codec. The internal buffer is padded with extra space. </summary>
        public CodecWriter(int bufferSize)
        {
            if (bufferSize <= ReservedSpace)
                throw new ArgumentOutOfRangeException("bufferSize", bufferSize, "Must be > " + ReservedSpace);
            BufferSize = bufferSize;
            Buffer = new byte[bufferSize + PaddingSize];
            _hashAlgorithm = new MD5CryptoServiceProvider();
        }

        public int Count
        {
            get { return _count; }
            set
            {
                if (value < 0 || value > BufferSize)
                    throw new ArgumentOutOfRangeException("value", value, "Must be >= 0 && <= " + BufferSize);
                _count = value;
            }
        }

        public ArraySegment<byte> UsedBuffer
        {
            get { return new ArraySegment<byte>(Buffer, 0, Count); }
        }

        #region Header & Hash calculation

        [UsedImplicitly]
        internal void FinishBlock(int count, bool makeFullBlock)
        {
            // Write needed bytes at the end, calculate how many bytes it took
            // shift array right and copy to the value to the beginning.
            ValidateCount((ulong) count);

            int tmp = _count;
            WriteUnsignedValue((uint) count); // Can be read by 64bit unsigned reader
            tmp = _count - tmp;

            Array.Copy(Buffer, 0, Buffer, tmp, _count);
            Array.Copy(Buffer, _count, Buffer, 0, tmp);

            // Write as many bytes as would fit of the MD5 signature
            byte[] hash = _hashAlgorithm.ComputeHash(Buffer, 0, _count);
            int hashSize = hash.Length;
            if (hashSize >= BufferSize - _count)
                hashSize = BufferSize - _count;
            Array.Copy(hash, 0, Buffer, _count, hashSize);
            _count += hashSize;

            if (makeFullBlock && _count < BufferSize)
            {
                Array.Clear(Buffer, _count, BufferSize - _count);
                _count = BufferSize;
            }
        }

        #endregion

        #region Writing values

        [UsedImplicitly]
        internal void WriteUnsignedValue(ulong value)
        {
            ThrowIfNotEnoughSpace(MaxBytesFor64);
            int count = _count;
            while (value > 127)
            {
                Buffer[count++] = (byte) (value & 0x7F | 0x80);
                value >>= 7;
            }
            Buffer[count++] = (byte) value;
            _count = count;
        }

        [UsedImplicitly]
        internal void WriteUnsignedValue(uint value)
        {
            ThrowIfNotEnoughSpace(MaxBytesFor32);
            int count = _count;
            while (value > 127)
            {
                Buffer[count++] = (byte) (value & 0x7F | 0x80);
                value >>= 7;
            }
            Buffer[count++] = (byte) value;
            _count = count;
        }

        [UsedImplicitly]
        internal bool WriteByte(byte value)
        {
            ThrowIfNotEnoughSpace(MaxBytesFor8);
            if (_count >= BufferSize - ReservedSpace)
                return false;

            Buffer[_count++] = value;
            return true;
        }

        [UsedImplicitly]
        internal bool WriteSignedValue(long value)
        {
            ThrowIfNotEnoughSpace(MaxBytesFor64);
            int pos = _count;

            if (value < 0)
            {
                while (true)
                {
                    var v = (byte) (value & 0x7F);
                    value = value >> 7;

                    // Shifting a signed value right fills leftmost positions with 1s
                    if (value != ~0 || (v & 0x40) == 0)
                        Buffer[pos++] = (byte) (v | 0x80);
                    else
                    {
                        Buffer[pos++] = v;
                        break;
                    }
                }
            }
            else
            {
                while (true)
                {
                    var v = (byte) (value & 0x7F);
                    value = value >> 7;

                    if (value != 0 || (v & 0x40) != 0)
                        Buffer[pos++] = (byte) (v | 0x80);
                    else
                    {
                        Buffer[pos++] = v;
                        break;
                    }
                }
            }

            if (pos > BufferSize - ReservedSpace)
                return false;

            _count = pos;
            return true;
        }

        #endregion

        #region Exceptions

        [UsedImplicitly]
        internal void ThrowOverflow<T>(T value)
        {
            throw new OverflowException(string.Format("Value {0} cannot be stored", value));
        }

        private void ThrowIfNotEnoughSpace(byte neededSpace)
        {
            if (_count + neededSpace >= Buffer.Length)
                throw new SerializerException(
                    "Unable to perform write operation requiring {0} bytes - buffer[{1}] is {2} bytes full",
                    neededSpace, BufferSize, Count);
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            _hashAlgorithm.Dispose();
        }

        #endregion
    }
}