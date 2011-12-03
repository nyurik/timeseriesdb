using System;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class CodecWriter : CodecBase
    {
        /// All buffers are created slightly bigger than needed so that WriteOperations do not need to check for buffer end for every byte
        private const int PaddingSize = 10;

        public readonly byte[] Buffer;
        public readonly int BufferSize;
        private int _bufferPos;

        /// <summary> Create writing codec. The internal buffer is padded with extra space. </summary>
        public CodecWriter(int bufferSize)
        {
            if (bufferSize <= 0)
                throw new ArgumentOutOfRangeException("bufferSize", bufferSize, "Must be positive");
            BufferSize = bufferSize;
            Buffer = new byte[bufferSize + PaddingSize];
        }

        public int BufferPos
        {
            get { return _bufferPos; }
            set
            {
                if (value < 0 || value > BufferSize)
                    throw new ArgumentOutOfRangeException("value", value, "Must be >= 0 && <= " + BufferSize);
                _bufferPos = value;
            }
        }

        public ArraySegment<byte> UsedBuffer
        {
            get { return new ArraySegment<byte>(Buffer, 0, BufferPos); }
        }

            #region Header

        [UsedImplicitly]
        internal void SkipHeader()
        {
            _bufferPos += HeaderSize;
        }

        [UsedImplicitly]
        internal void WriteHeader(int count)
        {
            byte[] tmp = BitConverter.GetBytes(count);
            Array.Copy(tmp, Buffer, HeaderSize);
        }

        #endregion

        #region Writing values

        [UsedImplicitly]
        internal void WriteUnsignedValue(ulong value)
        {
            ThrowIfNotEnoughSpace();
            while (value > 127)
            {
                Buffer[_bufferPos++] = (byte) (value & 0x7F | 0x80);
                value >>= 7;
            }
            Buffer[_bufferPos++] = (byte) value;
        }

        [UsedImplicitly]
        internal bool WriteByte(byte value)
        {
            ThrowIfNotEnoughSpace();
            if (_bufferPos >= BufferSize)
                return false;

            Buffer[_bufferPos++] = value;
            return true;
        }

        [UsedImplicitly]
        internal bool WriteSignedValue(long value)
        {
            ThrowIfNotEnoughSpace();
            int pos = _bufferPos;

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

            if (pos > BufferSize)
                return false;

            _bufferPos = pos;
            return true;
        }

        #endregion

        #region Exceptions

        [UsedImplicitly]
        internal void ThrowOverflow<T>(T value)
        {
            throw new OverflowException(string.Format("Value {0} cannot be stored", value));
        }

        private void ThrowIfNotEnoughSpace()
        {
            if (_bufferPos >= BufferSize + PaddingSize)
                throw new SerializerException(
                    "Unable to perform write operation - buffer[{0}] already has {1} bytes", BufferSize, BufferPos);
        }

        #endregion
    }
}