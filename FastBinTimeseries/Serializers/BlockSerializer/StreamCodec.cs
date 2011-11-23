using System;
using System.Runtime.Serialization;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    internal class StreamCodec
    {
        // All buffers are created slightly bigger than
        private const int PaddingSize = 10;

        public readonly byte[] Buffer;
        public readonly int BufferSize;
        private int _bufferPos;

        public StreamCodec(int bufferSize)
        {
            if(bufferSize<=0)
                throw new ArgumentOutOfRangeException("bufferSize", bufferSize, "Must be positive");

            BufferSize = bufferSize;
            Buffer = new byte[bufferSize + PaddingSize];
        }

        public int BufferPos
        {
            get { return _bufferPos; }
            set
            {
                if (value < 0 || value >= BufferSize)
                    throw new ArgumentOutOfRangeException("value", value, "Must be >= 0 && < BufferSize");
                _bufferPos = value;
            }
        }

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

        private void ThrowIfNotEnoughSpace()
        {
            if (_bufferPos >= BufferSize + PaddingSize)
                throw new SerializerException(
                    "Unable to perform write operation - buffer[{0}] already has {1} bytes", BufferSize, BufferPos);
        }

        internal static unsafe void UnsafeWriteUnsignedValue(byte* buff, ref int pos, ulong value)
        {
            int p = pos;
            while (value > 127)
            {
                buff[p++] = (byte) (value & 0x7F | 0x80);
                value >>= 7;
            }
            buff[p++] = (byte) value;
            pos = p;
        }

        internal bool WriteSignedValue(long value)
        {
            ThrowIfNotEnoughSpace();
            int pos = _bufferPos;

            if (value < 0)
            {
                while (true)
                {
                    var v = (byte)(value & 0x7F);
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

        internal static unsafe long ReadSignedValue(byte* buff, ref int pos)
        {
            int p = pos;

            long tmp64 = buff[p];
            if (tmp64 < 128)
            {
                const long mask = 1 << 7*1 - 1;
                if ((tmp64 & mask) != 0)
                    tmp64 |= -mask;
                pos++;
                return tmp64;
            }

            long res64 = tmp64 & 0x7f;
            if ((tmp64 = buff[p + 1]) < 128)
            {
                p += 2;
                res64 |= tmp64 << 7*1;
                const long mask = 1 << 7*2 - 1;
                if ((res64 & mask) != 0)
                    res64 |= -mask;
            }
            else
            {
                res64 |= (tmp64 & 0x7f) << 7*1;
                if ((tmp64 = buff[p + 2]) < 128)
                {
                    p += 3;
                    res64 |= tmp64 << 7*2;
                    const long mask = 1 << 7*3 - 1;
                    if ((res64 & mask) != 0)
                        res64 |= -mask;
                }
                else
                {
                    res64 |= (tmp64 & 0x7f) << 7*2;
                    if ((tmp64 = buff[p + 3]) < 128)
                    {
                        p += 4;
                        res64 |= tmp64 << 7*3;
                        const long mask = 1 << 7*4 - 1;
                        if ((res64 & mask) != 0)
                            res64 |= -mask;
                    }
                    else
                    {
                        res64 = res64 | (tmp64 & 0x7f) << 7*3;
                        if ((tmp64 = buff[p + 4]) < 128)
                        {
                            p += 5;
                            res64 |= tmp64 << 7*4;
                            const long mask = 1L << 7*5 - 1;
                            if ((res64 & mask) != 0)
                                res64 |= -mask;
                        }
                        else
                        {
                            res64 |= (tmp64 & 0x7f) << 7*4;
                            if ((tmp64 = buff[p + 5]) < 128)
                            {
                                p += 6;
                                res64 |= tmp64 << 7*5;
                                const long mask = 1L << 7*6 - 1;
                                if ((res64 & mask) != 0)
                                    res64 |= -mask;
                            }
                            else
                            {
                                res64 |= (tmp64 & 0x7f) << 7*5;
                                if ((tmp64 = buff[p + 6]) < 128)
                                {
                                    p += 7;
                                    res64 |= tmp64 << 7*6;
                                    const long mask = 1L << 7*7 - 1;
                                    if ((res64 & mask) != 0)
                                        res64 |= -mask;
                                }
                                else
                                {
                                    res64 |= (tmp64 & 0x7f) << 7*6;
                                    if ((tmp64 = buff[p + 7]) < 128)
                                    {
                                        p += 8;
                                        res64 |= tmp64 << 7*7;
                                        const long mask = 1L << 7*8 - 1;
                                        if ((res64 & mask) != 0)
                                            res64 |= -mask;
                                    }
                                    else
                                    {
                                        res64 |= (tmp64 & 0x7f) << 7*7;
                                        if ((tmp64 = buff[p + 8]) < 128)
                                        {
                                            p += 9;
                                            res64 |= tmp64 << 7*8;
                                            const long mask = 1L << 7*9 - 1;
                                            if ((res64 & mask) != 0)
                                                res64 |= -mask;
                                        }
                                        else
                                        {
                                            res64 |= (tmp64 & 0x7f) << 7*8;
                                            if ((tmp64 = buff[p + 9]) > 127)
                                            {
                                                pos = p + 10;
                                                ThrowOverflow();
                                                return 0;
                                            }

                                            p += 10;
                                            res64 |= tmp64 << 7*9;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            pos = p;
            return res64;
        }

        internal static unsafe ulong ReadUnsignedValue(byte* buff, ref int pos)
        {
            int p = pos;
            int tmp32 = buff[p];
            if (tmp32 < 128)
            {
                pos++;
                return (ulong) tmp32;
            }

            int res32 = tmp32 & 0x7f;
            if ((tmp32 = buff[p + 1]) < 128)
            {
                p += 2;
                res32 |= tmp32 << 7*1;
            }
            else
            {
                res32 |= (tmp32 & 0x7f) << 7*1;
                if ((tmp32 = buff[p + 2]) < 128)
                {
                    p += 3;
                    res32 |= tmp32 << 7*2;
                }
                else
                {
                    res32 |= (tmp32 & 0x7f) << 7*2;
                    if ((tmp32 = buff[p + 3]) < 128)
                    {
                        p += 4;
                        res32 |= tmp32 << 7*3;
                    }
                    else
                    {
                        long tmp64;
                        long res64 = res32 | (tmp32 & 0x7f) << 7*3;
                        if ((tmp64 = buff[p + 4]) < 128)
                        {
                            p += 5;
                            res64 |= tmp64 << 7*4;
                        }
                        else
                        {
                            res64 |= (tmp64 & 0x7f) << 7*4;
                            if ((tmp64 = buff[p + 5]) < 128)
                            {
                                p += 6;
                                res64 |= tmp64 << 7*5;
                            }
                            else
                            {
                                res64 |= (tmp64 & 0x7f) << 7*5;
                                if ((tmp64 = buff[p + 6]) < 128)
                                {
                                    p += 7;
                                    res64 |= tmp64 << 7*6;
                                }
                                else
                                {
                                    res64 |= (tmp64 & 0x7f) << 7*6;
                                    if ((tmp64 = buff[p + 7]) < 128)
                                    {
                                        p += 8;
                                        res64 |= tmp64 << 7*7;
                                    }
                                    else
                                    {
                                        res64 |= (tmp64 & 0x7f) << 7*7;
                                        if ((tmp64 = buff[p + 8]) < 128)
                                        {
                                            p += 9;
                                            res64 |= tmp64 << 7*8;
                                        }
                                        else
                                        {
                                            res64 |= (tmp64 & 0x7f) << 7*8;
                                            if ((tmp64 = buff[p + 9]) > 127)
                                            {
                                                pos = p + 10;
                                                ThrowOverflow();
                                                return 0;
                                            }

                                            p += 10;
                                            res64 |= tmp64 << 7*9;
                                        }
                                    }
                                }
                            }
                        }

                        pos = p;
                        return (ulong) res64;
                    }
                }
            }

            pos = p;
            return (uint) res32;
        }

        private static void ThrowOverflow()
        {
            throw new SerializerException("64bit value read overflow");
        }
    }
}