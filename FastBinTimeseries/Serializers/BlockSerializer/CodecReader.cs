using System;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class CodecReader : CodecBase
    {
        private byte[] _buffer;
        private int _bufferPos;

        public int BufferPos
        {
            get { return _bufferPos; }
            set
            {
                if (value < 0 || value > _buffer.Length)
                    throw new ArgumentOutOfRangeException("value", value, "Must be >= 0 && <= " + _buffer.Length);
                _bufferPos = value;
            }
        }

        /// <summary> Create codec for reading only </summary>
        public CodecReader(ArraySegment<byte> buffer)
        {
            AttachBuffer(buffer);
        }

        public byte[] Buffer
        {
            get { return _buffer; }
        }

        internal int ReadHeader()
        {
            int count = BitConverter.ToInt32(Buffer, 0);
            _bufferPos += HeaderSize;
            return count;
        }

        public void AttachBuffer(ArraySegment<byte> value)
        {
            if (value.Array == null)
                throw new ArgumentNullException("value");
            _buffer = value.Array;
            _bufferPos = value.Offset;
        }

        [UsedImplicitly]
        internal static unsafe ulong ReadUnsignedValueUnsafe(byte* buff, ref int pos)
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

        [UsedImplicitly]
        internal static unsafe long ReadSignedValueUnsafe(byte* buff, ref int pos)
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

        [UsedImplicitly]
        internal byte ReadByte()
        {
            return Buffer[_bufferPos++];
        }

        [UsedImplicitly]
        internal long ReadSignedValue()
        {
            int p = _bufferPos;
            byte[] buff = Buffer;

            long tmp64 = buff[p];
            if (tmp64 < 128)
            {
                const long mask = 1 << 7*1 - 1;
                if ((tmp64 & mask) != 0)
                    tmp64 |= -mask;
                _bufferPos++;
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
                                                _bufferPos = p + 10;
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

            _bufferPos = p;
            return res64;
        }

        private static void ThrowOverflow()
        {
            throw new SerializerException("64bit value read overflow");
        }
    }
}