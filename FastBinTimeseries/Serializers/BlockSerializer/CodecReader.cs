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
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class CodecReader : CodecBase
    {
        private byte[] _buffer;
        private int _bufferPos;

        /// <summary> Create codec for reading only </summary>
        public CodecReader(ArraySegment<byte> buffer)
        {
            AttachBuffer(buffer);
        }

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

        [UsedImplicitly]
        internal int ReadHeader()
        {
            return ValidateCount(ReadUnsignedValue());
        }

        public void AttachBuffer(ArraySegment<byte> value)
        {
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
            return _buffer[_bufferPos++];
        }

        [UsedImplicitly]
        internal ulong ReadUnsignedValue()
        {
            int p = _bufferPos;
            byte[] buff = _buffer;

            int tmp32 = buff[p];
            if (tmp32 < 128)
            {
                _bufferPos++;
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

                        _bufferPos = p;
                        return (ulong) res64;
                    }
                }
            }

            _bufferPos = p;
            return (uint) res32;
        }

        [UsedImplicitly]
        internal long ReadSignedValue()
        {
            int p = _bufferPos;
            byte[] buff = _buffer;

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

        public void Validate(int blockSize)
        {
            int pos = FastBinFileUtils.RoundDownToMultiple(_bufferPos, blockSize);
            if (pos == _bufferPos)
                throw new SerializerException("Cannot validate when BlockPos={0}, blockSize={1}", _bufferPos, blockSize);
            int dataSize = _bufferPos - pos;
            byte[] hash = HashAlgorithm.ComputeHash(_buffer, pos, dataSize);
            int hashSize = hash.Length;
            if (hashSize >= blockSize - dataSize)
                hashSize = blockSize - dataSize;
            for (int i = 0; i < hashSize; i++)
                if (hash[i] != _buffer[_bufferPos + i])
                    throw new SerializerException("Block validation failed, data might be corrupted");
        }
    }
}