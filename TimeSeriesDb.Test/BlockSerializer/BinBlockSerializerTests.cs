#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of TimeSeriesDb library
 * 
 *  TimeSeriesDb is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  TimeSeriesDb is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with TimeSeriesDb.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.Runtime.InteropServices;
using NUnit.Framework;
using NYurik.TimeSeriesDb.CommonCode;
using NYurik.TimeSeriesDb.Serializers;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

namespace NYurik.TimeSeriesDb.Test.BlockSerializer
{
    [TestFixture]
    public class BinBlockSerializerTests : TestsBase
    {
        private static void VerifyData(BinIndexedFile<TradesBlock> bf, TradesBlock[] data)
        {
            int ind = 0;
            foreach (var sg in bf.StreamSegments(0, false))
            {
                int last = sg.Count;
                for (int i = 0; i < last; i++)
                {
                    TradesBlock item = sg.Array[i];
                    if (!data[ind].Header.Equals(item.Header))
                        throw new Exception();
                    for (int j = 0; j < item.Items.Length; j++)
                    {
                        if (!data[ind].Items[j].Equals(item.Items[j]))
                            throw new Exception();
                    }

                    if (!data[ind].Header.Equals(item.Header))
                        throw new Exception();

                    ind++;
                }
            }
        }

        [Test]
        [Obsolete]
        public void Test()
        {
            const int blockSize = 100;
            var data = new TradesBlock[1000];

            for (int i = 0; i < data.Length; i++)
                data[i] = new TradesBlock(i, blockSize);

            string fileName = GetBinFileName();
            if (AllowCreate)
            {
                using (var f = new BinIndexedFile<TradesBlock>(fileName))
                {
                    ((IBinBlockSerializer) f.Serializer).ItemCount = blockSize;
                    f.InitializeNewFile();
                    f.WriteData(0, new ArraySegment<TradesBlock>(data));

                    VerifyData(f, data);
                }
            }

            using (var bf = (BinIndexedFile<TradesBlock>) BinaryFile.Open(fileName, false, LegacySupport.TypeResolver))
            {
                VerifyData(bf, data);
            }
        }
    }

    [BinarySerializer(typeof (BinBlockSerializer<TradesBlock, Hdr, Item>))]
    public class TradesBlock : IBinBlock<TradesBlock.Hdr, TradesBlock.Item>
    {
        private static UtcDateTime _firstTimeStamp = new UtcDateTime(2000, 1, 1);

        public Hdr Header;

        public TradesBlock()
        {
        }

        public TradesBlock(long i, int itemCount)
        {
            unchecked
            {
                Items = new Item[itemCount];
                Header = new Hdr((ushort) (itemCount - i%(itemCount/10)), _firstTimeStamp.AddMinutes(i));

                for (int j = 0; j < Header.ItemCount; j++)
                {
                    long tmp = i + j;
                    Items[j] = new Item((ushort) tmp, 1d/tmp, (int) tmp);
                }
            }
        }

        #region IBinBlock<Hdr,Item> Members

        Hdr IBinBlock<Hdr, Item>.Header
        {
            get { return Header; }
            set { Header = value; }
        }

        public Item[] Items { get; set; }

        #endregion

        #region Nested type: Hdr

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct Hdr : IEquatable<Hdr>
        {
            public readonly ushort ItemCount;
            public readonly UtcDateTime Timestamp;

            public Hdr(ushort itemCount, UtcDateTime timestamp)
            {
                ItemCount = itemCount;
                Timestamp = timestamp;
            }

            #region Implementation

            public bool Equals(Hdr other)
            {
                return other.ItemCount == ItemCount && other.Timestamp.Equals(Timestamp);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (obj.GetType() != typeof (Hdr)) return false;
                return Equals((Hdr) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (ItemCount.GetHashCode()*397) ^ Timestamp.GetHashCode();
                }
            }

            public override string ToString()
            {
                return string.Format("{0}, {1}", ItemCount, Timestamp);
            }

            #endregion
        }

        #endregion

        #region Nested type: Item

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct Item : IEquatable<Item>
        {
            public readonly double Value;
            public readonly int Size;
            public readonly ushort ShiftInMilliseconds;

            public Item(ushort shiftInMilliseconds, double value, int size)
            {
                Value = value;
                Size = size;
                ShiftInMilliseconds = shiftInMilliseconds;
            }

            #region Implementation

            public bool Equals(Item other)
            {
                return other.Value.Equals(Value) && other.Size == Size &&
                       other.ShiftInMilliseconds == ShiftInMilliseconds;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (obj.GetType() != typeof (Item)) return false;
                return Equals((Item) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    int result = Value.GetHashCode();
                    result = (result*397) ^ Size;
                    result = (result*397) ^ ShiftInMilliseconds.GetHashCode();
                    return result;
                }
            }

            public override string ToString()
            {
                return string.Format("{0}, {1}, {2}", ShiftInMilliseconds, Value, Size);
            }

            #endregion
        }

        #endregion
    }
}