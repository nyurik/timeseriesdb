using System;
using System.Runtime.InteropServices;
using NUnit.Framework;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class BinBlockSerializerTests : TestsBase
    {
        [Test]
        public void Test()
        {
            var data = new TradesBlock[1000];

            for (int i = 0; i < data.Length; i++)
                data[i] = new TradesBlock(i);

            var fileName = GetBinFileName();
            if (AllowCreate)
            {
                using (var f = new BinIndexedFile<TradesBlock>(fileName))
                {
                    f.InitializeNewFile();
                    f.WriteData(0, new ArraySegment<TradesBlock>(data));

                    VerifyData(f, data);
                }
            }
            
            using(var bf = (BinIndexedFile<TradesBlock>) BinaryFile.Open(fileName, false))
            {
                VerifyData(bf, data);
            }
        }

        private static void VerifyData(BinIndexedFile<TradesBlock> bf, TradesBlock[] data)
        {
            int ind = 0;
            foreach (var sg in bf.StreamSegments(0, false, 0))
            {
                int last = sg.Offset + sg.Count;
                for (int i = sg.Offset; i < last; i++)
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
    }

    [BinarySerializer(typeof (BinBlockSerializer<TradesBlock, Hdr, Item>))]
    [BinaryBlock(BlockItemCount)]
    public class TradesBlock : IBinBlock<TradesBlock.Hdr, TradesBlock.Item>
    {
        private const int BlockItemCount = 1000;
        private static UtcDateTime _firstTimeStamp = new UtcDateTime(2000, 1, 1);
        private readonly Item[] _items = new Item[BlockItemCount];

        public Hdr Header;

        public TradesBlock()
        {
        }

        public TradesBlock(long i)
        {
            unchecked
            {
                Header = new Hdr
                             {
                                 ItemCount = (ushort) (BlockItemCount - i%(BlockItemCount/10)),
                                 Timestamp = _firstTimeStamp.AddMinutes(i),
                                 Size = i,
                                 Value = i
                             };

                for (int j = 0; j < Header.ItemCount; j++)
                {
                    long tmp = i + j;
                    Items[j] =
                        new Item
                            {
                                ShiftInMilliseconds = (ushort) tmp,
                                ValueDiff = 1f/tmp,
                                SizeDiff = 1f/tmp
                            };
                }
            }
        }

        #region IBinBlock<Hdr,Item> Members

        Hdr IBinBlock<Hdr, Item>.Header
        {
            get { return Header; }
            set { Header = value; }
        }

        public Item[] Items
        {
            get { return _items; }
        }

        #endregion

        #region Nested type: Hdr

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct Hdr : IEquatable<Hdr>
        {
            public ushort ItemCount;
            public UtcDateTime Timestamp;
            public double Value;
            public double Size;

            #region Implementation

            public bool Equals(Hdr other)
            {
                return other.ItemCount == ItemCount && other.Timestamp.Equals(Timestamp) &&
                       other.Value.Equals(Value) &&  other.Size.Equals(Size);
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
                    int result = ItemCount.GetHashCode();
                    result = (result*397) ^ Timestamp.GetHashCode();
                    result = (result*397) ^ Value.GetHashCode();
                    result = (result*397) ^ Size.GetHashCode();
                    return result;
                }
            }

            public override string ToString()
            {
                return string.Format("{0}, {1}, {2}, {3}", ItemCount, Timestamp, Value, Size);
            }

            #endregion
        }

        #endregion

        #region Nested type: Item

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct Item : IEquatable<Item>
        {
            public ushort ShiftInMilliseconds;
            public float ValueDiff;
            public float SizeDiff;

            #region Implementation

            public bool Equals(Item other)
            {
                return other.ShiftInMilliseconds == ShiftInMilliseconds && other.ValueDiff.Equals(ValueDiff) &&
                       other.SizeDiff.Equals(SizeDiff);
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
                    int result = ShiftInMilliseconds.GetHashCode();
                    result = (result*397) ^ ValueDiff.GetHashCode();
                    result = (result*397) ^ SizeDiff.GetHashCode();
                    return result;
                }
            }

            public override string ToString()
            {
                return string.Format("{0}, {1}, {2}", ShiftInMilliseconds, ValueDiff, SizeDiff);
            }

            #endregion
        }

        #endregion
    }
}