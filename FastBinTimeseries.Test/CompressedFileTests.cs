using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test
{
    // ReSharper disable PossibleMultipleEnumeration

    [TestFixture]
    public class CompressedFileTests : TestsBase
    {
        private static BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1> NewBinCompressedSeriesFile(
            string fileName, bool uniqueTimestamps, int blockSizeExtra)
        {
            var bf = new BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>(fileName)
                         {UniqueIndexes = uniqueTimestamps};
            bf.BlockSize = bf.FieldSerializer.RootField.GetMaxByteSize() + CodecBase.ReservedSpace + blockSizeExtra;
            return bf;
        }

        [Test, Combinatorial]
        public void VariousLengthNonDuplTimeseries(
            [Values(0, 1, 2, 3, 4, 5, 10, 100, 10000)] int itemCount,
            [Values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 200, 10000)] int blockSizeExtra,
            [Values(true, false)] bool enableCache)
        {
            string name = string.Format(
                "itemCount={0}, blockSizeExtra={1}, cache={2}", itemCount, blockSizeExtra, enableCache);

            string fileName = GetBinFileName();
            using (BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1> f =
                AllowCreate
                    ? NewBinCompressedSeriesFile(fileName, true, blockSizeExtra)
                    : (BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>) BinaryFile.Open(fileName, false))
            {
                f.BinarySearchCacheSize = enableCache ? 0 : -1;

                IEnumerable<Buffer<_DatetimeByte_SeqPk1>> newData =
                    TestUtils.GenerateDataStream(_DatetimeByte_SeqPk1.New, itemCount, 0, itemCount);

                List<_DatetimeByte_SeqPk1> expected = newData.StreamSegmentValues().ToList();

                if (AllowCreate)
                {
                    f.InitializeNewFile();
                    f.AppendData(newData.Select(i => i.AsArraySegment));
                }

                _DatetimeByte_SeqPk1[] empty = _DatetimeByte_SeqPk1.Empty;

                TestUtils.CollectionAssertEqual(
                    empty, f.Stream(UtcDateTime.MinValue, inReverse: true),
                    "{0} - nothing before 0", name);

                TestUtils.CollectionAssertEqual(
                    empty, f.Stream(UtcDateTime.MaxValue), "{0} - nothing after max", name);

                if (expected.Count <= 0)
                {
                    Assert.IsNull(f.FirstFileIndex, "{0} null FirstInd", name);
                    Assert.IsNull(f.LastFileIndex, "{0} null LastInd", name);
                    TestUtils.CollectionAssertEqual(empty, f.Stream(UtcDateTime.MinValue), "{0} empty forward", name);
                    TestUtils.CollectionAssertEqual(
                        empty, f.Stream(UtcDateTime.MinValue, inReverse: true), "{0} empty backward", name);
                    return;
                }

                Assert.AreEqual(expected[0].a, f.FirstFileIndex, name + " first");
                Assert.AreEqual(expected[expected.Count - 1].a, f.LastFileIndex, "{0} last", name);


                _DatetimeByte_SeqPk1[] expectedRev = expected.ToArray();
                Array.Reverse(expectedRev);

                TestUtils.CollectionAssertEqual(expected, f.Stream(UtcDateTime.MinValue), "{0} full forward", name);
                TestUtils.CollectionAssertEqual(
                    expectedRev, f.Stream(UtcDateTime.MaxValue, inReverse: true), "{0} full backward", name);

                int maxSkip = Math.Min(10, expected.Count);
                for (int skip = 0; skip < maxSkip; skip++)
                {
                    for (int take = 0; take < 10; take++)
                    {
                        TestUtils.CollectionAssertEqual(
                            expected.Skip(skip).Take(take), f.Stream(expected[skip].a, maxItemCount: take),
                            "{0} skip {1} take {2}", name, skip, take);

                        TestUtils.CollectionAssertEqual(
                            expectedRev.Skip(skip).Take(take),
                            f.Stream(expectedRev[skip].a, maxItemCount: take, inReverse: true),
                            "{0} backward skip {1} take {2}", name, skip, take);
                    }
                }
            }
        }
    }
}