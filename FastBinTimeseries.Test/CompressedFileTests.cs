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

                if (expected.Count > 0)
                {
//                    Assert.AreEqual(expected[0].a, f.FirstFileIndex);
                    Assert.AreEqual(expected[expected.Count - 1].a, f.LastFileIndex);
                }
                else
                {
                    Assert.IsNull(f.FirstFileIndex);
                    Assert.IsNull(f.LastFileIndex);
                }

                TestUtils.CollectionAssertEqual(expected, f.Stream(UtcDateTime.MinValue), name);

                expected.Reverse();

                TestUtils.CollectionAssertEqual(
                    expected, f.Stream(UtcDateTime.MaxValue, inReverse: true), "reverse " + name);
            }
        }
    }
}