using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Test
{
    // ReSharper disable PossibleMultipleEnumeration

    [TestFixture]
    public class CompressedFileTests : TestsBase
    {
        private void RunTest(int itemCount, int repeatRuns, bool uniqueTimestamps, bool enableCache)
        {
            string fileName = GetBinFileName();
            using (BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1> f =
                AllowCreate
                    ? new BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>(fileName)
                          {UniqueIndexes = uniqueTimestamps}
                    : (BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>) BinaryFile.Open(fileName, false))
            {
                f.BinarySearchCacheSize = enableCache ? 0 : -1;

                IEnumerable<Buffer<_DatetimeByte_SeqPk1>> newData =
                    TestUtils.GenerateDataStream(_DatetimeByte_SeqPk1.New, itemCount, 0, 1);
                List<_DatetimeByte_SeqPk1> expected = newData.StreamSegmentValues().ToList();

                if (AllowCreate)
                {
                    f.InitializeNewFile();
                    f.AppendData(newData.Select(i => i.AsArraySegment));
                }

                IEnumerable<Buffer<_DatetimeByte_SeqPk1>> res = f.StreamSegments(UtcDateTime.MinValue);
                TestUtils.CollectionAssertEqual(expected, res.StreamSegmentValues());

                expected.Reverse();

                TestUtils.CollectionAssertEqual(expected, f.Stream(UtcDateTime.MaxValue, inReverse: true));
            }
        }

        [Test, Combinatorial]
        public void VariousLengthNonDuplTimeseries([Values(true, false)] bool uniqueTimestamps,
                                                   [Values(true, false)] bool enableCache)
        {
            const int repeatRuns = 10;
            RunTest(0, repeatRuns, uniqueTimestamps, enableCache);
            RunTest(1, repeatRuns, uniqueTimestamps, enableCache);
            RunTest(10, repeatRuns, uniqueTimestamps, enableCache);
            RunTest(100, repeatRuns, uniqueTimestamps, enableCache);
            RunTest(1000, repeatRuns, uniqueTimestamps, enableCache);
            RunTest(10000, repeatRuns, uniqueTimestamps, enableCache);
        }
    }
}