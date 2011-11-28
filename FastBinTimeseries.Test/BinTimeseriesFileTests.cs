using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class BinTimeseriesFileTests : TestsBase
    {
        private void RunTest(int itemCount, int repeatRuns, bool uniqueTimestamps, bool enableCache)
        {
            string fileName = GetBinFileName();
            using (BinTimeseriesFile<_DatetimeByte_SeqPk1> f =
                AllowCreate
                    ? new BinTimeseriesFile<_DatetimeByte_SeqPk1>(fileName) {UniqueTimestamps = uniqueTimestamps}
                    : (BinTimeseriesFile<_DatetimeByte_SeqPk1>) BinaryFile.Open(fileName, false))
            {
                f.BinarySearchCacheSize = enableCache ? 0 : -1;

                _DatetimeByte_SeqPk1[] newData =
                    TestUtils.GenerateData(_DatetimeByte_SeqPk1.New, itemCount, 0);

                if (AllowCreate)
                {
                    f.InitializeNewFile();
                    f.AppendData(new ArraySegment<_DatetimeByte_SeqPk1>(newData));
                }

                _DatetimeByte_SeqPk1[] res = f.ReadData(UtcDateTime.MinValue, UtcDateTime.MaxValue, int.MaxValue);
                TestUtils.AreEqual(newData, res);

                CollectionAssert.AreEqual(newData, f.Stream(UtcDateTime.MinValue));

                Array.Reverse(newData);
                CollectionAssert.AreEqual(newData, f.Stream(UtcDateTime.MaxValue, inReverse: true));

                if (itemCount > 0)
                {
                    for (int i = 0; i < Math.Min(repeatRuns, itemCount); i++)
                    {
                        UtcDateTime from = _DatetimeByte_SeqPk1.New(itemCount - i).a;
                        UtcDateTime until = _DatetimeByte_SeqPk1.New(itemCount).a;

                        res = f.ReadData(from, until, int.MaxValue);

                        _DatetimeByte_SeqPk1[] expected = TestUtils.GenerateData(
                            _DatetimeByte_SeqPk1.New, i, itemCount - i);
                        TestUtils.AreEqual(expected, res);

                        List<_DatetimeByte_SeqPk1> res1 = f.Stream(@from, until.AddTicks(1)).ToList();
                        CollectionAssert.AreEqual(expected, res1);

                        Array.Reverse(expected);

                        List<_DatetimeByte_SeqPk1> res2 = f.Stream(until, @from, inReverse: true).ToList();
                        CollectionAssert.AreEqual(expected, res2);
                    }
                }
            }
        }

        [Test, Ignore]
        public void BasicFunctionality()
        {
            _DatetimeByte_SeqPk1[] newData = TestUtils.GenerateData(
                _DatetimeByte_SeqPk1.New, 10000, 0);

            string fileName = GetBinFileName();
            using (BinSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1> f =
                AllowCreate
                    ? new BinSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>(fileName) {UniqueIndexes = false}
                    : (BinSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>) BinaryFile.Open(fileName, false))
            {
                if (AllowCreate)
                {
                    f.InitializeNewFile();
                    f.AppendData(new ArraySegment<_DatetimeByte_SeqPk1>(newData));
                }

                _DatetimeByte_SeqPk1[] res = f.ReadData(UtcDateTime.MinValue, UtcDateTime.MaxValue, int.MaxValue);
                TestUtils.AreEqual(newData, res);

                //                if (itemCount > 0)
                //                {
                //                    res = f.ReadData(
                //                        _DatetimeByte_SeqPk1.New(itemCount - 1).a,
                //                        _DatetimeByte_SeqPk1.New(itemCount).a,
                //                        int.MaxValue);
                //                    TestUtils.AreEqual(
                //                        TestUtils.GenerateData<_DatetimeByte_SeqPk1>(
                //                            _DatetimeByte_SeqPk1.New, 1, itemCount - 1), res);
                //                }
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