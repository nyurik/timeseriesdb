using System;
using System.Diagnostics.Contracts;
using System.IO;
using System.Reflection;
using System.Text;
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

                if (itemCount > 0)
                {
                    for (int i = 0; i < repeatRuns; i++)
                    {
                        res = f.ReadData(
                            _DatetimeByte_SeqPk1.New(itemCount - 1).a,
                            _DatetimeByte_SeqPk1.New(itemCount).a,
                            int.MaxValue);

                        TestUtils.AreEqual(
                            TestUtils.GenerateData(
                                _DatetimeByte_SeqPk1.New, 1, itemCount - 1), res);
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
            using (BinTimeseriesFile<_DatetimeByte_SeqPk1> f =
                AllowCreate
                    ? new BinTimeseriesFile<_DatetimeByte_SeqPk1>(fileName) {UniqueTimestamps = false}
                    : (BinTimeseriesFile<_DatetimeByte_SeqPk1>) BinaryFile.Open(fileName, false))
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
            const int repeatRuns = 5;
            RunTest(0, repeatRuns, uniqueTimestamps, enableCache);
            RunTest(1, repeatRuns, uniqueTimestamps, enableCache);
            RunTest(10, repeatRuns, uniqueTimestamps, enableCache);
            RunTest(100, repeatRuns, uniqueTimestamps, enableCache);
            RunTest(1000, repeatRuns, uniqueTimestamps, enableCache);
            RunTest(10000, repeatRuns, uniqueTimestamps, enableCache);
        }
    }
}