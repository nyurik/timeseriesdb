using System;
using NUnit.Framework;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class BinTimeseriesFileTests : TestsBase
    {
        private void RunTest(int itemCount, bool uniqueTimestamps)
        {
            string fileName = GetBinFileName();
            using (var f = 
                AllowCreate
                ? new BinTimeseriesFile<_DatetimeByte_SeqPk1>(fileName) {UniqueTimestamps = uniqueTimestamps}
                : (BinTimeseriesFile<_DatetimeByte_SeqPk1>)BinaryFile.Open(fileName, false))
            {
                _DatetimeByte_SeqPk1[] newData = TestUtils.GenerateData<_DatetimeByte_SeqPk1>(_DatetimeByte_SeqPk1.New,
                                                                                              itemCount, 0);
                if (AllowCreate)
                {
                    f.InitializeNewFile();
                    f.AppendData(new ArraySegment<_DatetimeByte_SeqPk1>(newData));
                }

                _DatetimeByte_SeqPk1[] res = f.ReadData(UtcDateTime.MinValue, UtcDateTime.MaxValue, int.MaxValue);
                TestUtils.AreEqual(newData, res);

                if (itemCount > 0)
                {
                    res = f.ReadData(
                        _DatetimeByte_SeqPk1.New(itemCount - 1).a,
                        _DatetimeByte_SeqPk1.New(itemCount).a,
                        int.MaxValue);
                    TestUtils.AreEqual(
                        TestUtils.GenerateData<_DatetimeByte_SeqPk1>(
                            _DatetimeByte_SeqPk1.New, 1, itemCount - 1), res);
                }
            }
        }

        [Test, Ignore]
        public void BasicFunctionality()
        {
            _DatetimeByte_SeqPk1[] newData = TestUtils.GenerateData<_DatetimeByte_SeqPk1>(
                _DatetimeByte_SeqPk1.New, 10000, 0);

            string fileName = GetBinFileName();
            using (var f = 
                AllowCreate
                ? new BinTimeseriesFile<_DatetimeByte_SeqPk1>(fileName) {UniqueTimestamps = false}
                : (BinTimeseriesFile<_DatetimeByte_SeqPk1>)BinaryFile.Open(fileName, false))
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

        [Test]
        public void VariousLengthNonDuplTimeseries()
        {
            RunTest(0, true);
            RunTest(1, true);
            RunTest(10, true);
            RunTest(100, true);
            RunTest(1000, true);
            RunTest(10000, true);

            RunTest(0, false);
            RunTest(1, false);
            RunTest(10, false);
            RunTest(100, false);
            RunTest(1000, false);
            RunTest(10000, false);
        }
    }
}