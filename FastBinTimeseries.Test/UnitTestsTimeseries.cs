using System;
using System.IO;
using System.Reflection;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class UnitTestsTimeseries
    {
        #region Setup/Teardown

        [SetUp]
        public void Cleanup()
        {
            if (File.Exists(binFile))
                File.Delete(binFile);
        }

        #endregion

        private static readonly string binFile = MethodBase.GetCurrentMethod().DeclaringType + ".bsd";

        private void RunTest(int itemCount)
        {
            Cleanup();
            using (var f = new BinTimeseriesFile<_DatetimeByte_SeqPk1>(binFile))
            {
                f.InitializeNewFile();

                var newData = TestUtils.GenerateData<_DatetimeByte_SeqPk1>(_DatetimeByte_SeqPk1.New,
                                                                        itemCount, 0);
                f.AppendData(newData, 0, newData.Length);

                var res = f.ReadData(DateTime.MinValue, DateTime.MaxValue, int.MaxValue);
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

        [Test]
        public void BasicTimeseries()
        {
            RunTest(0);
            RunTest(1);
            RunTest(10);
            RunTest(100);
            RunTest(1000);
            RunTest(10000);
        }
    }
}