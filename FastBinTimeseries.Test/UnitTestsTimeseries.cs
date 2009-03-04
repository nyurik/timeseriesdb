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
            using (var f = new BinTimeseriesFile<StructTimeValue>(binFile))
            {
                f.InitializeNewFile();

                var newData = UnitTestsUtils.GenerateData<StructTimeValue>(UnitTestsUtils.CreateStructTimeValue,
                                                                           itemCount, 0);
                f.AppendData(newData, 0, newData.Length);

                var res = f.ReadData(DateTime.MinValue, DateTime.MaxValue, int.MaxValue);
                UnitTestsUtils.AreEqual(newData, res);

                if (itemCount > 0)
                {
                    res = f.ReadData(
                        UnitTestsUtils.CreateStructTimeValue(itemCount - 1).timestamp,
                        UnitTestsUtils.CreateStructTimeValue(itemCount).timestamp,
                        int.MaxValue);
                    UnitTestsUtils.AreEqual(
                        UnitTestsUtils.GenerateData<StructTimeValue>(
                            UnitTestsUtils.CreateStructTimeValue, 1, itemCount - 1), res);
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