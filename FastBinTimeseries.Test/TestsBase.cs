using System.IO;
using System.Reflection;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    public class TestsBase
    {
        protected static readonly string BinFileName = MethodBase.GetCurrentMethod().DeclaringType + ".bsd";

        [SetUp,TearDown]
        public void Cleanup()
        {
            if (File.Exists(BinFileName))
                File.Delete(BinFileName);
        }
    }
}