using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test.BlockSerializer
{
    [TestFixture]
    public class DeltaSerializerTests : TestsBase
    {
        private void TestStub<T>(T[] buff, int count)
        {
            for (int i = 0; i < 10000; i++)
            {
                for (int j = 0; j < buff.Length; j++)
                {
                }
            }
        }

        [Test]
        public void Test()
        {
//            const int blockSize = 100;
//            var data = new DeltaBlock[1000];
//
//            for (int i = 0; i < data.Length; i++)
//                data[i] = new DeltaBlock(blockSize);
//
//            string fileName = GetBinFileName();
//            if (AllowCreate)
//            {
//                using (var f = new BinCompressedSeriesFile2<,>(fileName))
//                {
//                    f.InitializeNewFile();
//                    f.ProcessWriteStream(0, new ArraySegment<TradesBlock>(data));
//
//                    VerifyData(f, data);
//                }
//            }
//
//            using (var bf = (BinIndexedFile<TradesBlock>) BinaryFile.Open(fileName, false))
//            {
//                VerifyData(bf, data);
//            }
        }

        [Test]
        public void Test1()
        {
        }
    }
}