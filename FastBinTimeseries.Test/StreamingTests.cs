using System;
using System.IO;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class StreamingTests : TestsBase
    {
        private static byte NewByte(long i)
        {
            return (byte) (i%byte.MaxValue);
        }

        [Test]
        public void StreamingTest()
        {
            string fileName = GetBinFileName();

            byte[] data = TestUtils.GenerateData(NewByte, 10000, 0);
            if (AllowCreate)
            {
                using (var b = new BinIndexedFile<byte>(fileName))
                {
                    b.InitializeNewFile();
                    b.WriteData(0, new ArraySegment<byte>(data));
                }
            }

            byte[] bytes = File.ReadAllBytes(fileName);

            using (var b = (BinIndexedFile<byte>) BinaryFile.Open(fileName, false))
            {
                var ms = new MemoryStream(bytes);
                var cs = new ConfigurableStream(ms);
                var data2 = new byte[data.Length/2];

                cs.AllowSeek = cs.AllowWrite = false;
                var b2 = (BinIndexedFile<byte>) BinaryFile.Open(cs, null);
                Assert.IsTrue(b2.IsOpen);
                Assert.AreEqual(b.ItemSize, b2.ItemSize);

                b2.ReadData(0, new ArraySegment<byte>(data2));
                CollectionAssert.AreEqual(TestUtils.GenerateData(NewByte, data.Length/2, 0), data2);

                b2.ReadData(0, new ArraySegment<byte>(data2));
                CollectionAssert.AreEqual(TestUtils.GenerateData(NewByte, data.Length/2, data.Length/2), data2);
            }
        }
    }
}