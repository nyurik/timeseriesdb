using System;
using System.IO;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class BinIndexedFileCoreTests : TestsBase
    {
        private const string TagString = "Test123";

        private static void AfterInitValidation(BinaryFile<byte> f, bool canWrite, string fileName)
        {
            // assignment tests
            f.AssertException<byte, InvalidOperationException>(i => i.Tag = "a");
            f.AssertException<byte, InvalidOperationException>(i => i.Serializer = null);

            Assert.AreEqual(0, f.Count);
            Assert.AreEqual(new Version(1, 0), f.FileVersion);
            Assert.Greater(f.HeaderSize, 0);
            Assert.AreEqual(true, f.IsEmpty);
            Assert.AreEqual(1, f.ItemSize);
            Assert.AreEqual(canWrite, f.CanWrite);
            Assert.AreEqual(TagString, f.Tag);
            Assert.AreEqual(fileName, f.FileName);
        }

        [Test]
        public void ArrayCompare()
        {
            const int bufSize = 1024*1024*1;
            var buf1 = new byte[bufSize];
            var buf2 = new byte[bufSize];

            var buf1All = new ArraySegment<byte>(buf1);
            var buf2All = new ArraySegment<byte>(buf2);

            TestUtils.AreEqual(buf1All, buf2All, "compare zeroes");

            for (int i = 0; i < bufSize; i++) buf2[i] = buf1[i] = (byte) (i & 0xFF);

            TestUtils.AreEqual(buf1All, buf2All, "compare byte 0,1,2,3,...,255,0,...");
            TestUtils.AreNotEqual(new ArraySegment<byte>(buf1, 255, bufSize - 255),
                                  new ArraySegment<byte>(buf2, 0, bufSize - 255));
            TestUtils.AreEqual(new ArraySegment<byte>(buf1, 256, bufSize - 256),
                               new ArraySegment<byte>(buf2, 0, bufSize - 256));
            TestUtils.AreNotEqual(new ArraySegment<byte>(buf1, 257, bufSize - 257),
                                  new ArraySegment<byte>(buf2, 0, bufSize - 257));
            TestUtils.AreEqual(new ArraySegment<byte>(buf1, 255, bufSize - 511),
                               new ArraySegment<byte>(buf2, 511, bufSize - 511));
            TestUtils.AreEqual(new ArraySegment<byte>(buf1, 257, bufSize - 257),
                               new ArraySegment<byte>(buf2, 1, bufSize - 257));

            for (int i = 0; i < 1000; i++)
            {
                buf1[i]++;
                TestUtils.AreNotEqual(buf1All, buf2All);
                buf1[i]--;
            }
            TestUtils.AreEqual(buf1All, buf2All);
            for (int i = 0; i < 1000; i++)
            {
                buf1[bufSize - i - 1]++;
                TestUtils.AreNotEqual(buf1All, buf2All);
                buf1[bufSize - i - 1]--;
            }
            TestUtils.AreEqual(buf1All, buf2All);
        }

        [Test]
        public void BasicFunctionality()
        {
            string fileName = GetBinFileName();
            if (AllowCreate)
            {
                BinIndexedFile<byte> temp;
                using (var f = new BinIndexedFile<byte>(fileName))
                {
                    temp = f;
                    f.AssertException<byte, InvalidOperationException>(i => i.Count);
                    f.AssertException<byte, InvalidOperationException>(i => i.FileVersion);
                    f.AssertException<byte, InvalidOperationException>(i => i.HeaderSize);
                    f.AssertException<byte, InvalidOperationException>(i => i.IsEmpty);
                    f.AssertException<byte, InvalidOperationException>(i => i.ItemSize);

                    Assert.IsTrue(f.CanWrite);
                    Assert.IsFalse(f.IsInitialized);
                    Assert.IsFalse(f.IsDisposed);
                    Assert.IsFalse(f.IsOpen);
                    Assert.AreEqual(fileName, f.FileName);
                    Assert.AreEqual("", f.Tag);
                    Assert.AreEqual(typeof (byte), f.ItemType);
                    Assert.IsNotNull(f.Serializer);
                    Assert.IsNotNull(f.Serializer.Version);
                    Version curBaseVer = f.BaseVersion;
                    f.BaseVersion = new Version(1, 0);
                    f.BaseVersion = new Version(1, 1);
                    f.BaseVersion = new Version(1, 2);
                    f.AssertException<byte, ArgumentNullException>(i => f.BaseVersion = null);
                    f.AssertException<byte, ArgumentOutOfRangeException>(i => f.BaseVersion = new Version(0, 0));
                    f.BaseVersion = curBaseVer;
                    f.Tag = TagString;

                    f.InitializeNewFile();

                    Assert.IsTrue(f.IsInitialized);
                    Assert.IsFalse(f.IsDisposed);
                    Assert.IsTrue(f.IsOpen);
                    Assert.AreEqual(fileName, f.FileName);

                    f.AssertException<byte, InvalidOperationException>(i => i.InitializeNewFile());
                    f.AssertException<byte, InvalidOperationException>(i => { f.BaseVersion = new Version(1, 1); });

                    AfterInitValidation(f, true, fileName);
                }


                // allowed
                temp.Close();
                ((IDisposable) temp).Dispose();
                temp.AssertException<byte, InvalidOperationException>(i => i.Tag);

                Assert.IsTrue(temp.IsInitialized);
                Assert.IsTrue(temp.IsDisposed);
                Assert.IsFalse(temp.IsOpen);
                Assert.AreEqual(fileName, temp.FileName);


                using (var f = (BinIndexedFile<byte>) BinaryFile.Open(fileName, AllowCreate))
                {
                    AfterInitValidation(f, true, fileName);
                    f.Close();
                    f.AssertException<byte, InvalidOperationException>(i => i.Tag);

                    Assert.IsTrue(f.IsInitialized);
                    Assert.IsTrue(f.IsDisposed);
                    Assert.IsFalse(f.IsOpen);
                    Assert.AreEqual(fileName, f.FileName);
                }
            }

            using (var f = (BinIndexedFile<byte>) BinaryFile.Open(fileName, false))
            {
                AfterInitValidation(f, false, fileName);
                ((IDisposable) f).Dispose();
                f.AssertException<byte, InvalidOperationException>(i => i.Tag);
            }

            using (var f = new BinIndexedFile<byte>(fileName))
            {
                f.AssertException<byte, IOException>(i => i.InitializeNewFile());

                if (RunMode == Mode.OneTime)
                {
                    File.Delete(fileName);
                    f.InitializeNewFile();
                }
            }
        }
    }
}