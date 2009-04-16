using System;
using System.IO;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class BinIndexedFileCoreTests : TestsBase
    {
        private const string TagString = "Test123";

        private static void AfterInitValidation(BinaryFile<byte> f, bool canWrite)
        {
            // assignment tests
            AssertInvalidOperationException(f, i => i.Tag = "a");
            AssertInvalidOperationException(f, i => i.Serializer = null);

            Assert.AreEqual(0, f.Count);
            Assert.AreEqual(new Version(1, 0), f.FileVersion);
            Assert.Greater(f.HeaderSize, 0);
            Assert.AreEqual(true, f.IsEmpty);
            Assert.AreEqual(1, f.ItemSize);
            Assert.AreEqual(canWrite, f.CanWrite);
            Assert.AreEqual(TagString, f.Tag);
            Assert.AreEqual(BinFileName, f.FileName);
        }

        private static void AssertInvalidOperationException<T>(BinaryFile<T> f, Func<BinaryFile<T>, object> operation)
        {
            try
            {
                object i = operation(f);
                Assert.Fail("Should have thrown an InvalidOperatioExcetpion, but {0} was returned instead", i);
            }
            catch (InvalidOperationException)
            {
            }
        }

        [Test]
        public void ArrayCompare()
        {
            const int bufSize = 1024*1024*1;
            var buf1 = new byte[bufSize];
            var buf2 = new byte[bufSize];

            var buf1all = new ArraySegment<byte>(buf1);
            var buf2all = new ArraySegment<byte>(buf2);

            TestUtils.AreEqual(buf1all, buf2all, "compare zeroes");

            for (int i = 0; i < bufSize; i++) buf2[i] = buf1[i] = (byte) (i & 0xFF);

            TestUtils.AreEqual(buf1all, buf2all, "compare byte 0,1,2,3,...,255,0,...");
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
                TestUtils.AreNotEqual(buf1all, buf2all);
                buf1[i]--;
            }
            TestUtils.AreEqual(buf1all, buf2all);
            for (int i = 0; i < 1000; i++)
            {
                buf1[bufSize - i - 1]++;
                TestUtils.AreNotEqual(buf1all, buf2all);
                buf1[bufSize - i - 1]--;
            }
            TestUtils.AreEqual(buf1all, buf2all);
        }

        [Test]
        public void BasicFunctionality()
        {
            BinIndexedFile<byte> temp;
            using (var f = new BinIndexedFile<byte>(BinFileName))
            {
                temp = f;
                AssertInvalidOperationException(f, i => i.Count);
                AssertInvalidOperationException(f, i => i.BaseVersion);
                AssertInvalidOperationException(f, i => i.FileVersion);
                AssertInvalidOperationException(f, i => i.SerializerVersion);
                AssertInvalidOperationException(f, i => i.HeaderSize);
                AssertInvalidOperationException(f, i => i.IsEmpty);
                AssertInvalidOperationException(f, i => i.ItemSize);

                Assert.IsTrue(f.CanWrite);
                Assert.IsFalse(f.IsInitialized);
                Assert.IsFalse(f.IsDisposed);
                Assert.IsFalse(f.IsOpen);
                Assert.AreEqual(BinFileName, f.FileName);
                Assert.AreEqual("", f.Tag);
                f.Tag = TagString;

                f.InitializeNewFile();

                Assert.IsTrue(f.IsInitialized);
                Assert.IsFalse(f.IsDisposed);
                Assert.IsTrue(f.IsOpen);
                Assert.AreEqual(BinFileName, f.FileName);

                AssertInvalidOperationException(
                    f, i =>
                           {
                               i.InitializeNewFile();
                               return null;
                           });

                AfterInitValidation(f, true);
            }

            // allowed
            temp.Close();
            ((IDisposable) temp).Dispose();
            AssertInvalidOperationException(temp, i => i.Tag);

            Assert.IsTrue(temp.IsInitialized);
            Assert.IsTrue(temp.IsDisposed);
            Assert.IsFalse(temp.IsOpen);
            Assert.AreEqual(BinFileName, temp.FileName);

            using (var file = (BinIndexedFile<byte>) BinaryFile.Open(BinFileName, true))
            {
                AfterInitValidation(file, true);
                file.Close();
                AssertInvalidOperationException(file, i => i.Tag);

                Assert.IsTrue(file.IsInitialized);
                Assert.IsTrue(file.IsDisposed);
                Assert.IsFalse(file.IsOpen);
                Assert.AreEqual(BinFileName, file.FileName);
            }

            using (var file = (BinIndexedFile<byte>) BinaryFile.Open(BinFileName, false))
            {
                AfterInitValidation(file, false);
                ((IDisposable) file).Dispose();
                AssertInvalidOperationException(file, i => i.Tag);
            }

            using (var f = new BinIndexedFile<byte>(BinFileName))
            {
                try
                {
                    f.InitializeNewFile();
                    Assert.Fail("existing file - must have failed");
                }
                catch (IOException)
                {
                }

                File.Delete(BinFileName);

                f.InitializeNewFile();
            }
        }
    }
}