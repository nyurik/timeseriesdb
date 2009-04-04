using System;
using System.IO;
using System.Reflection;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class CoreTests
    {
        #region Setup/Teardown

        [SetUp]
        public void Cleanup()
        {
            // perform the init to count accurate performance
            new PackedDateTime();
            if (File.Exists(binFile))
                File.Delete(binFile);
        }

        #endregion

        private static readonly string binFile = MethodBase.GetCurrentMethod().DeclaringType + ".bsd";
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
        public void BasicFunctionality()
        {
            BinIndexedFile<byte> temp = null;
            using (var f = new BinIndexedFile<byte>(binFile))
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
                Assert.AreEqual("", f.Tag);
                f.Tag = TagString;

                f.InitializeNewFile();

                Assert.IsTrue(f.IsInitialized);
                Assert.IsFalse(f.IsDisposed);
                Assert.IsTrue(f.IsOpen);

                AssertInvalidOperationException(f, i =>
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

            using (var file = (BinIndexedFile<byte>) BinaryFile.Open(binFile, true))
            {
                AfterInitValidation(file, true);
                file.Close();
                AssertInvalidOperationException(file, i => i.Tag);

                Assert.IsTrue(file.IsInitialized);
                Assert.IsTrue(file.IsDisposed);
                Assert.IsFalse(file.IsOpen);
            }

            using (var file = (BinIndexedFile<byte>) BinaryFile.Open(binFile, false))
            {
                AfterInitValidation(file, false);
                ((IDisposable) file).Dispose();
                AssertInvalidOperationException(file, i => i.Tag);
            }

            using (var f = new BinIndexedFile<byte>(binFile))
            {
                try
                {
                    f.InitializeNewFile();
                    Assert.Fail("existing file - must have failed");
                }
                catch (IOException)
                {
                }

                File.Delete(binFile);

                f.InitializeNewFile();
            }
        }
    }
}