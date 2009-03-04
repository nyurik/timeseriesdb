using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class UnitTestsSimple
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

        private static void WriteData<T>(BinIndexedFile<T> f, long firstItemIndex, T[] buffer)
        {
            f.WriteData(firstItemIndex, buffer, 0, buffer.Length);
        }

        private static void ReadAndAssert<T>(T[] expected, BinIndexedFile<T> f, int firstItemIndex, long count)
            where T : IEquatable<T>
        {
            var buffer = new T[count];
            f.ReadData(firstItemIndex, buffer, 0, buffer.Length);
            UnitTestsUtils.AreEqual(expected, buffer);
        }

        private static void EmptyFile<T>(int expectedItemSize)
        {
            int hdrSize;
            Version fileVersion, baseVersion, serializerVersion;

            using (var f = new BinIndexedFile<T>(binFile))
            {
                f.InitializeNewFile();
                fileVersion = f.FileVersion;
                Assert.IsNotNull(fileVersion);
                baseVersion = f.BaseVersion;
                Assert.IsNotNull(baseVersion);
                serializerVersion = f.SerializerVersion;
                Assert.IsNotNull(serializerVersion);

                Assert.AreEqual(true, f.CanWrite);
                Assert.AreEqual(0, f.Count);

                hdrSize = f.HeaderSize;

                Assert.AreEqual(expectedItemSize, f.ItemSize);
                Assert.AreEqual(hdrSize, f.HeaderSizeAsItemCount*f.ItemSize);
                Assert.IsTrue(f.IsEmpty);
            }

            using (var file = BinaryFile.Open(binFile, false))
            {
                Assert.IsInstanceOfType(typeof (BinIndexedFile<T>), file);
                var f = (BinIndexedFile<T>) file;

                Assert.AreEqual(fileVersion, f.FileVersion);
                Assert.AreEqual(baseVersion, f.BaseVersion);
                Assert.AreEqual(serializerVersion, f.SerializerVersion);

                Assert.AreEqual(false, f.CanWrite);
                Assert.AreEqual(0, f.Count);

                Assert.AreEqual(hdrSize, f.HeaderSize);

                Assert.AreEqual(hdrSize, f.HeaderSizeAsItemCount*f.ItemSize);
                Assert.IsTrue(f.IsEmpty);
                Assert.AreEqual(expectedItemSize, f.ItemSize);
            }
        }

        private static void FileIncrementalAddition<T>(Func<long, T> converter) where T : IEquatable<T>
        {
            var data0 = UnitTestsUtils.GenerateData(converter, 1, 10);
            var data1 = UnitTestsUtils.GenerateData(converter, 2, 20);
            var data2 = UnitTestsUtils.GenerateData(converter, 3, 30);

            using (var f = new BinIndexedFile<T>(binFile))
            {
                f.InitializeNewFile();
                f.WriteData(0, data0, 0, data0.Length);

                Assert.AreEqual(1, f.Count);
                Assert.IsFalse(f.IsEmpty);

                ReadAndAssert(data0, f, 0, f.Count);
            }

            using (var file = BinaryFile.Open(binFile, true))
            {
                Assert.IsInstanceOfType(typeof (BinIndexedFile<T>), file);
                var f = (BinIndexedFile<T>) file;
                Assert.AreEqual(1, f.Count);
                ReadAndAssert(data0, f, 0, f.Count);

                // Replace with buff2 starting at 0
                WriteData(f, 0, data1);
                Assert.AreEqual(2, f.Count);
                ReadAndAssert(data1, f, 0, f.Count);

                // Append buff1
                WriteData(f, f.Count, data0);
                Assert.AreEqual(3, f.Count);
                ReadAndAssert(data0, f, 2, 1);

                // Write buff3 instead of buff1
                WriteData(f, data1.Length, data2);
                Assert.AreEqual(data1.Length + data2.Length, f.Count);
                ReadAndAssert(UnitTestsUtils.Concatenate(data1, data2), f, 0, f.Count);
            }
        }

        private void PageBorderOperations<T>(Func<long, T> converter, bool enableMemoryMappedAccess)
            where T : IEquatable<T>
        {
            for (var i = 1; i < 5; i++)
                PageBorderOperations(converter, enableMemoryMappedAccess, BinaryFile.MinPageSize*i);

            PageBorderOperations(converter, enableMemoryMappedAccess,
                                 BinaryFile.MinLargePageSize - BinaryFile.MinPageSize);
            PageBorderOperations(converter, enableMemoryMappedAccess, BinaryFile.MinLargePageSize);
            PageBorderOperations(converter, enableMemoryMappedAccess,
                                 BinaryFile.MinLargePageSize + BinaryFile.MinPageSize);

            PageBorderOperations(converter, enableMemoryMappedAccess,
                                 2*BinaryFile.MinLargePageSize - BinaryFile.MinPageSize);
            PageBorderOperations(converter, enableMemoryMappedAccess, 2*BinaryFile.MinLargePageSize);
            PageBorderOperations(converter, enableMemoryMappedAccess,
                                 2*BinaryFile.MinLargePageSize + BinaryFile.MinPageSize);
        }

        private void PageBorderOperations<T>(Func<long, T> converter, bool enableMemoryMappedAccess, int pageSize)
            where T : IEquatable<T>
        {
            Cleanup();

            using (var f = new BinIndexedFile<T>(binFile))
            {
                f.InitializeNewFile();
                f.EnableMemoryMappedFileAccess = enableMemoryMappedAccess;

                var itemsPerPage = pageSize/Marshal.SizeOf(typeof (T));
                var items1stPg = (int) UnitTestsUtils.RoundUpToMultiple(f.HeaderSizeAsItemCount, itemsPerPage) -
                                 f.HeaderSizeAsItemCount;

                if (items1stPg == 0)
                    items1stPg = itemsPerPage;

                var dataMinusOne = UnitTestsUtils.GenerateData(converter, items1stPg - 1, 0);
                var dataZero = UnitTestsUtils.GenerateData(converter, items1stPg, 0);
                var dataPlusOne = UnitTestsUtils.GenerateData(converter, items1stPg + 1, 0);

                f.WriteData(0, dataMinusOne, 0, dataMinusOne.Length);
                Assert.AreEqual(f.HeaderSize + (items1stPg - 1)*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataMinusOne, f, 0, dataMinusOne.Length);

                f.WriteData(0, dataZero, 0, dataZero.Length);
                Assert.AreEqual(f.HeaderSize + items1stPg*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataZero, f, 0, dataZero.Length);

                f.WriteData(0, dataPlusOne, 0, dataPlusOne.Length);
                Assert.AreEqual(f.HeaderSize + (items1stPg + 1)*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataPlusOne, f, 0, dataPlusOne.Length);

                ReadAndAssert(UnitTestsUtils.GenerateData(converter, 1, items1stPg - 1), f, items1stPg - 1, 1);
                ReadAndAssert(UnitTestsUtils.GenerateData(converter, 1, items1stPg), f, items1stPg, 1);
                ReadAndAssert(UnitTestsUtils.GenerateData(converter, 2, items1stPg - 1), f, items1stPg - 1, 2);
            }
        }

        [Test]
        public void EmptyFileByte()
        {
            EmptyFile<byte>(1);
        }

        [Test]
        public void EmptyFileStruct3()
        {
            EmptyFile<Struct3Byte>(3);
        }

        [Test]
        public void EmptyFileStruct3Union()
        {
            EmptyFile<Struct3ByteUnion>(3);
        }

        [Test]
        public void EmptyFileStructTimeValue()
        {
            EmptyFile<StructTimeValue>(12);
        }

        [Test]
        public void IncrementalAdditionByte()
        {
            FileIncrementalAddition<byte>(UnitTestsUtils.CreateByte);
        }

        [Test]
        public void IncrementalAdditionStruct3()
        {
            FileIncrementalAddition<Struct3Byte>(UnitTestsUtils.CreateStruct3);
        }

        [Test]
        public void IncrementalAdditionStruct3Union()
        {
            FileIncrementalAddition<Struct3ByteUnion>(UnitTestsUtils.CreateStruct3Union);
        }

        [Test]
        public void IncrementalAdditionStructTimeValue()
        {
            FileIncrementalAddition<StructTimeValue>(UnitTestsUtils.CreateStructTimeValue);
        }

        [Test]
        public void PageCheckMMFByte()
        {
            PageBorderOperations<byte>(UnitTestsUtils.CreateByte, true);
        }

        [Test]
        public void PageCheckMMFStruct3Page()
        {
            PageBorderOperations<Struct3Byte>(UnitTestsUtils.CreateStruct3, true);
        }

        [Test]
        public void PageCheckMMFStruct3Union()
        {
            PageBorderOperations<Struct3ByteUnion>(UnitTestsUtils.CreateStruct3Union, true);
        }

        [Test]
        public void PageCheckMMFStructTimeValue()
        {
            PageBorderOperations<StructTimeValue>(UnitTestsUtils.CreateStructTimeValue, true);
        }

        [Test]
        public void PageCheckStreamByte()
        {
            PageBorderOperations<byte>(UnitTestsUtils.CreateByte, false);
        }

        [Test]
        public void PageCheckStreamStruct3Page()
        {
            PageBorderOperations<Struct3Byte>(UnitTestsUtils.CreateStruct3, false);
        }

        [Test]
        public void PageCheckStreamStruct3Union()
        {
            PageBorderOperations<Struct3ByteUnion>(UnitTestsUtils.CreateStruct3Union, false);
        }

        [Test]
        public void PageCheckStreamStructTimeValue()
        {
            PageBorderOperations<StructTimeValue>(UnitTestsUtils.CreateStructTimeValue, false);
        }
    }
}