using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class UnitTestsSimple
    {
        #region Setup/Teardown

        [SetUp]
        [TearDown]
        public void Cleanup()
        {
            if (File.Exists(binFile))
                File.Delete(binFile);
        }

        #endregion

        private const string binFile = @"LargeTempDataFile2.bsd";

        private static void PageBorderOperations<T>(Func<long, T> converter)
        {
            using (var f = new BinIndexedFile<T>(binFile))
            {
                var items1stPg = (int) RoundUpToMultiple(f.HeaderSizeAsItemCount, f.ItemsPerPage) -
                                 f.HeaderSizeAsItemCount;

                if (items1stPg == 0)
                    items1stPg = f.ItemsPerPage;

                var dataMinusOne = GenerateData(converter, items1stPg - 1, 0);
                var dataZero = GenerateData(converter, items1stPg, 0);
                var dataPlusOne = GenerateData(converter, items1stPg + 1, 0);

                f.WriteData(0, dataMinusOne, 0, dataMinusOne.Length);
                Assert.AreEqual(f.HeaderSize + (items1stPg - 1)*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataMinusOne, f, 0, dataMinusOne.Length);

                f.WriteData(0, dataZero, 0, dataZero.Length);
                Assert.AreEqual(f.HeaderSize + items1stPg*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataZero, f, 0, dataZero.Length);

                f.WriteData(0, dataPlusOne, 0, dataPlusOne.Length);
                Assert.AreEqual(f.HeaderSize + (items1stPg + 1)*f.ItemSize + f.PagePadding, new FileInfo(binFile).Length);
                ReadAndAssert(dataPlusOne, f, 0, dataPlusOne.Length);
            }
        }

        private static void FileIncrementalAddition<T>(Func<long, T> converter)
        {
            var data0 = GenerateData(converter, 1, 10);
            var data1 = GenerateData(converter, 2, 20);
            var data2 = GenerateData(converter, 3, 30);

            using (var f = new BinIndexedFile<T>(binFile))
            {
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
                ReadAndAssert(Concatenate(data1, data2), f, 0, f.Count);
            }
        }

        static long RoundUpToMultiple(long value, long multiple)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException("value", value, "Value must be >= 0");
            if (value == 0)
                return 0;
            return value - 1 + (multiple - (value - 1) % multiple);
        }

        private static T[] Concatenate<T>(params T[][] arrays)
        {
            var res = new List<T>();
            foreach (var a in arrays)
                res.AddRange(a);
            return res.ToArray();
        }

        private static void WriteData<T>(BinIndexedFile<T> f, long firstItemIndex, T[] buffer)
        {
            f.WriteData(firstItemIndex, buffer, 0, buffer.Length);
        }

        private static T[] GenerateData<T>(Func<long, T> converter, int count, int startFrom)
        {
            var result = new T[count];
            for (long i = 0; i < count; i++)
                result[i] = converter(i + startFrom);
            return result;
        }

        private static void ReadAndAssert<T>(T[] expected, BinIndexedFile<T> f, int firstItemIndex, long count)
        {
            var buffer = new T[count];
            f.ReadData(firstItemIndex, buffer, 0, buffer.Length);
            CollectionAssert.AreEqual(expected, buffer);
        }

        private static Struct3Byte CreateStruct3(long i)
        {
            return new Struct3Byte(
                (byte) ((i & 0xFF0000) >> 16), (byte) ((i & 0xFF00) >> 8),
                (byte) (i & 0xFF));
        }

        private static byte CreateByte(long i)
        {
            return (byte) (i & 0xFF);
        }

        [Test]
        public void ByteEmptyFile()
        {
            int hdrSize, pageSize;
            Version fileVersion, baseVersion, serializerVersion;

            using (var f = new BinIndexedFile<byte>(binFile))
            {
                fileVersion = f.FileVersion;
                Assert.IsNotNull(fileVersion);
                baseVersion = f.BaseVersion;
                Assert.IsNotNull(baseVersion);
                serializerVersion = f.SerializerVersion;
                Assert.IsNotNull(serializerVersion);

                Assert.AreEqual(true, f.CanWrite);
                Assert.AreEqual(0, f.Count);

                hdrSize = f.HeaderSize;
                pageSize = f.PageSize;

                Assert.AreEqual(0, pageSize%(4*1024));
                Assert.AreEqual(1, f.ItemSize);
                Assert.AreEqual(hdrSize, f.HeaderSizeAsItemCount*f.ItemSize);
                Assert.IsTrue(f.IsEmpty);
                Assert.AreEqual(pageSize/1, f.ItemsPerPage);
                Assert.AreEqual(0, f.PagePadding);
            }

            using (var file = BinaryFile.Open(binFile, false))
            {
                Assert.IsInstanceOfType(typeof (BinIndexedFile<byte>), file);
                var f = (BinIndexedFile<byte>) file;

                Assert.AreEqual(fileVersion, f.FileVersion);
                Assert.AreEqual(baseVersion, f.BaseVersion);
                Assert.AreEqual(serializerVersion, f.SerializerVersion);

                Assert.AreEqual(false, f.CanWrite);
                Assert.AreEqual(0, f.Count);

                Assert.AreEqual(hdrSize, f.HeaderSize);
                Assert.AreEqual(pageSize, f.PageSize);

                Assert.AreEqual(hdrSize, f.HeaderSizeAsItemCount*f.ItemSize);
                Assert.IsTrue(f.IsEmpty);
                Assert.AreEqual(1, f.ItemSize);
                Assert.AreEqual(pageSize/1, f.ItemsPerPage);
                Assert.AreEqual(0, f.PagePadding);
            }
        }

        [Test]
        public void ByteFileIncrementalAddition()
        {
            FileIncrementalAddition<byte>(CreateByte);
        }

        [Test]
        public void ByteFilePageBorderOps()
        {
            PageBorderOperations<byte>(CreateByte);
        }

        [Test]
        public void Struct3EmptyFile()
        {
            int hdrSize, pageSize;
            Version fileVersion, baseVersion, serializerVersion;

            using (var f = new BinIndexedFile<Struct3Byte>(binFile))
            {
                fileVersion = f.FileVersion;
                Assert.IsNotNull(fileVersion);
                baseVersion = f.BaseVersion;
                Assert.IsNotNull(baseVersion);
                serializerVersion = f.SerializerVersion;
                Assert.IsNotNull(serializerVersion);

                Assert.AreEqual(true, f.CanWrite);
                Assert.AreEqual(0, f.Count);

                hdrSize = f.HeaderSize;
                pageSize = f.PageSize;

                Assert.AreEqual(0, pageSize%(4*1024));
                Assert.AreEqual(3, f.ItemSize);
                Assert.AreEqual(hdrSize, f.HeaderSizeAsItemCount*f.ItemSize);
                Assert.IsTrue(f.IsEmpty);
                Assert.AreEqual(pageSize/3, f.ItemsPerPage);
                Assert.AreEqual(1, f.PagePadding);
            }

            using (var file = BinaryFile.Open(binFile, false))
            {
                Assert.IsInstanceOfType(typeof (BinIndexedFile<Struct3Byte>), file);
                var f = (BinIndexedFile<Struct3Byte>) file;

                Assert.AreEqual(fileVersion, f.FileVersion);
                Assert.AreEqual(baseVersion, f.BaseVersion);
                Assert.AreEqual(serializerVersion, f.SerializerVersion);

                Assert.AreEqual(false, f.CanWrite);
                Assert.AreEqual(0, f.Count);

                Assert.AreEqual(hdrSize, f.HeaderSize);
                Assert.AreEqual(pageSize, f.PageSize);

                Assert.AreEqual(hdrSize, f.HeaderSizeAsItemCount*f.ItemSize);
                Assert.IsTrue(f.IsEmpty);
                Assert.AreEqual(3, f.ItemSize);
                Assert.AreEqual(pageSize/3, f.ItemsPerPage);
                Assert.AreEqual(1, f.PagePadding);
            }
        }

        [Test]
        public void Struct3FileIncrementalAddition()
        {
            FileIncrementalAddition<Struct3Byte>(CreateStruct3);
        }
        [Test]
        public void Struct3PageBorderOps()
        {
            PageBorderOperations<Struct3Byte>(CreateStruct3);
        }
    }
}