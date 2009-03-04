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
            TestUtils.AreEqual(expected, buffer);
        }

        private void EmptyFile<T>(int expectedItemSize)
        {
            Cleanup();

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

        private void FileIncrementalAddition<T>(Func<long, T> converter) where T : IEquatable<T>
        {
            Cleanup();

            var data0 = TestUtils.GenerateData(converter, 1, 10);
            var data1 = TestUtils.GenerateData(converter, 2, 20);
            var data2 = TestUtils.GenerateData(converter, 3, 30);

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
                ReadAndAssert(TestUtils.Concatenate(data1, data2), f, 0, f.Count);
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
                var items1stPg = (int) TestUtils.RoundUpToMultiple(f.HeaderSizeAsItemCount, itemsPerPage) -
                                 f.HeaderSizeAsItemCount;

                if (items1stPg == 0)
                    items1stPg = itemsPerPage;

                var dataMinusOne = TestUtils.GenerateData(converter, items1stPg - 1, 0);
                var dataZero = TestUtils.GenerateData(converter, items1stPg, 0);
                var dataPlusOne = TestUtils.GenerateData(converter, items1stPg + 1, 0);

                f.WriteData(0, dataMinusOne, 0, dataMinusOne.Length);
                Assert.AreEqual(f.HeaderSize + (items1stPg - 1)*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataMinusOne, f, 0, dataMinusOne.Length);

                f.WriteData(0, dataZero, 0, dataZero.Length);
                Assert.AreEqual(f.HeaderSize + items1stPg*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataZero, f, 0, dataZero.Length);

                f.WriteData(0, dataPlusOne, 0, dataPlusOne.Length);
                Assert.AreEqual(f.HeaderSize + (items1stPg + 1)*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataPlusOne, f, 0, dataPlusOne.Length);

                ReadAndAssert(TestUtils.GenerateData(converter, 1, items1stPg - 1), f, items1stPg - 1, 1);
                ReadAndAssert(TestUtils.GenerateData(converter, 1, items1stPg), f, items1stPg, 1);
                ReadAndAssert(TestUtils.GenerateData(converter, 2, items1stPg - 1), f, items1stPg - 1, 2);
            }
        }

        [Test]
        public unsafe void PrintStructSizes()
        {
            PrintSize<byte>(sizeof(byte));
            PrintSize<_3Byte_noAttr>(sizeof(_3Byte_noAttr));
            PrintSize<_3Byte_2Shrt_ExplPk1>(sizeof(_3Byte_2Shrt_ExplPk1));
            PrintSize<_IntBool_SeqPk1>(sizeof(_IntBool_SeqPk1));
            PrintSize<_DatetimeByte_SeqPk1>(sizeof(_DatetimeByte_SeqPk1));
            PrintSize<_DatetimeBool_SeqPk1>(sizeof(_DatetimeBool_SeqPk1));
            PrintSize<_LongBool_SeqPk1>(sizeof(_LongBool_SeqPk1));
            PrintSize<_LongByte_SeqPk1>(sizeof(_LongByte_SeqPk1));
            PrintSize<_BoolLongBool_SeqPk1>(sizeof(_BoolLongBool_SeqPk1));
            PrintSize<_ByteLongByte_SeqPk1>(sizeof(_ByteLongByte_SeqPk1));
        }

        private static void PrintSize<T>(int size)
        {
            var marshalSizeOf = Marshal.SizeOf(typeof(T));
            Console.WriteLine("Marshal.SizeOf({0}) = {1}", typeof(T).Name, marshalSizeOf);
            Console.WriteLine("sizeof({0})         = {1}{2}\n", typeof (T).Name, size,
                              marshalSizeOf != size ? " ****" : "");
        }

        [Test]
        public void EmptyFile()
        {
            EmptyFile<byte>(1);
            EmptyFile<_3Byte_noAttr>(3);
            EmptyFile<_3Byte_2Shrt_ExplPk1>(3);
            EmptyFile<_IntBool_SeqPk1>(5);
            EmptyFile<_DatetimeByte_SeqPk1>(12);
            EmptyFile<_DatetimeBool_SeqPk1>(12);
            EmptyFile<_LongBool_SeqPk1>(9);
            EmptyFile<_LongByte_SeqPk1>(9);
            EmptyFile<_BoolLongBool_SeqPk1>(10);
            EmptyFile<_ByteLongByte_SeqPk1>(10);
        }

        [Test]
        public void IncrementalAddition()
        {
            FileIncrementalAddition<byte>(TestUtils.NewByte);
            FileIncrementalAddition<_3Byte_noAttr>(_3Byte_noAttr.New);
            FileIncrementalAddition<_3Byte_2Shrt_ExplPk1>(_3Byte_2Shrt_ExplPk1.New);
            FileIncrementalAddition<_IntBool_SeqPk1>(_IntBool_SeqPk1.New);
            FileIncrementalAddition<_DatetimeByte_SeqPk1>(_DatetimeByte_SeqPk1.New);
            FileIncrementalAddition<_DatetimeBool_SeqPk1>(_DatetimeBool_SeqPk1.New);
            FileIncrementalAddition<_LongBool_SeqPk1>(_LongBool_SeqPk1.New);
            FileIncrementalAddition<_LongByte_SeqPk1>(_LongByte_SeqPk1.New);
            FileIncrementalAddition<_BoolLongBool_SeqPk1>(_BoolLongBool_SeqPk1.New);
            FileIncrementalAddition<_ByteLongByte_SeqPk1>(_ByteLongByte_SeqPk1.New);
        }

        [Test]
        public void PageCheckMMF()
        {
            PageBorderOperations<byte>(TestUtils.NewByte, true);
            PageBorderOperations<_3Byte_noAttr>(_3Byte_noAttr.New, true);
            PageBorderOperations<_3Byte_2Shrt_ExplPk1>(_3Byte_2Shrt_ExplPk1.New, true);
            PageBorderOperations<_IntBool_SeqPk1>(_IntBool_SeqPk1.New, true);
            PageBorderOperations<_DatetimeByte_SeqPk1>(_DatetimeByte_SeqPk1.New, true);
            PageBorderOperations<_DatetimeBool_SeqPk1>(_DatetimeBool_SeqPk1.New, true);
            PageBorderOperations<_LongBool_SeqPk1>(_LongBool_SeqPk1.New, true);
            PageBorderOperations<_LongByte_SeqPk1>(_LongByte_SeqPk1.New, true);
            PageBorderOperations<_BoolLongBool_SeqPk1>(_BoolLongBool_SeqPk1.New, true);
            PageBorderOperations<_ByteLongByte_SeqPk1>(_ByteLongByte_SeqPk1.New, true);
        }

        [Test]
        public void PageCheckStream()
        {
            PageBorderOperations<byte>(TestUtils.NewByte, false);
            PageBorderOperations<_3Byte_noAttr>(_3Byte_noAttr.New, false);
            PageBorderOperations<_3Byte_2Shrt_ExplPk1>(_3Byte_2Shrt_ExplPk1.New, false);
            PageBorderOperations<_IntBool_SeqPk1>(_IntBool_SeqPk1.New, false);
            PageBorderOperations<_DatetimeByte_SeqPk1>(_DatetimeByte_SeqPk1.New, false);
            PageBorderOperations<_DatetimeBool_SeqPk1>(_DatetimeBool_SeqPk1.New, false);
            PageBorderOperations<_LongBool_SeqPk1>(_LongBool_SeqPk1.New, false);
            PageBorderOperations<_LongByte_SeqPk1>(_LongByte_SeqPk1.New, false);
            PageBorderOperations<_BoolLongBool_SeqPk1>(_BoolLongBool_SeqPk1.New, false);
            PageBorderOperations<_ByteLongByte_SeqPk1>(_ByteLongByte_SeqPk1.New, false);
        }
    }
}