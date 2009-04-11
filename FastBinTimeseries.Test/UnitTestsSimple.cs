using System;
using System.Diagnostics;
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
            // perform the init to count accurate performance
            new PackedDateTime();
            if (File.Exists(binFile))
                File.Delete(binFile);
        }

        #endregion

        private static readonly string binFile = MethodBase.GetCurrentMethod().DeclaringType + ".bsd";

        private static void WriteData<T>(BinIndexedFile<T> f, long firstItemIndex, T[] buffer)
        {
            f.WriteData(firstItemIndex, new ArraySegment<T>(buffer));
        }

        private static void ReadAndAssert<T>(T[] expected, BinIndexedFile<T> f, int firstItemIndex, long count)
            where T : IEquatable<T>
        {
            var buffer = new T[count];
            f.ReadData(firstItemIndex, new ArraySegment<T>(buffer));
            TestUtils.AreEqual(expected, buffer);
        }

        private void EmptyFile<T>(int expectedItemSize)
        {
            const string testName = "EmptyFile";
            try
            {
                Stopwatch sw = TestStart();

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
                    Assert.IsTrue(hdrSize%f.ItemSize == 0);
                    Assert.IsTrue(f.IsEmpty);
                }

                using (BinaryFile file = BinaryFile.Open(binFile, false))
                {
                    Assert.IsInstanceOfType(typeof (BinIndexedFile<T>), file);
                    var f = (BinIndexedFile<T>) file;

                    Assert.AreEqual(fileVersion, f.FileVersion);
                    Assert.AreEqual(baseVersion, f.BaseVersion);
                    Assert.AreEqual(serializerVersion, f.SerializerVersion);

                    Assert.AreEqual(false, f.CanWrite);
                    Assert.AreEqual(0, f.Count);

                    Assert.AreEqual(hdrSize, f.HeaderSize);

                    Assert.IsTrue(f.IsEmpty);
                    Assert.AreEqual(expectedItemSize, f.ItemSize);
                }

                TestStop<T>(testName, sw);
            }
            catch
            {
                Console.WriteLine("Error in " + testName);
                throw;
            }
        }

        private Stopwatch TestStart()
        {
            Cleanup();
            return Stopwatch.StartNew();
        }

        private static void TestStop<T>(string name, Stopwatch sw)
        {
            Console.WriteLine("{0}<{1}>\t\t{2:n}ms", name, typeof (T).Name, sw.ElapsedMilliseconds);
            sw.Stop();
        }

        private void FileIncrementalAddition<T>(Func<long, T> converter) where T : IEquatable<T>
        {
            const string testName = "FileIncrementalAddition";
            try
            {
                Stopwatch sw = TestStart();

                T[] data0 = TestUtils.GenerateData(converter, 1, 10);
                T[] data1 = TestUtils.GenerateData(converter, 2, 20);
                T[] data2 = TestUtils.GenerateData(converter, 3, 30);

                using (var f = new BinIndexedFile<T>(binFile))
                {
                    f.InitializeNewFile();
                    f.WriteData(0, new ArraySegment<T>(data0));

                    Assert.AreEqual(true, f.CanWrite);
                    Assert.AreEqual(1, f.Count);
                    Assert.IsFalse(f.IsEmpty);

                    ReadAndAssert(data0, f, 0, f.Count);
                }

                using (BinaryFile file = BinaryFile.Open(binFile, true))
                {
                    Assert.IsInstanceOfType(typeof (BinIndexedFile<T>), file);
                    var f = (BinIndexedFile<T>) file;
                    Assert.AreEqual(true, f.CanWrite);
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

                TestStop<T>(testName, sw);
            }
            catch
            {
                Console.WriteLine("Error in " + testName);
                throw;
            }
        }

        private void PageBorderOperations<T>(Func<long, T> converter, bool enableMemoryMappedAccess)
            where T : IEquatable<T>
        {
            string testName = "PageBorderOperations_" + (enableMemoryMappedAccess ? "MMF" : "Stream");
            try
            {
                Stopwatch sw = TestStart();

                for (int i = 1; i < 5; i++)
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

                TestStop<T>(testName, sw);
            }
            catch
            {
                Console.WriteLine("Error in " + testName);
                throw;
            }
        }

        private void PageBorderOperations<T>(Func<long, T> converter, bool enableMemoryMappedAccess, int pageSize)
            where T : IEquatable<T>
        {
            Cleanup();

            using (var f = new BinIndexedFile<T>(binFile))
            {
                f.InitializeNewFile();
                f.EnableMemoryMappedFileAccess = enableMemoryMappedAccess;

                int itemsPerPage = pageSize/Marshal.SizeOf(typeof (T));
                int headerSizeAsItemCount = f.HeaderSize/f.ItemSize;
                int items1stPg = (int) TestUtils.RoundUpToMultiple(headerSizeAsItemCount, itemsPerPage) -
                                 headerSizeAsItemCount;

                if (items1stPg == 0)
                    items1stPg = itemsPerPage;

                T[] dataMinusOne = TestUtils.GenerateData(converter, items1stPg - 1, 0);
                T[] dataZero = TestUtils.GenerateData(converter, items1stPg, 0);
                T[] dataPlusOne = TestUtils.GenerateData(converter, items1stPg + 1, 0);

                f.WriteData(0, new ArraySegment<T>(dataMinusOne));
                Assert.AreEqual(f.HeaderSize + (items1stPg - 1)*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataMinusOne, f, 0, dataMinusOne.Length);

                f.WriteData(0, new ArraySegment<T>(dataZero));
                Assert.AreEqual(f.HeaderSize + items1stPg*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataZero, f, 0, dataZero.Length);

                f.WriteData(0, new ArraySegment<T>(dataPlusOne));
                Assert.AreEqual(f.HeaderSize + (items1stPg + 1)*f.ItemSize, new FileInfo(binFile).Length);
                ReadAndAssert(dataPlusOne, f, 0, dataPlusOne.Length);

                ReadAndAssert(TestUtils.GenerateData(converter, 1, items1stPg - 1), f, items1stPg - 1, 1);
                ReadAndAssert(TestUtils.GenerateData(converter, 1, items1stPg), f, items1stPg, 1);
                ReadAndAssert(TestUtils.GenerateData(converter, 2, items1stPg - 1), f, items1stPg - 1, 2);
            }
        }

        private static void PrintSize<T>(int size)
        {
            int marshalSizeOf = Marshal.SizeOf(typeof (T));
            Console.WriteLine("Marshal.SizeOf({0}) = {1}", typeof (T).Name, marshalSizeOf);
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
            EmptyFile<_DatetimeByte_SeqPk1>(9);
            EmptyFile<_DatetimeBool_SeqPk1>(9);
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

        [Test]
        public unsafe void PrintStructSizes()
        {
            PrintSize<byte>(sizeof (byte));
            PrintSize<_3Byte_noAttr>(sizeof (_3Byte_noAttr));
            PrintSize<_3Byte_2Shrt_ExplPk1>(sizeof (_3Byte_2Shrt_ExplPk1));
            PrintSize<_IntBool_SeqPk1>(sizeof (_IntBool_SeqPk1));
            PrintSize<_DatetimeByte_SeqPk1>(sizeof (_DatetimeByte_SeqPk1));
            PrintSize<_DatetimeBool_SeqPk1>(sizeof (_DatetimeBool_SeqPk1));
            PrintSize<_LongBool_SeqPk1>(sizeof (_LongBool_SeqPk1));
            PrintSize<_LongByte_SeqPk1>(sizeof (_LongByte_SeqPk1));
            PrintSize<_BoolLongBool_SeqPk1>(sizeof (_BoolLongBool_SeqPk1));
            PrintSize<_ByteLongByte_SeqPk1>(sizeof (_ByteLongByte_SeqPk1));
        }
    }
}