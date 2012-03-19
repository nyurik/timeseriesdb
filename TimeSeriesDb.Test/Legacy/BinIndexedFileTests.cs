#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of TimeSeriesDb library
 * 
 *  TimeSeriesDb is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  TimeSeriesDb is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with TimeSeriesDb.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using NUnit.Framework;

namespace NYurik.TimeSeriesDb.Test.Legacy
{
    [TestFixture]
    [Obsolete]
    public class BinIndexedFileTests : LegacyTestsBase
    {
        private const bool EnableLongerTests = false;

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

                string fileName = GetBinFileName();

                if (AllowCreate)
                {
                    using (var f = new BinIndexedFile<T>(fileName))
                    {
                        f.InitializeNewFile();
                        fileVersion = f.Version;
                        Assert.IsNotNull(fileVersion);
                        baseVersion = f.BaseVersion;
                        Assert.IsNotNull(baseVersion);
                        serializerVersion = f.Serializer.Version;
                        Assert.IsNotNull(serializerVersion);

                        Assert.AreEqual(true, f.CanWrite);
                        Assert.AreEqual(0, f.Count);

                        hdrSize = f.HeaderSize;

                        Assert.AreEqual(expectedItemSize, f.ItemSize);
                        Assert.IsTrue(hdrSize%f.ItemSize == 0);
                        Assert.IsTrue(f.IsEmpty);
                    }
                }
                else
                {
                    fileVersion = baseVersion = serializerVersion = default(Version);
                    hdrSize = 0;
                }

                using (BinaryFile file = BinaryFile.Open(fileName, false, LegacyResolver))
                {
                    Assert.IsInstanceOf<BinIndexedFile<T>>(file);
                    Assert.AreEqual(typeof (T), file.ItemType);
                    var f = (BinIndexedFile<T>) file;

                    if (AllowCreate)
                    {
                        Assert.AreEqual(fileVersion, f.Version);
                        Assert.AreEqual(baseVersion, f.BaseVersion);
                        Assert.AreEqual(serializerVersion, f.Serializer.Version);
                        Assert.AreEqual(hdrSize, f.HeaderSize);
                    }

                    Assert.AreEqual(false, f.CanWrite);
                    Assert.AreEqual(0, f.Count);

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
            DeleteTempFiles();
            return Stopwatch.StartNew();
        }

        private static void TestStop<T>(string name, Stopwatch sw)
        {
            Console.WriteLine("{0}<{1}>\t\t{2:n}ms", name, typeof (T).Name, sw.ElapsedMilliseconds);
            sw.Stop();
        }

        private void FileIncrementalAddition<T>() where T : IEquatable<T>
        {
            const string testName = "FileIncrementalAddition";
            try
            {
                Stopwatch sw = TestStart();

                T[] data0 = TestUtils.GenerateData<T>(1, 10);
                T[] data1 = TestUtils.GenerateData<T>(2, 20);
                T[] data2 = TestUtils.GenerateData<T>(3, 30);

                string fileName = GetBinFileName();

                if (AllowCreate)
                {
                    using (var f = new BinIndexedFile<T>(fileName))
                    {
                        f.InitializeNewFile();
                        f.WriteData(0, new ArraySegment<T>(data0));

                        Assert.AreEqual(true, f.CanWrite);
                        Assert.AreEqual(1, f.Count);
                        Assert.IsFalse(f.IsEmpty);

                        ReadAndAssert(data0, f, 0, f.Count);
                    }
                }

                using (BinaryFile file = BinaryFile.Open(fileName, AllowCreate, LegacyResolver))
                {
                    Assert.IsInstanceOf<BinIndexedFile<T>>(file);
                    var f = (BinIndexedFile<T>) file;

                    Assert.AreEqual(AllowCreate, f.CanWrite);
                    if (AllowCreate)
                    {
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
                    }

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

        private void PageBorderOperations<T>(bool enableMma, bool enableLargePages)
            where T : IEquatable<T>
        {
            string testName = "PageBorderOperations_" + (enableMma ? "MMF" : "Stream");
            try
            {
                Stopwatch sw = TestStart();

                for (int i = 1; i < 5; i++)
                    PageBorderOperations<T>(enableMma, BinaryFile.MinPageSize*i);

                PageBorderOperations<T>(enableMma, BinaryFile.MaxLargePageSize - BinaryFile.MinPageSize);
                PageBorderOperations<T>(enableMma, BinaryFile.MaxLargePageSize);
                PageBorderOperations<T>(enableMma, BinaryFile.MaxLargePageSize + BinaryFile.MinPageSize);

                if (enableLargePages)
                {
                    PageBorderOperations<T>(enableMma, 2*BinaryFile.MaxLargePageSize - BinaryFile.MinPageSize);
                    PageBorderOperations<T>(enableMma, 2*BinaryFile.MaxLargePageSize);
                    PageBorderOperations<T>(enableMma, 2*BinaryFile.MaxLargePageSize + BinaryFile.MinPageSize);
                }

                TestStop<T>(testName, sw);
            }
            catch
            {
                Console.WriteLine("Error in " + testName);
                throw;
            }
        }

        private void PageBorderOperations<T>(bool enableMma, int pageSize)
            where T : IEquatable<T>
        {
            DeleteTempFiles();

            string fileName = GetBinFileName();
            using (
                BinIndexedFile<T> f = AllowCreate
                                          ? new BinIndexedFile<T>(fileName)
                                          : (BinIndexedFile<T>)
                                            BinaryFile.Open(fileName, false, LegacyResolver))
            {
                if (AllowCreate)
                {
                    f.InitializeNewFile();
                    f.EnableMemMappedAccessOnRead = enableMma;
                    f.EnableMemMappedAccessOnWrite = enableMma;
                }

                int itemsPerPage = pageSize/Marshal.SizeOf(typeof (T));
                int headerSizeAsItemCount = f.HeaderSize/f.ItemSize;
                int items1StPg = (int) TestUtils.RoundUpToMultiple(headerSizeAsItemCount, itemsPerPage) -
                                 headerSizeAsItemCount;

                if (items1StPg == 0)
                    items1StPg = itemsPerPage;

                T[] dataMinusOne = TestUtils.GenerateData<T>(items1StPg - 1, 0);
                T[] dataZero = TestUtils.GenerateData<T>(items1StPg, 0);
                T[] dataPlusOne = TestUtils.GenerateData<T>(items1StPg + 1, 0);

                if (AllowCreate)
                {
                    f.WriteData(0, new ArraySegment<T>(dataMinusOne));
                    Assert.AreEqual(f.HeaderSize + (items1StPg - 1)*f.ItemSize, new FileInfo(fileName).Length);
                    ReadAndAssert(dataMinusOne, f, 0, dataMinusOne.Length);

                    f.WriteData(0, new ArraySegment<T>(dataZero));
                    Assert.AreEqual(f.HeaderSize + items1StPg*f.ItemSize, new FileInfo(fileName).Length);
                    ReadAndAssert(dataZero, f, 0, dataZero.Length);

                    f.WriteData(0, new ArraySegment<T>(dataPlusOne));
                }

                Assert.AreEqual(f.HeaderSize + (items1StPg + 1)*f.ItemSize, new FileInfo(fileName).Length);
                ReadAndAssert(dataPlusOne, f, 0, dataPlusOne.Length);

                ReadAndAssert(TestUtils.GenerateData<T>(1, items1StPg - 1), f, items1StPg - 1, 1);
                ReadAndAssert(TestUtils.GenerateData<T>(1, items1StPg), f, items1StPg, 1);
                ReadAndAssert(TestUtils.GenerateData<T>(2, items1StPg - 1), f, items1StPg - 1, 2);
            }
        }

        private static void PrintSize<T>(int size)
        {
            int marshalSizeOf = Marshal.SizeOf(typeof (T));
            Console.WriteLine("Marshal.SizeOf({0}) = {1}", typeof (T).Name, marshalSizeOf);
            Console.WriteLine(
                "sizeof({0}) = {1}{2}\n", typeof (T).Name, size,
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
            EmptyFile<_FixedByteBuff7>(15);
        }

        [Test]
        public void IncrementalAddition()
        {
            FileIncrementalAddition<byte>();
            FileIncrementalAddition<_3Byte_noAttr>();
            FileIncrementalAddition<_3Byte_2Shrt_ExplPk1>();
            FileIncrementalAddition<_IntBool_SeqPk1>();
            FileIncrementalAddition<_DatetimeByte_SeqPk1>();
            FileIncrementalAddition<_DatetimeBool_SeqPk1>();
            FileIncrementalAddition<_LongBool_SeqPk1>();
            FileIncrementalAddition<_LongByte_SeqPk1>();
            FileIncrementalAddition<_BoolLongBool_SeqPk1>();
            FileIncrementalAddition<_ByteLongByte_SeqPk1>();
            FileIncrementalAddition<_FixedByteBuff7>();
        }

        [Test]
        public void PageCheckMmf(
            [Values(true, false)] bool enableMemoryMappedAccess)
        {
            PageBorderOperations<byte>(enableMemoryMappedAccess, EnableLongerTests);
            PageBorderOperations<_3Byte_noAttr>(enableMemoryMappedAccess, EnableLongerTests);
            PageBorderOperations<_3Byte_2Shrt_ExplPk1>(enableMemoryMappedAccess, EnableLongerTests);
            PageBorderOperations<_IntBool_SeqPk1>(enableMemoryMappedAccess, EnableLongerTests);
            PageBorderOperations<_DatetimeByte_SeqPk1>(enableMemoryMappedAccess, EnableLongerTests);
            PageBorderOperations<_DatetimeBool_SeqPk1>(enableMemoryMappedAccess, EnableLongerTests);
            PageBorderOperations<_LongBool_SeqPk1>(enableMemoryMappedAccess, EnableLongerTests);
            PageBorderOperations<_LongByte_SeqPk1>(enableMemoryMappedAccess, EnableLongerTests);
            PageBorderOperations<_BoolLongBool_SeqPk1>(enableMemoryMappedAccess, EnableLongerTests);
            PageBorderOperations<_ByteLongByte_SeqPk1>(enableMemoryMappedAccess, EnableLongerTests);
            PageBorderOperations<_FixedByteBuff7>(enableMemoryMappedAccess, EnableLongerTests);
        }

        [Test]
        public void BrokenFixedByteBuff()
        {
            PageBorderOperations<_FixedByteBuff7>(true, EnableLongerTests);
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
            PrintSize<_FixedByteBuff7>(sizeof (_FixedByteBuff7));
        }
    }
}