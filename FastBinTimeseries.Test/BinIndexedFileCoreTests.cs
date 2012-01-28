#region COPYRIGHT

/*
 *     Copyright 2009-2011 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of FastBinTimeseries library
 * 
 *  FastBinTimeseries is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  FastBinTimeseries is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with FastBinTimeseries.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class BinIndexedFileCoreTests : TestsBase
    {
        private const string TagString = "Test123";

        private static void AfterInitValidation(BinaryFile<byte> f, bool canWrite, string fileName)
        {
            // assignment tests
            TestUtils.AssertException<InvalidOperationException>(() => { f.Tag = "a"; });
            TestUtils.AssertException<InvalidOperationException>(() => { f.Serializer = null; });

            Assert.AreEqual(0, f.Count);
            Assert.AreEqual(new Version(1, 0), f.Version);
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
            TestUtils.AreNotEqual(
                new ArraySegment<byte>(buf1, 255, bufSize - 255),
                new ArraySegment<byte>(buf2, 0, bufSize - 255));
            TestUtils.AreEqual(
                new ArraySegment<byte>(buf1, 256, bufSize - 256),
                new ArraySegment<byte>(buf2, 0, bufSize - 256));
            TestUtils.AreNotEqual(
                new ArraySegment<byte>(buf1, 257, bufSize - 257),
                new ArraySegment<byte>(buf2, 0, bufSize - 257));
            TestUtils.AreEqual(
                new ArraySegment<byte>(buf1, 255, bufSize - 511),
                new ArraySegment<byte>(buf2, 511, bufSize - 511));
            TestUtils.AreEqual(
                new ArraySegment<byte>(buf1, 257, bufSize - 257),
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
                    TestUtils.AssertException<InvalidOperationException>(() => f.Count);
                    TestUtils.AssertException<InvalidOperationException>(() => f.Version);
                    TestUtils.AssertException<InvalidOperationException>(() => f.HeaderSize);
                    TestUtils.AssertException<InvalidOperationException>(() => f.IsEmpty);
                    TestUtils.AssertException<InvalidOperationException>(() => f.ItemSize);
                    TestUtils.AssertException<InvalidOperationException>(() => f.EnableMemMappedAccessOnRead);
                    TestUtils.AssertException<InvalidOperationException>(() => f.EnableMemMappedAccessOnWrite);
                    TestUtils.AssertException<InvalidOperationException>(() => f.Serializer.Version);
                    TestUtils.AssertException<InvalidOperationException>(() => f.CanWrite);

                    Assert.IsFalse(f.IsInitialized);
                    Assert.IsFalse(f.IsDisposed);
                    Assert.IsFalse(f.IsOpen);
                    Assert.AreEqual(fileName, f.FileName);
                    Assert.AreEqual("", f.Tag);
                    Assert.AreEqual(typeof (byte), f.ItemType);
                    Assert.IsNotNull(f.Serializer);
                    f.Tag = TagString;
                    Assert.AreEqual(TagString, f.Tag);

                    Version curBaseVer = f.BaseVersion;
                    f.BaseVersion = new Version(1, 0);
                    f.BaseVersion = new Version(1, 1);
                    f.BaseVersion = new Version(1, 2);
                    TestUtils.AssertException<ArgumentNullException>(() => { f.BaseVersion = null; });
                    TestUtils.AssertException<IncompatibleVersionException>(
                        () => { f.BaseVersion = new Version(0, 0); });
                    f.BaseVersion = curBaseVer;


                    f.InitializeNewFile();

                    Assert.IsTrue(f.CanWrite);

                    Assert.IsNotNull(f.Serializer.Version);
                    Assert.IsTrue(f.IsInitialized);
                    Assert.IsFalse(f.IsDisposed);
                    Assert.IsTrue(f.IsOpen);
                    Assert.AreEqual(fileName, f.FileName);

                    Assert.IsFalse(f.EnableMemMappedAccessOnRead);
                    f.EnableMemMappedAccessOnRead = false;
                    Assert.IsFalse(f.EnableMemMappedAccessOnRead);
                    f.EnableMemMappedAccessOnRead = true;
                    Assert.IsTrue(f.EnableMemMappedAccessOnRead);

                    Assert.IsFalse(f.EnableMemMappedAccessOnWrite);
                    f.EnableMemMappedAccessOnWrite = false;
                    Assert.IsFalse(f.EnableMemMappedAccessOnWrite);
                    f.EnableMemMappedAccessOnWrite = true;
                    Assert.IsTrue(f.EnableMemMappedAccessOnWrite);

                    TestUtils.AssertException<InvalidOperationException>(() => f.InitializeNewFile());
                    TestUtils.AssertException<InvalidOperationException>(() => { f.BaseVersion = new Version(1, 1); });

                    AfterInitValidation(f, true, fileName);
                }

                temp.Close(); // allowed after disposing
                ((IDisposable) temp).Dispose(); // disposing multiple times is ok

                TestUtils.AssertException<InvalidOperationException>(() => temp.Tag);
                TestUtils.AssertException<InvalidOperationException>(() => temp.EnableMemMappedAccessOnRead);
                TestUtils.AssertException<InvalidOperationException>(() => temp.EnableMemMappedAccessOnWrite);
                TestUtils.AssertException<InvalidOperationException>(() => temp.Count);
                TestUtils.AssertException<InvalidOperationException>(() => temp.IsEmpty);
                TestUtils.AssertException<InvalidOperationException>(() => temp.ItemSize);
                TestUtils.AssertException<InvalidOperationException>(() => temp.NonGenericSerializer);
                TestUtils.AssertException<InvalidOperationException>(() => temp.Serializer);

                Assert.IsTrue(temp.IsInitialized);
                Assert.IsTrue(temp.IsDisposed);
                Assert.IsFalse(temp.IsOpen);
                Assert.AreEqual(fileName, temp.FileName);
                Assert.AreEqual(typeof (byte), temp.ItemType);


                using (
                    var f =
                        (BinIndexedFile<byte>) BinaryFile.Open(fileName, AllowCreate, LegacySupport.GenerateMapping()))
                {
                    AfterInitValidation(f, true, fileName);
                    f.Close();
                    TestUtils.AssertException<InvalidOperationException>(() => f.Tag);

                    Assert.IsTrue(f.IsInitialized);
                    Assert.IsTrue(f.IsDisposed);
                    Assert.IsFalse(f.IsOpen);
                    Assert.AreEqual(fileName, f.FileName);
                }
            }

            using (var f = (BinIndexedFile<byte>) BinaryFile.Open(fileName, false, LegacySupport.GenerateMapping()))
            {
                AfterInitValidation(f, false, fileName);
                ((IDisposable) f).Dispose();
                TestUtils.AssertException<InvalidOperationException>(() => f.Tag);
            }

            using (var f = new BinIndexedFile<byte>(fileName))
            {
                TestUtils.AssertException<IOException>(() => f.InitializeNewFile());

                if (RunMode == Mode.OneTime)
                {
                    File.Delete(fileName);
                    f.InitializeNewFile();
                }
            }
        }

        [Test]
        public void MappingTest()
        {
            string fileName = GetBinFileName();
            _DatetimeByte_SeqPk1[] data = TestUtils.GenerateData(_DatetimeByte_SeqPk1.New, 1, 10);
            if (AllowCreate)
            {
                using (var f = new BinIndexedFile<_DatetimeByte_SeqPk1>(fileName))
                {
                    f.InitializeNewFile();
                    f.WriteData(0, new ArraySegment<_DatetimeByte_SeqPk1>(data));
                }
            }

            IDictionary<string, Type> map = LegacySupport.GenerateMapping();
            // ReSharper disable AssignNullToNotNullAttribute
            map.Add(typeof (_DatetimeByte_SeqPk1).GetUnversionedNameAssembly(), typeof (_LongByte_SeqPk1));
            // ReSharper restore AssignNullToNotNullAttribute

            using (BinaryFile f = BinaryFile.Open(fileName, false, map))
            {
                var p = (BinIndexedFile<_LongByte_SeqPk1>) f;

                var data2 = new _LongByte_SeqPk1[1];
                p.ReadData(0, new ArraySegment<_LongByte_SeqPk1>(data2));

                Assert.AreEqual(data[0].a.Ticks, data2[0].a);
                Assert.AreEqual(data[0].b, data2[0].b);
            }
        }
    }
}