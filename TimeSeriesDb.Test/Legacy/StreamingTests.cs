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
using System.IO;
using NUnit.Framework;

namespace NYurik.TimeSeriesDb.Test.Legacy
{
    [TestFixture]
    [Obsolete]
    public class StreamingTests : TestsBase
    {
        [Test]
        public void StreamingTest()
        {
            string fileName = GetBinFileName();

            byte[] data = TestUtils.GenerateData<byte>(10000, 0);
            if (AllowCreate)
            {
                using (var b = new BinIndexedFile<byte>(fileName))
                {
                    b.InitializeNewFile();
                    b.WriteData(0, new ArraySegment<byte>(data));
                }
            }

            byte[] bytes = File.ReadAllBytes(fileName);

            using (var b = (BinIndexedFile<byte>) BinaryFile.Open(fileName, false, LegacyResolver))
            {
                var ms = new MemoryStream(bytes);
                var cs = new ConfigurableStream(ms);
                var data2 = new byte[data.Length/2];

                cs.AllowSeek = cs.AllowWrite = false;
                var b2 = (BinIndexedFile<byte>) BinaryFile.Open(cs, LegacyResolver);
                Assert.IsTrue(b2.IsOpen);
                Assert.AreEqual(b.ItemSize, b2.ItemSize);

                b2.ReadData(0, new ArraySegment<byte>(data2));
                TestUtils.CollectionAssertEqual(TestUtils.GenerateData<byte>(data.Length/2, 0), data2);

                b2.ReadData(0, new ArraySegment<byte>(data2));
                TestUtils.CollectionAssertEqual(TestUtils.GenerateData<byte>(data.Length/2, data.Length/2), data2);
            }
        }
    }
}