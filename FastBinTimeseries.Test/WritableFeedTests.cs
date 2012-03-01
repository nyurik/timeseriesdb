#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
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
using System.Linq;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    // ReSharper disable AccessToDisposedClosure

    [TestFixture]
    public class WritableFeedTests : TestsBase
    {
        private void Append(Func<string, IWritableFeed<long, _LongByte_SeqPk1>> newFile)
        {
            string fileName = GetBinFileName();
            using (IWritableFeed<long, _LongByte_SeqPk1> f =
                AllowCreate
                    ? newFile(fileName)
                    : (IWritableFeed<long, _LongByte_SeqPk1>) BinaryFile.Open(fileName, false))
            {
                if (AllowCreate)
                {
                    f.AppendData(Data(10, 20));
                    TestUtils.CollectionAssertEqual(Data(10, 20), f.Stream(0), "#1");

                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data(5, 30)), "#2");
                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data(10, 20)), "#2a");
                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data(9, 15)), "#2b");
                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data(11, 15)), "#2c");
                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data(19, 20)), "#2d");
                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data(19, 20)), "#2d");

                    if (f.UniqueIndexes)
                    {
                        TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data(20, 20)), "#3");
                        TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data(20, 30)), "#3a");
                    }

                    f.AppendData(new[] {new ArraySegment<_LongByte_SeqPk1>(new _LongByte_SeqPk1[0], 0, 0)});
                    TestUtils.CollectionAssertEqual(Data(10, 20), f.Stream(0), "#5");

                    f.AppendData(new[] {new ArraySegment<_LongByte_SeqPk1>(new _LongByte_SeqPk1[1], 1, 0)});
                    TestUtils.CollectionAssertEqual(Data(10, 20), f.Stream(0), "#6");

                    f.AppendData(new ArraySegment<_LongByte_SeqPk1>[0]);
                    TestUtils.CollectionAssertEqual(Data(10, 20), f.Stream(0), "#7");

                    f.AppendData(Data(10, 14, 2), true);
                    TestUtils.CollectionAssertEqual(Data(10, 14, 2), f.Stream(0), "#8");

                    f.AppendData(Data(10, 13), true);
                    TestUtils.CollectionAssertEqual(Data(10, 13), f.Stream(0), "#9");

                    f.AppendData(Data(14, 14));
                    TestUtils.CollectionAssertEqual(Data(10, 14), f.Stream(0), "#10");

                    f.AppendData(Data(15, 16));
                    TestUtils.CollectionAssertEqual(Data(10, 16), f.Stream(0), "#11");

                    f.AppendData(Data(10, 10), true);
                    TestUtils.CollectionAssertEqual(Data(10, 10), f.Stream(0), "#12");

                    if (!f.UniqueIndexes)
                    {
                        f.AppendData(Data(10, 10));
                        TestUtils.CollectionAssertEqual(Add(Data(10, 10), Data(10, 10)), f.Stream(0), "#13");

                        f.AppendData(Data(10, 10), true);
                        TestUtils.CollectionAssertEqual(Data(10, 10), f.Stream(0), "#14");

                        f.AppendData(Data(10, 11));
                        TestUtils.CollectionAssertEqual(Add(Data(10, 10), Data(10, 11)), f.Stream(0), "#15");
                    }

                    f.AppendData(Data(5, 10), true);
                }

                TestUtils.CollectionAssertEqual(Data(5, 10), f.Stream(0), "#final");
            }
        }

        private static IEnumerable<T> Add<T>(params IEnumerable<T>[] enmrs)
        {
            return enmrs.SelectMany(i => i);
        }

        private static IEnumerable<ArraySegment<_LongByte_SeqPk1>> Data(int minValue, int maxValue, int step = 1)
        {
            return TestUtils.GenerateSimpleData(_LongByte_SeqPk1.New, minValue, maxValue, step);
        }

        [Test]
        public void AppendBinCompressedSeriesFile([Values(true, false)] bool isUnique)
        {
            Append(
                fn =>
                    {
                        var r = new BinCompressedSeriesFile<long, _LongByte_SeqPk1>(fn) {UniqueIndexes = isUnique};
                        r.InitializeNewFile();
                        return r;
                    });
        }

        [Test]
        public void AppendBinSeriesFile([Values(true, false)] bool isUnique)
        {
            Append(
                fn =>
                    {
                        var r = new BinSeriesFile<long, _LongByte_SeqPk1>(fn) {UniqueIndexes = isUnique};
                        r.InitializeNewFile();
                        return r;
                    });
        }
    }
}