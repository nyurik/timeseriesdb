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
using System.IO;
using System.Linq;
using NUnit.Framework;
using Strct = NYurik.FastBinTimeseries.Test._LongByte_SeqPk1;

namespace NYurik.FastBinTimeseries.Test
{
    // ReSharper disable AccessToDisposedClosure

    [TestFixture]
    public class WritableFeedTests : TestsBase
    {
        private void Append(Func<string, IWritableFeed<long, Strct>> newFile)
        {
            string fileName = GetBinFileName();
            using (IWritableFeed<long, Strct> f =
                AllowCreate
                    ? newFile(fileName)
                    : (IWritableFeed<long, Strct>) BinaryFile.Open(fileName, false))
            {
                if (AllowCreate)
                {
                    f.AppendData(Data<Strct>(10, 20));
                    TestUtils.CollectionAssertEqual(Data<Strct>(10, 20), f.Stream(0), "#1");

                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data<Strct>(5, 30)), "#2");
                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data<Strct>(10, 20)), "#2a");
                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data<Strct>(9, 15)), "#2b");
                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data<Strct>(11, 15)), "#2c");
                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data<Strct>(19, 20)), "#2d");
                    TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data<Strct>(19, 20)), "#2d");

                    if (f.UniqueIndexes)
                    {
                        TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data<Strct>(20, 20)), "#3");
                        TestUtils.AssertException<BinaryFileException>(() => f.AppendData(Data<Strct>(20, 30)), "#3a");
                    }

                    f.AppendData(new[] {new ArraySegment<Strct>(new Strct[0], 0, 0)});
                    TestUtils.CollectionAssertEqual(Data<Strct>(10, 20), f.Stream(0), "#5");

                    f.AppendData(new[] {new ArraySegment<Strct>(new Strct[1], 1, 0)});
                    TestUtils.CollectionAssertEqual(Data<Strct>(10, 20), f.Stream(0), "#6");

                    f.AppendData(new ArraySegment<Strct>[0]);
                    TestUtils.CollectionAssertEqual(Data<Strct>(10, 20), f.Stream(0), "#7");

                    f.AppendData(Data<Strct>(10, 14, 2), true);
                    TestUtils.CollectionAssertEqual(Data<Strct>(10, 14, 2), f.Stream(0), "#8");

                    f.AppendData(Data<Strct>(10, 13), true);
                    TestUtils.CollectionAssertEqual(Data<Strct>(10, 13), f.Stream(0), "#9");

                    f.AppendData(Data<Strct>(14, 14));
                    TestUtils.CollectionAssertEqual(Data<Strct>(10, 14), f.Stream(0), "#10");

                    f.AppendData(Data<Strct>(15, 16));
                    TestUtils.CollectionAssertEqual(Data<Strct>(10, 16), f.Stream(0), "#11");

                    f.AppendData(Data<Strct>(10, 10), true);
                    TestUtils.CollectionAssertEqual(Data<Strct>(10, 10), f.Stream(0), "#12");

                    if (!f.UniqueIndexes)
                    {
                        f.AppendData(Data<Strct>(10, 10));
                        TestUtils.CollectionAssertEqual(
                            Join(Data<Strct>(10, 10), Data<Strct>(10, 10)), f.Stream(0), "#13");

                        f.AppendData(Data<Strct>(10, 10), true);
                        TestUtils.CollectionAssertEqual(Data<Strct>(10, 10), f.Stream(0), "#14");

                        f.AppendData(Data<Strct>(10, 11));
                        TestUtils.CollectionAssertEqual(
                            Join(Data<Strct>(10, 10), Data<Strct>(10, 11)), f.Stream(0), "#15");
                    }

                    f.AppendData(Data<Strct>(5, 10), true);
                }

                TestUtils.CollectionAssertEqual(Data<Strct>(5, 10), f.Stream(0), "#final");
            }
        }

        [Test(Description = "Issue #4 by karl23")]
        public void CompressedAppendBug4()
        {
            string fileName = GetBinFileName();
            if (!AllowCreate)
                return;

            using (var f = new BinCompressedSeriesFile<_CmplxIdx, _4Flds_ComplxIdx>(fileName))
            {
                f.UniqueIndexes = false;
                f.InitializeNewFile();

                f.AppendData(Data<_4Flds_ComplxIdx>(10, 20000));
                TestUtils.CollectionAssertEqual(
                    Data<_4Flds_ComplxIdx>(10, 20000),
                    f.Stream(new _CmplxIdx {Field1 = 0}, new _CmplxIdx {Field1 = 50000}), "#1");

                for (int ix = 0; ix < 5000; ix++)
                    f.AppendData(Data<_4Flds_ComplxIdx>(20000 + 5*ix, 20000 + 5*(ix + 1)));
            }
        }

        [Test]
        public void AppendBinCompressedSeriesFile([Values(true, false)] bool isUnique)
        {
            Append(
                fn =>
                    {
                        var r = new BinCompressedSeriesFile<long, Strct>(fn) {UniqueIndexes = isUnique};
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
                        var r = new BinSeriesFile<long, Strct>(fn) {UniqueIndexes = isUnique};
                        r.InitializeNewFile();
                        return r;
                    });
        }
    }
}