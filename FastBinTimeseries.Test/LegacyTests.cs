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
using NUnit.Framework;
using Strct = NYurik.FastBinTimeseries.Test._LongByte_SeqPk1;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    [Obsolete("All these tests check obsolete methods")]
    public class LegacyTests : TestsBase
    {
        private BinSeriesFile<long, Strct> OpenFile(string fileName)
        {
            if (!AllowCreate)
                return (BinSeriesFile<long, Strct>) BinaryFile.Open(fileName, false);

            var r = new BinSeriesFile<long, Strct>(fileName) {UniqueIndexes = false};
            r.InitializeNewFile();
            return r;
        }

        [Test(Description = "Issue #8 by karl23")]
        public void TestLegacyReadSupportWithOffset()
        {
            using (BinSeriesFile<long, Strct> f = OpenFile(GetBinFileName()))
            {
                if (AllowCreate)
                {
                    f.AppendData(Data<Strct>(10, 20));
                    TestUtils.CollectionAssertEqual(Data<Strct>(10, 20), f.Stream(0), "#1");
                }

                var buf = new ArraySegment<Strct>(new Strct[21], 10, 11);
                f.ReadData(0, buf);
                TestUtils.CollectionAssertEqual(Data<Strct>(10, 20), buf.StreamSegmentValues(), "#2");
            }
        }
    }
}