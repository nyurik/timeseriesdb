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
using NUnit.Framework;

namespace NYurik.TimeSeriesDb.Test.Legacy
{
    [TestFixture]
    [Obsolete("All these tests check obsolete methods")]
    public class LegacyTests : LegacyTestsBase
    {
        private BinSeriesFile<long, _LongByte_SeqPk1> OpenFile(string fileName)
        {
            if (!AllowCreate)
                return (BinSeriesFile<long, _LongByte_SeqPk1>) BinaryFile.Open(fileName, false);

            var r = new BinSeriesFile<long, _LongByte_SeqPk1>(fileName) {UniqueIndexes = false};
            r.InitializeNewFile();
            return r;
        }

        [Test(Description = "Issue #8 by karl23")]
        public void TestLegacyReadSupportWithOffset()
        {
            using (BinSeriesFile<long, _LongByte_SeqPk1> f = OpenFile(GetBinFileName()))
            {
                if (AllowCreate)
                {
                    f.AppendData(Data<_LongByte_SeqPk1>(10, 20));
                    TestUtils.CollectionAssertEqual(Data<_LongByte_SeqPk1>(10, 20), f.Stream(0), "#1");
                }

                var buf = new ArraySegment<_LongByte_SeqPk1>(new _LongByte_SeqPk1[21], 10, 11);
                f.ReadData(0, buf);
                TestUtils.CollectionAssertEqual(Data<_LongByte_SeqPk1>(10, 20), buf.Stream(), "#2");
            }
        }
    }
}