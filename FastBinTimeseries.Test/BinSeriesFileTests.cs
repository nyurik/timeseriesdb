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
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class BinSeriesFileTests : TestsBase
    {
        [Test]
        public void Append()
        {
            string fileName = GetBinFileName();
            using (BinSeriesFile<long, _LongByte_SeqPk1> f =
                AllowCreate
                    ? new BinSeriesFile<long, _LongByte_SeqPk1>(fileName, typeof (_LongByte_SeqPk1).GetField("a"))
                    : (BinSeriesFile<long, _LongByte_SeqPk1>) BinaryFile.Open(fileName, false))
            {
                if (AllowCreate)
                {
                    f.InitializeNewFile();

                    IEnumerable<ArraySegment<_LongByte_SeqPk1>> d = TestUtils.GenerateDataStream(
                        _LongByte_SeqPk1.New, int.MaxValue, 20, 40);
                    f.AppendData(d);
                    TestUtils.CollectionAssertEqual(d, f.Stream(0));

                    f.AppendData(TestUtils.GenerateDataStream(_LongByte_SeqPk1.New, int.MaxValue, 30, 50));
                    TestUtils.CollectionAssertEqual(
                        TestUtils.GenerateDataStream(_LongByte_SeqPk1.New, int.MaxValue, 20, 50), f.Stream(0));
                }
            }
        }
    }
}