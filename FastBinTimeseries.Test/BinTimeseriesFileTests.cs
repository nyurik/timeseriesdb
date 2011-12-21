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
using System.Linq;
using NUnit.Framework;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class BinTimeseriesFileTests : TestsBase
    {
        private void RunTest(int itemCount, int repeatRuns, bool uniqueTimestamps, bool enableCache)
        {
            string fileName = GetBinFileName();
            using (BinTimeseriesFile<_DatetimeByte_SeqPk1> f =
                AllowCreate
                    ? new BinTimeseriesFile<_DatetimeByte_SeqPk1>(fileName) {UniqueTimestamps = uniqueTimestamps}
                    : (BinTimeseriesFile<_DatetimeByte_SeqPk1>) BinaryFile.Open(fileName, false))
            {
                f.BinarySearchCacheSize = enableCache ? 0 : -1;

                _DatetimeByte_SeqPk1[] newData =
                    TestUtils.GenerateData(_DatetimeByte_SeqPk1.New, itemCount, 0);

                if (AllowCreate)
                {
                    f.InitializeNewFile();
                    f.AppendData(new ArraySegment<_DatetimeByte_SeqPk1>(newData));
                }

                _DatetimeByte_SeqPk1[] res = f.ReadData(UtcDateTime.MinValue, UtcDateTime.MaxValue, int.MaxValue);
                TestUtils.AreEqual(newData, res);

                TestUtils.CollectionAssertEqual(newData, f.Stream(UtcDateTime.MinValue));

                Array.Reverse(newData);
                TestUtils.CollectionAssertEqual(newData, f.Stream(UtcDateTime.MaxValue, inReverse: true));

                if (itemCount > 0)
                {
                    for (int i = 0; i < Math.Min(repeatRuns, itemCount); i++)
                    {
                        UtcDateTime fromInd = _DatetimeByte_SeqPk1.New(itemCount - i).a;
                        UtcDateTime untilInd = _DatetimeByte_SeqPk1.New(itemCount).a;

                        res = f.ReadData(fromInd, untilInd, int.MaxValue);

                        _DatetimeByte_SeqPk1[] expected = TestUtils.GenerateData(
                            _DatetimeByte_SeqPk1.New, i, itemCount - i);
                        TestUtils.AreEqual(expected, res);

                        List<_DatetimeByte_SeqPk1> res1 = f.Stream(fromInd, untilInd.AddTicks(1)).ToList();
                        TestUtils.CollectionAssertEqual(expected, res1);

                        Array.Reverse(expected);

                        List<_DatetimeByte_SeqPk1> res2 = f.Stream(untilInd, fromInd, inReverse: true).ToList();
                        TestUtils.CollectionAssertEqual(expected, res2);
                    }
                }
            }
        }

        [Test, Ignore]
        public void BasicFunctionality()
        {
            _DatetimeByte_SeqPk1[] newData = TestUtils.GenerateData(
                _DatetimeByte_SeqPk1.New, 10000, 0);

            string fileName = GetBinFileName();
            using (BinSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1> f =
                AllowCreate
                    ? new BinSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>(fileName) {UniqueIndexes = false}
                    : (BinSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>) BinaryFile.Open(fileName, false))
            {
                if (AllowCreate)
                {
                    f.InitializeNewFile();
                    f.AppendData(new ArraySegment<_DatetimeByte_SeqPk1>(newData));
                }

                _DatetimeByte_SeqPk1[] res = f.ReadData(UtcDateTime.MinValue, UtcDateTime.MaxValue, int.MaxValue);
                TestUtils.AreEqual(newData, res);

                //                if (itemCount > 0)
                //                {
                //                    res = f.ReadData(
                //                        _DatetimeByte_SeqPk1.New(itemCount - 1).a,
                //                        _DatetimeByte_SeqPk1.New(itemCount).a,
                //                        int.MaxValue);
                //                    TestUtils.AreEqual(
                //                        TestUtils.GenerateData<_DatetimeByte_SeqPk1>(
                //                            _DatetimeByte_SeqPk1.New, 1, itemCount - 1), res);
                //                }
            }
        }

        [Test, Combinatorial]
        public void VariousLengthNonDuplTimeseries(
            [Values(0, 1, 10, 100, 1000, 10000)] int itemCount,
            [Values(true, false)] bool uniqueTimestamps,
            [Values(true, false)] bool enableCache)
        {
            const int repeatRuns = 10;
            RunTest(itemCount, repeatRuns, uniqueTimestamps, enableCache);
        }
    }
}