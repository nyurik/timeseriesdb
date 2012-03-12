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
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test.Legacy
{
    // ReSharper disable PossibleMultipleEnumeration

    [TestFixture, Explicit("Slow tests, some not working")]
    [Obsolete]
    public class CompressedFileTests : LegacyTestsBase
    {
        private readonly _DatetimeByte_SeqPk1[] _empty = new _DatetimeByte_SeqPk1[0];

        private void Run(string name, int itemCount,
                         Func<string, IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>> newFile,
                         Action<IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>> update,
                         Action<IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>> init)
        {
            string fileName = GetBinFileName();

            IEnumerable<ArraySegment<_DatetimeByte_SeqPk1>> newData = Data(itemCount, 0, itemCount);
            List<_DatetimeByte_SeqPk1> expected = newData.StreamSegmentValues().ToList();
            Assert.AreEqual(itemCount, expected.Count);
            _DatetimeByte_SeqPk1[] expectedRev = expected.ToArray();
            Array.Reverse(expectedRev);
            IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>
                f = !AllowCreate
                        ? (IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>)
                          BinaryFile.Open(fileName, false, LegacySupport.TypeResolver)
                        : newFile(fileName);
            try
            {
                if (update != null)
                    update(f);

                if (AllowCreate)
                {
                    init(f);
                    f.AppendData(newData);
                    f.Dispose();
                    f =
                        (IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>)
                        BinaryFile.Open(fileName, false, LegacySupport.TypeResolver);
                }

                TestUtils.CollectionAssertEqual(
                    _empty, f.Stream(UtcDateTime.MinValue, inReverse: true),
                    "nothing before 0 {0}", name);

                TestUtils.CollectionAssertEqual(
                    _empty, f.Stream(UtcDateTime.MaxValue),
                    "nothing after max {0}", name);

                if (itemCount <= 0)
                {
                    Assert.IsTrue(f.IsEmpty, "IsEmpty {0}", name);
                    Assert.AreEqual(default(UtcDateTime), f.FirstIndex, "default FirstInd {0}", name);
                    Assert.AreEqual(default(UtcDateTime), f.LastIndex, "default LastInd {0}", name);
                    TestUtils.CollectionAssertEqual(_empty, f.Stream(UtcDateTime.MinValue), "empty forward {0}", name);
                    TestUtils.CollectionAssertEqual(
                        _empty, f.Stream(UtcDateTime.MinValue, inReverse: true), "empty backward {0}", name);
                    return;
                }

                Assert.IsFalse(f.IsEmpty, "!IsEmpty {0}", name);

                Assert.AreEqual(expected[0].a, f.FirstIndex, name + " first");
                Assert.AreEqual(expected[itemCount - 1].a, f.LastIndex, "last {0}", name);

                TestUtils.CollectionAssertEqual(expected, f.Stream(UtcDateTime.MinValue), "full forward {0}", name);
                TestUtils.CollectionAssertEqual(
                    expectedRev, f.Stream(UtcDateTime.MaxValue, inReverse: true), "full backward {0}", name);

                const int skipStart = 0;
                const int takeStart = 0;

                const int maxSkipCount = 50;

                int maxSkip = Math.Min(maxSkipCount, itemCount);
                for (int skip = skipStart; skip < maxSkip; skip++)
                {
                    int maxTake = Math.Min(maxSkipCount, itemCount - maxSkip + 1);
                    for (int take = takeStart; take < maxTake; take++)
                    {
                        TestUtils.CollectionAssertEqual(
                            expected.Skip(skip).Take(take), f.Stream(expected[skip].a, maxItemCount: take),
                            "skip {1} take {2} {0}", name, skip, take);

                        if (itemCount < take)
                            TestUtils.CollectionAssertEqual(
                                expected.Skip(skip).Take(take - 1),
                                f.Stream(expected[skip].a.AddSeconds(1), maxItemCount: take - 1),
                                "next tick skip {1} take ({2}-1) {0}", name, skip, take);

                        if (itemCount < skip)
                        {
                            TestUtils.CollectionAssertEqual(
                                expectedRev.Skip(skip - 1).Take(take),
                                f.Stream(expectedRev[skip].a, maxItemCount: take, inReverse: true),
                                "backward, existing item, skip {1} take {2} {0}", name, skip, take);

                            TestUtils.CollectionAssertEqual(
                                expectedRev.Skip(skip - 1).Take(take),
                                f.Stream(expectedRev[skip].a.AddSeconds(-1), maxItemCount: take, inReverse: true),
                                "backward, non-existing, skip {1} take {2} {0}", name, skip, take);
                        }
                    }
                }
            }
            finally
            {
                f.Dispose();
            }
        }

        private void AppendTest(string name, int segSize, int itemCount,
                                Func<int, int, int, IEnumerable<ArraySegment<_DatetimeByte_SeqPk1>>> data,
                                Func<string, IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>> newFile,
                                Action<IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>> update,
                                Action<IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>> init)
        {
            if (RunMode != Mode.OneTime)
                Assert.Inconclusive("RunMode={0} is not supported", RunMode);

            string fileName = GetBinFileName();
            using (IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1> f = newFile(fileName))
            {
                if (update != null)
                    update(f);

                init(f);

                TestUtils.CollectionAssertEqual(
                    _empty, f.Stream(UtcDateTime.MinValue),
                    "initial empty {0}", name);

                int lastStep = 0;
                foreach (int step in new[] {itemCount/10, itemCount/5, itemCount/3, itemCount})
                {
                    var lastBefore = f.LastIndex;
                    f.AppendData(data(segSize, lastStep, step));
                    var lastAfter = f.LastIndex;

                    if (step > 0)
                        Assert.AreNotEqual(lastBefore, lastAfter);

                    TestUtils.CollectionAssertEqual(
                        data(itemCount, 0, step),
                        f.Stream(UtcDateTime.MinValue),
                        "adding {0} to {1} {2}", lastStep, step, name);

                    lastStep = step; // +(f.UniqueIndexes ? 0 : 1);
                }

                int halfItemCnt = itemCount/2;
                if (itemCount >= 2)
                {
                    f.AppendData(data(segSize, 0, halfItemCnt), true);
                    TestUtils.CollectionAssertEqual(
                        data(segSize, 0, halfItemCnt), f.Stream(UtcDateTime.MinValue),
                        "nothing before 0 {0}", name);
                }

                if (halfItemCnt > 2)
                {
                    // ReSharper disable AccessToDisposedClosure
                    TestUtils.AssertException<BinaryFileException>(
                        () => f.AppendData(data(segSize, halfItemCnt - 1, halfItemCnt)));
                    TestUtils.AssertException<BinaryFileException>(
                        () => f.AppendData(data(segSize, halfItemCnt - 1, halfItemCnt + 1)));
                    TestUtils.AssertException<BinaryFileException>(
                        () => f.AppendData(data(segSize, halfItemCnt - 2, halfItemCnt - 1)));
                    TestUtils.AssertException<BinaryFileException>(
                        () => f.AppendData(data(segSize, halfItemCnt - 2, halfItemCnt + 1)));
                    // ReSharper restore AccessToDisposedClosure
                }
            }
        }

        private IEnumerable<T> DuplicatesEvery<T>(long start, long until, long duplEvery)
        {
            int cnt = 0;
            var newObj = TestUtils.GetObjFactory<T>();
            for (long i = start; i < until; i++)
            {
                T v = newObj(i);
                yield return v;
                if (++cnt%duplEvery == 0)
                    yield return v;
            }
        }

        // ReSharper disable FunctionNeverReturns

        private IEnumerable<int> SameValue(int value)
        {
            while (true)
                yield return value;
        }

        // ReSharper restore FunctionNeverReturns

        private IEnumerable<ArraySegment<T>> Segments<T>(IEnumerable<T> values, IEnumerable<int> segSizes)
        {
            using (IEnumerator<T> vals = values.GetEnumerator())
            {
                T[] array = null;
                foreach (int count in segSizes)
                {
                    if (array == null || array.Length < count)
                        array = new T[count];

                    int i;
                    for (i = 0; i < count; i++)
                    {
                        if (!vals.MoveNext())
                            yield break;
                        array[i] = vals.Current;
                    }

                    yield return new ArraySegment<T>(array, 0, i);
                }
            }
        }

        private void AppendDuplTest(string name, int segSize,
                                    Func<string, IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>> newFile,
                                    Action<IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>> update,
                                    Action<IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1>> init)
        {
            if (RunMode != Mode.OneTime)
                Assert.Inconclusive("RunMode={0} is not supported", RunMode);

            string fileName = GetBinFileName();
            using (IWritableFeed<UtcDateTime, _DatetimeByte_SeqPk1> f = newFile(fileName))
            {
                if (update != null)
                    update(f);

                init(f);

                IEnumerable<ArraySegment<_DatetimeByte_SeqPk1>> dat =
                    Segments(DuplicatesEvery<_DatetimeByte_SeqPk1>(0, 2, 1), SameValue(segSize));

                f.AppendData(dat);

                TestUtils.CollectionAssertEqual(
                    dat, f.Stream(UtcDateTime.MinValue),
                    "adding dupl start={0}, until={1}, duplEvery={2}, fixedSegments={3} {4}", 0, 2, 1, segSize,
                    name);
            }
        }

        private static IEnumerable<ArraySegment<_DatetimeByte_SeqPk1>> Data(int segSize, int minValue, int maxValue)
        {
            return TestUtils.GenerateDataStream<_DatetimeByte_SeqPk1>(segSize, minValue, maxValue);
        }

        private static IEnumerable<ArraySegment<_DatetimeByte_SeqPk1>> DataDupl(int segSize, int minValue, int maxValue)
        {
            return TestUtils.GenerateDataStream<_DatetimeByte_SeqPk1>(segSize, minValue, maxValue);
        }

//        static void Main(string[] args)
//        {
//            var t = new CompressedFileTests();
//            t.Cleanup();
//            t.AppendTest(10, 10, 20, false, false);
//            t.Cleanup();
//        }
//

        [Test]
        public void AppendDuplTest(
            [Values(1, 2, 3, 5, 10, 100)] int segSize,
            [Values(0, 1, 2, 3, 11, 200, 10000)] int blockSizeExtra,
            [Values(true, false)] bool enableCache)
        {
            AppendDuplTest(
                string.Format(
                    "in segSize={0}, blockSizeExtra={1}, cache={2}",
                    segSize, blockSizeExtra, enableCache),
                segSize,
                fileName =>
                    {
                        var bf = new BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>(fileName) {UniqueIndexes = false};

                        bf.BlockSize = bf.FieldSerializer.RootField.GetMaxByteSize() + CodecBase.ReservedSpace
                                       + blockSizeExtra;
                        return bf;
                    },
                f => ((BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>) f).BinarySearchCacheSize = enableCache ? 0 : -1,
                f => ((BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>) f).InitializeNewFile());
        }

        [Test]
        public void AppendTest(
            [Values(1, 2, 3, 5, 10, 100)] int segSize,
            [Values(0, 1, 2, 3, 4, 5, 10, 100, 10000)] int itemCount,
            [Values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 200, 10000)] int blockSizeExtra,
            [Values(true, false)] bool enableCache,
            [Values(true, false)] bool uniqueIndexes)
        {
            Func<int, int, int, IEnumerable<ArraySegment<_DatetimeByte_SeqPk1>>> data =
                uniqueIndexes
                    ? Data
                    : (Func<int, int, int, IEnumerable<ArraySegment<_DatetimeByte_SeqPk1>>>) DataDupl;

            AppendTest(
                string.Format(
                    "in segSize={0}, itemCount={1}, blockSizeExtra={2}, cache={3}, uniqueIndexes={4}", segSize,
                    itemCount, blockSizeExtra,
                    enableCache, uniqueIndexes),
                segSize, itemCount, data,
                fileName =>
                    {
                        var bf = new BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>(fileName) {UniqueIndexes = uniqueIndexes};

                        bf.BlockSize = bf.FieldSerializer.RootField.GetMaxByteSize() + CodecBase.ReservedSpace
                                       + blockSizeExtra;
                        return bf;
                    },
                f => ((BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>) f).BinarySearchCacheSize = enableCache ? 0 : -1,
                f => ((BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>) f).InitializeNewFile());
        }

        [Test, Combinatorial]
        public void CompressedGeneralTest(
            [Values(0, 1, 2, 3, 4, 5, 10, 100, 10000)] int itemCount,
            [Values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 200, 10000)] int blockSizeExtra,
            [Values(true, false)] bool enableCache)
        {
            Run(
                string.Format(
                    "in itemCount={0}, blockSizeExtra={1}, cache={2}", itemCount, blockSizeExtra, enableCache),
                itemCount,
                fileName =>
                    {
                        var bf = new BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>(fileName) {UniqueIndexes = true};

                        bf.BlockSize = bf.FieldSerializer.RootField.GetMaxByteSize() + CodecBase.ReservedSpace
                                       + blockSizeExtra;
                        bf.ValidateOnRead = true;
                        return bf;
                    },
                f => ((BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>) f).BinarySearchCacheSize = enableCache ? 0 : -1,
                f => ((BinCompressedSeriesFile<UtcDateTime, _DatetimeByte_SeqPk1>) f).InitializeNewFile());
        }

        [Test, Combinatorial]
        public void UncompressedGeneralTimeseries(
            [Values(0, 1, 2, 3, 4, 5, 10, 100, 10000)] int itemCount,
            [Values(true, false)] bool enableCache)
        {
            Run(
                string.Format("in itemCount={0}, cache={1}", itemCount, enableCache),
                itemCount,
                fileName => new BinTimeseriesFile<_DatetimeByte_SeqPk1>(fileName) {UniqueIndexes = true},
                f => ((BinTimeseriesFile<_DatetimeByte_SeqPk1>) f).BinarySearchCacheSize = enableCache ? 0 : -1,
                f => ((BinTimeseriesFile<_DatetimeByte_SeqPk1>) f).InitializeNewFile());
        }
    }
}