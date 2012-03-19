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

namespace NYurik.TimeSeriesDb.Test
{
    // ReSharper disable AccessToDisposedClosure

    [TestFixture]
    public class WritableFeedTests : TestsBase
    {
        private void Run<TInd, T>(Func<string, IWritableFeed<TInd, T>> newFile)
            where TInd : IComparable<TInd>
        {
            bool allowsDefaults = typeof (T).IsValueType && typeof (TInd).IsValueType;
            Tuple<Func<long, T>, T> inf = TestUtils.GetObjInfo<T>();

            var minVal = allowsDefaults ? default(T) : inf.Item1(0);
            var val1 = inf.Item1(10);
            var val2 = inf.Item1(20);
            var maxVal = inf.Item2;

            using (var f = Open(newFile))
                Regular(f);

            using (var f = Open(newFile))
                TwoValueTest(f, minVal, maxVal);

            using (var f = Open(newFile))
                TwoValueTest(f, minVal, val1);

            using (var f = Open(newFile))
                TwoValueTest(f, val1, val2);

            using (var f = Open(newFile))
                TwoValueTest(f, val2, maxVal);
        }

        private IWritableFeed<TInd, T> Open<TInd, T>(Func<string, IWritableFeed<TInd, T>> newFile)
            where TInd : IComparable<TInd>
        {
            return AllowCreate
                       ? newFile(GetBinFileName())
                       : (IWritableFeed<TInd, T>) BinaryFile.Open(GetBinFileName(), false, LegacyResolver);
        }

        private void TwoValueTest<TInd, T>(IWritableFeed<TInd, T> f, T itm1, T itm2)
            where TInd : IComparable<TInd>
        {
            var item1 = new[] {new ArraySegment<T>(new[] {itm1})};
            var item2 = new[] {new ArraySegment<T>(new[] {itm2})};

            if (AllowCreate)
            {
                f.AppendData(item1);
                TestUtils.CollectionAssertEqual(item1, f.Stream(), "#1");

                if (f.UniqueIndexes)
                    Assert.Throws<BinaryFileException>(() => f.AppendData(item1), "#2a");
                else
                {
                    f.AppendData(item1);
                    TestUtils.CollectionAssertEqual(Join(item1, item1), f.Stream(), "#2b");

                    f.AppendData(item1, true);
                    TestUtils.CollectionAssertEqual(item1, f.Stream(), "#2c");
                }

                f.AppendData(item2);
                TestUtils.CollectionAssertEqual(Join(item1, item2), f.Stream(), "#3");

                f.AppendData(item2, true);
                TestUtils.CollectionAssertEqual(Join(item1, item2), f.Stream(), "#3a");

                if (f.UniqueIndexes)
                    Assert.Throws<BinaryFileException>(() => f.AppendData(item2), "#4a");
                else
                {
                    f.AppendData(item2);
                    TestUtils.CollectionAssertEqual(Join(item1, item2, item2), f.Stream(), "#4b");
                }

                f.AppendData(item1, true);
            }

            TestUtils.CollectionAssertEqual(item1, f.Stream(), "#final");
        }

        private void Regular<TInd, T>(IWritableFeed<TInd, T> f)
            where TInd : IComparable<TInd>
        {
            if (AllowCreate)
            {
                if (!typeof (T).IsValueType || !typeof (TInd).IsValueType)
                {
                    var dfltItem = new[] {new ArraySegment<T>(new[] {default(T)})};
                    Assert.Throws<BinaryFileException>(() => f.AppendData(dfltItem));
                }

                f.AppendData(Data<T>(10, 20));
                TestUtils.CollectionAssertEqual(Data<T>(10, 20), f.Stream(), "#1");

                Assert.Throws<BinaryFileException>(() => f.AppendData(Data<T>(5, 30)), "#2");
                Assert.Throws<BinaryFileException>(() => f.AppendData(Data<T>(10, 20)), "#2a");
                Assert.Throws<BinaryFileException>(() => f.AppendData(Data<T>(9, 15)), "#2b");
                Assert.Throws<BinaryFileException>(() => f.AppendData(Data<T>(11, 15)), "#2c");
                Assert.Throws<BinaryFileException>(() => f.AppendData(Data<T>(19, 20)), "#2d");
                Assert.Throws<BinaryFileException>(() => f.AppendData(Data<T>(19, 20)), "#2d");

                if (f.UniqueIndexes)
                {
                    Assert.Throws<BinaryFileException>(() => f.AppendData(Data<T>(20, 20)), "#3");
                    Assert.Throws<BinaryFileException>(() => f.AppendData(Data<T>(20, 30)), "#3a");
                }

                f.AppendData(new[] {new ArraySegment<T>(new T[0], 0, 0)});
                TestUtils.CollectionAssertEqual(Data<T>(10, 20), f.Stream(), "#5");

                f.AppendData(new[] {new ArraySegment<T>(new T[1], 1, 0)});
                TestUtils.CollectionAssertEqual(Data<T>(10, 20), f.Stream(), "#6");

                f.AppendData(new ArraySegment<T>[0]);
                TestUtils.CollectionAssertEqual(Data<T>(10, 20), f.Stream(), "#7");

                f.AppendData(Data<T>(10, 14, 2), true);
                TestUtils.CollectionAssertEqual(Data<T>(10, 14, 2), f.Stream(), "#8");

                f.AppendData(Data<T>(10, 13), true);
                TestUtils.CollectionAssertEqual(Data<T>(10, 13), f.Stream(), "#9");

                f.AppendData(Data<T>(14, 14));
                TestUtils.CollectionAssertEqual(Data<T>(10, 14), f.Stream(), "#10");

                f.AppendData(Data<T>(15, 16));
                TestUtils.CollectionAssertEqual(Data<T>(10, 16), f.Stream(), "#11");

                f.AppendData(Data<T>(10, 10), true);
                TestUtils.CollectionAssertEqual(Data<T>(10, 10), f.Stream(), "#12");

                if (!f.UniqueIndexes)
                {
                    f.AppendData(Data<T>(10, 10));
                    TestUtils.CollectionAssertEqual(
                        Join(Data<T>(10, 10), Data<T>(10, 10)), f.Stream(), "#13");

                    f.AppendData(Data<T>(10, 10), true);
                    TestUtils.CollectionAssertEqual(Data<T>(10, 10), f.Stream(), "#14");

                    f.AppendData(Data<T>(10, 11));
                    TestUtils.CollectionAssertEqual(
                        Join(Data<T>(10, 10), Data<T>(10, 11)), f.Stream(), "#15");
                }

                f.AppendData(Data<T>(5, 10), true);
            }

            TestUtils.CollectionAssertEqual(Data<T>(5, 10), f.Stream(), "#final");
        }

        private static IWritableFeed<TInd, T> CreateCompressedFile<TInd, T>(bool isUnique, string fn)
            where TInd : IComparable<TInd>
            where T : IEquatable<T>
        {
            var r = new BinCompressedSeriesFile<TInd, T>(fn) {UniqueIndexes = isUnique};
            r.InitializeNewFile();
            return r;
        }

        private static IWritableFeed<TInd, T> CreateFile<TInd, T>(bool isUnique, string fn)
            where TInd : IComparable<TInd>
            where T : IEquatable<T>
        {
            var r = new BinSeriesFile<TInd, T>(fn) {UniqueIndexes = isUnique};
            r.InitializeNewFile();
            return r;
        }

        [Test]
        public void AppendBinCompressedSeriesFile([Values(true, false)] bool isUnique)
        {
            Run(fn => CreateCompressedFile<byte, _3Byte_2Shrt_ExplPk1>(isUnique, fn));
            Run(fn => CreateCompressedFile<byte, _3Byte_noAttr>(isUnique, fn));
            Run(fn => CreateCompressedFile<long, _BoolLongBool_SeqPk1>(isUnique, fn));
            Run(fn => CreateCompressedFile<long, _ByteLongByte_SeqPk1>(isUnique, fn));
            Run(fn => CreateCompressedFile<int, _IntBool_SeqPk1>(isUnique, fn));
            Run(fn => CreateCompressedFile<long, _LongBool_SeqPk1>(isUnique, fn));
            Run(fn => CreateCompressedFile<long, _LongByte_SeqPk1>(isUnique, fn));

            // fixed buffers are not yet supported
            // Run(fn => CreateCompressedFile<long, _FixedByteBuff7>(isUnique, fn));

            Run(fn => CreateCompressedFile<_CmplxIdx, _4Flds_ComplxIdx>(isUnique, fn));

            Run(fn => CreateCompressedFile<long, _LongBool_Class>(isUnique, fn));
            Run(fn => CreateCompressedFile<_CmplxIdxClass, _4Flds_ComplxIdxClass>(isUnique, fn));
            Run(fn => CreateCompressedFile<_CmplxIdxClass, _4FldsClass_ComplxIdxClass>(isUnique, fn));
        }

        [Test]
        public void AppendBinSeriesFile([Values(true, false)] bool isUnique)
        {
            Run(fn => CreateFile<byte, _3Byte_2Shrt_ExplPk1>(isUnique, fn));
            Run(fn => CreateFile<byte, _3Byte_noAttr>(isUnique, fn));
            Run(fn => CreateFile<long, _BoolLongBool_SeqPk1>(isUnique, fn));
            Run(fn => CreateFile<long, _ByteLongByte_SeqPk1>(isUnique, fn));
            Run(fn => CreateFile<int, _IntBool_SeqPk1>(isUnique, fn));
            Run(fn => CreateFile<long, _LongBool_SeqPk1>(isUnique, fn));
            Run(fn => CreateFile<long, _LongByte_SeqPk1>(isUnique, fn));

            Run(fn => CreateFile<long, _FixedByteBuff7>(isUnique, fn));

            Run(fn => CreateFile<_CmplxIdx, _4Flds_ComplxIdx>(isUnique, fn));
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
                    f.Stream(new _CmplxIdx {a = 0}, new _CmplxIdx {a = 50000}), "#1");

                for (int ix = 0; ix < 5000; ix++)
                    f.AppendData(Data<_4Flds_ComplxIdx>(20000 + 5*ix, 20000 + 5*(ix + 1)));
            }
        }
    }
}