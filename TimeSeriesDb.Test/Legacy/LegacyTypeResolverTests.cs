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
using NYurik.TimeSeriesDb.CommonCode;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;
using NYurik.TimeSeriesDb.Test.BlockSerializer;

namespace NYurik.TimeSeriesDb.Test.Legacy
{
    [TestFixture]
    [Obsolete("All these tests check loading obsolete types that might have been created during renames")]
    public class LegacyTypeResolverTests : LegacyTestsBase
    {
        private void AssertType<T>(string typeStr)
        {
            Assert.AreEqual(typeof (T), LegacyResolver(typeStr));

            var suffixes =
                new[]
                    {
                        ", Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587",
                        ", mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                        ", Version=1.0.0.0, Culture=neutral, PublicKeyToken=null",
                    };

            // One at a time
            foreach (var sfx in suffixes)
            {
                var t2 = typeStr.Replace(sfx, "");
                Assert.AreEqual(typeof (T), LegacyResolver(t2));
            }

            // Remove all
            foreach (var sfx in suffixes)
                typeStr = typeStr.Replace(sfx, "");
            Assert.AreEqual(typeof (T), LegacyResolver(typeStr));
        }

        [Test]
        public void LegacyTypeResolver()
        {
            AssertType<BinCompressedSeriesFile<_CmplxIdx, _4Flds_ComplxIdx>>(
                "NYurik.FastBinTimeseries.BinCompressedSeriesFile`2[[NYurik.FastBinTimeseries.Test._CmplxIdx, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587],[NYurik.FastBinTimeseries.Test._4Flds_ComplxIdx, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587]], NYurik.FastBinTimeseries, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<BinCompressedSeriesFile<Byte, _3Byte_2Shrt_ExplPk1>>(
                "NYurik.FastBinTimeseries.BinCompressedSeriesFile`2[[System.Byte, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089],[NYurik.FastBinTimeseries.Test._3Byte_2Shrt_ExplPk1, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587]], NYurik.FastBinTimeseries, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<BinIndexedFile<TradesBlock>>(
                "NYurik.FastBinTimeseries.BinIndexedFile`1[[NYurik.FastBinTimeseries.Test.BlockSerializer.TradesBlock, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587]], NYurik.FastBinTimeseries, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<BinIndexedFile<Byte>>(
                "NYurik.FastBinTimeseries.BinIndexedFile`1[[System.Byte, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089]], NYurik.FastBinTimeseries, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<BinSeriesFile<Byte, _3Byte_2Shrt_ExplPk1>>(
                "NYurik.FastBinTimeseries.BinSeriesFile`2[[System.Byte, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089],[NYurik.FastBinTimeseries.Test._3Byte_2Shrt_ExplPk1, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587]], NYurik.FastBinTimeseries, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<BinSeriesFile<Int64, _LongByte_SeqPk1>>(
                "NYurik.FastBinTimeseries.BinSeriesFile`2[[System.Int64, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089],[NYurik.FastBinTimeseries.Test._LongByte_SeqPk1, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587]], NYurik.FastBinTimeseries, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<BinTimeseriesFile<_DatetimeByte_SeqPk1>>(
                "NYurik.FastBinTimeseries.BinTimeseriesFile`1[[NYurik.FastBinTimeseries.Test._DatetimeByte_SeqPk1, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587]], NYurik.FastBinTimeseries.Legacy, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<UtcDateTime>(
                "NYurik.FastBinTimeseries.CommonCode.UtcDateTime, NYurik.FastBinTimeseries, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
            AssertType<UtcDateTime>(
                "NYurik.FastBinTimeseries.CommonCode.UtcDateTime, NYurik.FastBinTimeseries.Legacy, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<DefaultTypeSerializer<TradesBlock.Hdr>>(
                "NYurik.FastBinTimeseries.DefaultTypeSerializer`1[[NYurik.FastBinTimeseries.Test.BlockSerializer.TradesBlock+Hdr, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587]], NYurik.FastBinTimeseries, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<DefaultTypeSerializer<Byte>>(
                "NYurik.FastBinTimeseries.DefaultTypeSerializer`1[[System.Byte, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089]], NYurik.FastBinTimeseries, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<BinBlockSerializer<TradesBlock, TradesBlock.Hdr, TradesBlock.Item>>(
                "NYurik.FastBinTimeseries.Serializers.BlockSerializer.BinBlockSerializer`3[[NYurik.FastBinTimeseries.Test.BlockSerializer.TradesBlock, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587],[NYurik.FastBinTimeseries.Test.BlockSerializer.TradesBlock+Hdr, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587],[NYurik.FastBinTimeseries.Test.BlockSerializer.TradesBlock+Item, NYurik.FastBinTimeseries.Test, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587]], NYurik.FastBinTimeseries, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<SimpleField>(
                "NYurik.FastBinTimeseries.Serializers.BlockSerializer.SimpleField, NYurik.FastBinTimeseries, Version=1.113.0.0, Culture=neutral, PublicKeyToken=e1fb87dd8007f587");
            AssertType<Int32>(
                "System.Int32, mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089");

            // Issue #9 by karl23
            AssertType<UtcDateTime>(
                "NYurik.FastBinTimeseries.CommonCode.UtcDateTime, NYurik.FastBinTimeseries, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null");
        }
    }
}