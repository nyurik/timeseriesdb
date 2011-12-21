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
using NUnit.Framework;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test.BlockSerializer
{
    [TestFixture]
    public class PrimitiveTypeTests : SerializtionTestsBase
    {
        [Test]
        public void TypeByte()
        {
            Run(Range(byte.MinValue, byte.MaxValue, i => (byte) (i + 1)));
        }

        [Test, Explicit, Category("Long test")]
        public void TypeDouble()
        {
            // double: +/- 5.0 x 10-324  to  +/- 1.7 x 10308, 15-16 digits precision

            const int maxDigits = 15;

            var min = (int) (-Math.Pow(10, maxDigits));
            var max = (int) (Math.Pow(10, maxDigits));

            Run(Values(i => (double) i, min, max), "*1", i => ((ScaledDeltaField) i).Multiplier = 1);
            Run(
                Values(i => (double) i/10, min, max), "*10", i => ((ScaledDeltaField) i).Multiplier = 10,
                (x, y) => Math.Abs(x - y) < 0.1);
            Run(
                Values(i => (double) i/100, min, max), "*100", i => ((ScaledDeltaField) i).Multiplier = 100,
                (x, y) => Math.Abs(x - y) < 0.01);

            // Very large numbers cannot be stored as double
            TestUtils.AssertException<OverflowException>(
                () =>
                Run(
                    Range(-Math.Pow(10, maxDigits + 3), -Math.Pow(10, maxDigits + 3) + 10, i => i + 0.1),
                    "*10 Large Neg", i => ((ScaledDeltaField) i).Multiplier = 10,
                    (x, y) => Math.Abs(x - y) < 0.1));

            TestUtils.AssertException<OverflowException>(
                () =>
                Run(
                    Range(Math.Pow(10, maxDigits + 3), Math.Pow(10, maxDigits + 3) + 10, i => i + 0.1),
                    "*10 Large Pos", i => ((ScaledDeltaField) i).Multiplier = 10,
                    (x, y) => Math.Abs(x - y) < 0.1));
        }

        [Test, Explicit, Category("Long test")]
        public void TypeFloat()
        {
            // float: +/- 1.5 x 10-45 to +/- 3.4 x 1038, 7 digits precision

            const int maxDigits = 7;

            var min = (int) (-Math.Pow(10, maxDigits));
            var max = (int) (Math.Pow(10, maxDigits));

            Run(Values(i => (float) i, min, max), "*1", i => ((ScaledDeltaField) i).Multiplier = 1);
            Run(
                Values(i => (float) i/10, min, max), "*10", i => ((ScaledDeltaField) i).Multiplier = 10,
                (x, y) => Math.Abs(x - y) < 0.1);
            Run(
                Values(i => (float) i/100, min, max), "*100", i => ((ScaledDeltaField) i).Multiplier = 100,
                (x, y) => Math.Abs(x - y) < 0.01);

            // Very large numbers cannot be stored as float
            TestUtils.AssertException<OverflowException>(
                () =>
                Run(
                    Range(
                        (float) -Math.Pow(10, maxDigits + 3), (float) -Math.Pow(10, maxDigits + 3) + 10,
                        i => (float) (i + 0.1)),
                    "*10 Large Neg", i => ((ScaledDeltaField) i).Multiplier = 10,
                    (x, y) => Math.Abs(x - y) < 0.1));

            TestUtils.AssertException<OverflowException>(
                () =>
                Run(
                    Range(
                        (float) Math.Pow(10, maxDigits + 3), (float) Math.Pow(10, maxDigits + 3) + 10,
                        i => (float) (i + 0.1)), "*10 Large Pos", i => ((ScaledDeltaField) i).Multiplier = 10,
                    (x, y) => Math.Abs(x - y) < 0.1));
        }

        [Test, Explicit, Category("Long test")]
        public void TypeInt()
        {
            Run(Values(i => (int) i));
            Run(
                Values(i => (int) i), "/10", i => ((ScaledDeltaField) i).Divider = 10,
                (x, y) => x/10*10 == y/10*10);
        }

        [Test, Explicit, Category("Long test")]
        public void TypeLong()
        {
            Run(Values(i => i));
            Run(Values(i => i), "/10", i => ((ScaledDeltaField) i).Divider = 10, (x, y) => x/10*10 == y/10*10);
        }

        [Test]
        public void TypeSbyte()
        {
            Run(Range(sbyte.MinValue, sbyte.MaxValue, i => (sbyte) (i + 1)));
        }

        [Test]
        public void TypeShort()
        {
            Run(Range(short.MinValue, short.MaxValue, i => (short) (i + 1)));
            Run(
                Range(short.MinValue, short.MaxValue, i => (short) (i + 1)), "/10",
                i => ((ScaledDeltaField) i).Divider = 10,
                (x, y) => x/10*10 == y/10*10);
        }

        [Test, Explicit, Category("Long test")]
        public void TypeUint()
        {
            Run(Values(i => (uint) i));
            Run(
                Values(i => (uint) i), "/10", i => ((ScaledDeltaField) i).Divider = 10,
                (x, y) => x/10*10 == y/10*10);
        }

        [Test, Explicit, Category("Long test")]
        public void TypeUlong()
        {
            Run(Values(i => (ulong) i));
            Run(
                Values(i => (ulong) i), "/10", i => ((ScaledDeltaField) i).Divider = 10,
                (x, y) => x/10*10 == y/10*10);
        }

        [Test]
        public void TypeUshort()
        {
            Run(Range(ushort.MinValue, ushort.MaxValue, i => (ushort) (i + 1)));
            Run(
                Range(short.MinValue, short.MaxValue, i => (short) (i + 1)),
                "/10", i => ((ScaledDeltaField) i).Divider = 10, (x, y) => x/10*10 == y/10*10);
        }

        [Test]
        public void TypeUtcDateTime()
        {
            Run(Range(UtcDateTime.MinValue, UtcDateTime.MinValue.AddSeconds(.5), i => i.AddTicks(1)));
            Run(Range(UtcDateTime.MaxValue.AddSeconds(-.5), UtcDateTime.MaxValue, i => i.AddTicks(1)));
            Run(
                Range(new UtcDateTime(2011, 1, 1), new UtcDateTime(2011, 2, 1), i => i.AddHours(1)), "Each hour",
                i => ((UtcDateTimeField) i).TimeDivider = TimeSpan.FromHours(1));
        }
    }
}