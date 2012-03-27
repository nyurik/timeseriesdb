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
using NYurik.TimeSeriesDb.Serializers;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

// ReSharper disable RedundantTypeArgumentsOfMethod
// ReSharper disable CompareOfFloatsByEqualityOperator

namespace NYurik.TimeSeriesDb.Test.BlockSerializer
{
    [TestFixture]
    public class PrimitiveTypeTests : SerializtionTestsBase
    {
        private static void Set(BaseField i, int mult = 0, int div = 0, double? prec = null)
        {
            var dlt = i as ScaledDeltaFloatField;
            if (dlt != null)
            {
                if (mult != 0)
                    dlt.Multiplier = mult;
                if (div != 0)
                    dlt.Divider = div;
                if (prec != null)
                    dlt.Precision = prec.Value;
            }
            else
            {
                var dltI = (ScaledDeltaIntField) i;
                if (div != 0)
                    dltI.Divider = div;
            }
        }

        private void TestPrecision<T>(T expected, T slightlyUnder, T slightlyOver, double precFail, double precOk, int mult = 0, int div = 0)
        {
            Assert.Throws<SerializerException>(
                () => Run(new[] {slightlyOver}, set: i => Set(i, mult, div, precFail), name: "failOver"));
            Assert.Throws<SerializerException>(
                () => Run(new[] {slightlyUnder}, set: i => Set(i, mult, prec: precFail), name: "failUnder"));
            Run(
                new[] {slightlyOver}, set: i => Set(i, mult, div, precOk), name: "okOver",
                comp: (x, y) => y.Equals(expected));
            Run(
                new[] {slightlyUnder}, set: i => Set(i, mult, div, precOk), name: "okUnder",
                comp: (x, y) => y.Equals(expected));
        }

        [Test]
        public void TypeByte()
        {
            Run(Range<byte>(byte.MinValue, byte.MaxValue, i => (byte)(i + 1)));
        }

        [Test, Explicit, Category("Long test")]
        public void TypeDouble()
        {
            TestPrecision<double>(0.1, 0.09, 0.11, 0.01, 0.1, 10);

            // double: +/- 5.0 x 10-324  to  +/- 1.7 x 10308, 15-16 digits precision
            const int maxDigits = 15;

            var min = (long) (-Math.Pow(10, maxDigits));
            var max = (long) (Math.Pow(10, maxDigits));

            Run(Values(i => (double) i, min, max), "*1", i => Set(i, 1, prec: 0.00001));
            Run(
                Values(i => (double) i/10, min, max), "*10", i => Set(i, 10, prec: 0.1),
                (x, y) => Math.Abs(x - y) < 0.1);
            Run(
                Values(i => (double) i/100, min, max), "*100", i => Set(i, 100, prec: 0.01),
                (x, y) => Math.Abs(x - y) < 0.01);

            // Very large numbers cannot be stored as double
            Assert.Throws<OverflowException>(
                () => Run(
                    Range<double>(-Math.Pow(10, maxDigits + 3), -Math.Pow(10, maxDigits + 3) + 10, i => i + 0.1),
                    "*10 Large Neg", i => Set(i, 10),
                    (x, y) => Math.Abs(x - y) < 0.1));

            Assert.Throws<OverflowException>(
                () =>
                Run(
                    Range<double>(Math.Pow(10, maxDigits + 3), Math.Pow(10, maxDigits + 3) + 10, i => i + 0.1),
                    "*10 Large Pos", i => Set(i, 10),
                    (x, y) => Math.Abs(x - y) < 0.1));
        }

        [Test, Explicit, Category("Long test")]
        public void TypeFloat()
        {
            // float: +/- 1.5 x 10-45 to +/- 3.4 x 1038, 7 digits precision

            const int maxDigits = 7;

            var min = (int) (-Math.Pow(10, maxDigits));
            var max = (int) (Math.Pow(10, maxDigits));

            Run(Values(i => (float) i, min, max), "*1", i => Set(i, 1));
            Run(
                Values(i => (float) i/10, min, max), "*10", i => Set(i, 10),
                (x, y) => Math.Abs(x - y) < 0.1);
            Run(
                Values(i => (float) i/100, min, max), "*100", i => Set(i, 100),
                (x, y) => Math.Abs(x - y) < 0.01);

            // Very large numbers cannot be stored as float
            Assert.Throws<OverflowException>(
                () =>
                Run(
                    Range<float>(
                        (float) -Math.Pow(10, maxDigits + 3), (float) -Math.Pow(10, maxDigits + 3) + 10,
                        i => (float) (i + 0.1)),
                    "*10 Large Neg", i => Set(i, 10),
                    (x, y) => Math.Abs(x - y) < 0.1));

            Assert.Throws<OverflowException>(
                () =>
                Run(
                    Range<float>(
                        (float) Math.Pow(10, maxDigits + 3), (float) Math.Pow(10, maxDigits + 3) + 10,
                        i => (float) (i + 0.1)), "*10 Large Pos", i => Set(i, 10),
                    (x, y) => Math.Abs(x - y) < 0.1));
        }

        [Test, Explicit, Category("Long test")]
        public void TypeInt()
        {
            // 9->0, 
            TestPrecision<int>(100, 99, 101, 1, 10, div: 10);
            Run(Values(i => (int)i));
            Run(
                Values(i => (int) i), "/10", i => Set(i, div: 10),
                (x, y) => x/10*10 == y/10*10);
        }

        [Test, Explicit, Category("Long test")]
        public void TypeLong()
        {
            Run(Values(i => i));
            Run(Values(i => i), "/10", i => Set(i, div: 10), (x, y) => x/10*10 == y/10*10);
        }

        [Test]
        public void TypeSbyte()
        {
            Run(Range<sbyte>(sbyte.MinValue, sbyte.MaxValue, i => (sbyte) (i + 1)));
        }

        [Test]
        public void TypeShort()
        {
            //Run(Range<short>(short.MinValue, short.MaxValue, i => (short) (i + 1)));
            Run(
                Range<short>(short.MinValue + 5, short.MaxValue - 5, i => (short) (i + 1)),
                "/10", i => Set(i, div: 10, prec:5),
                (x, y) => Math.Round(x/10.0)*10 == y);
        }

        [Test, Explicit, Category("Long test")]
        public void TypeUint()
        {
            Run(Values(i => (uint) i));
            Run(
                Values(i => (uint) i), "/10", i => Set(i, div: 10),
                (x, y) => x/10*10 == y/10*10);
        }

        [Test, Explicit, Category("Long test")]
        public void TypeUlong()
        {
            Run(Values(i => (ulong) i));
            Run(
                Values(i => (ulong) i), "/10", i => Set(i, div: 10),
                (x, y) => x/10*10 == y/10*10);
        }

        [Test]
        public void TypeUshort()
        {
            Run(Range<ushort>(ushort.MinValue, ushort.MaxValue, i => (ushort) (i + 1)));
            Run(
                Range<short>(short.MinValue, short.MaxValue, i => (short) (i + 1)),
                "/10", i => Set(i, div: 10), (x, y) => x/10*10 == y/10*10);
        }

        [Test, Explicit, Category("Long test")]
        public void TypeUtcDateTime()
        {
            Run(Range<UtcDateTime>(UtcDateTime.MinValue, UtcDateTime.MinValue.AddSeconds(.5), i => i.AddTicks(1)));
            Run(Range<UtcDateTime>(UtcDateTime.MaxValue.AddSeconds(-.5), UtcDateTime.MaxValue, i => i.AddTicks(1)));
            Run(
                Range<UtcDateTime>(new UtcDateTime(2011, 1, 1), new UtcDateTime(2011, 2, 1), i => i.AddHours(1)),
                "Each hour",
                i => ((UtcDateTimeField) i).TimeDivider = TimeSpan.FromHours(1));
        }
    }
}