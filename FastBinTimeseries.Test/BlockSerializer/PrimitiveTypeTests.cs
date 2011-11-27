using System;
using System.Collections.Generic;
using System.Globalization;
using NUnit.Framework;
using NYurik.FastBinTimeseries.Serializers;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test.BlockSerializer
{
    [TestFixture]
    public class PrimitiveTypeTests : TestsBase
    {
        private IEnumerable<T> Values<T>(Func<long, T> converter, long min, long max)
        {
            if (max - min > ushort.MaxValue || max - min <= 0)
            {
                foreach (long i in StreamCodecTests.TestValuesGenerator())
                    if (i >= min && i <= max)
                        yield return converter(i);
            }
            else
                for (long i = min; i <= max; i++)
                    yield return converter(i);
        }


        private void Run<T>(Func<long, T> converter, string name = null,
                            Action<MultipliedDeltaSerializer> updateSrlzr = null, Func<T, T, bool> comparer = null,
                            long min = long.MinValue, long max = long.MaxValue)
        {
            var codec = new StreamCodec(10000);

            try
            {
                BaseSerializer fldSerializer = FieldsSerializer.GetSerializer(typeof (T));
                if (updateSrlzr != null)
                {
                    var srl = fldSerializer as MultipliedDeltaSerializer;
                    if (srl == null)
                        return;
                    updateSrlzr(srl);
                }

                IEnumerable<T> values = Values(converter, min, max);
                Func<StreamCodec, IEnumerator<T>, bool> serialize =
                    DynamicSerializer<T>.GenerateSerializer(fldSerializer);
                Action<StreamCodec, Buff<T>, int> deserialize = DynamicSerializer<T>.GenerateDeSerializer(fldSerializer);

                TestUtils.CollectionAssertEqual(
                    // ReSharper disable PossibleMultipleEnumeration
                    values, RoundTrip(serialize, deserialize, codec, values),
                    // ReSharper restore PossibleMultipleEnumeration
                    typeof (T).Name + name, comparer);
            }
            catch (Exception x)
            {
                throw new SerializerException(
                    x, "codec.BufferPos={0}, codec.Buffer[pos-1]={1}",
                    codec.BufferPos,
                    codec.BufferPos > 0
                        ? codec.Buffer[codec.BufferPos - 1].ToString(CultureInfo.InvariantCulture)
                        : "n/a");
            }
        }

        private static IEnumerable<T> RoundTrip<T>(
            Func<StreamCodec, IEnumerator<T>, bool> serialize,
            Action<StreamCodec, Buff<T>, int> deserialize,
            StreamCodec codec, IEnumerable<T> values)
        {
            using (IEnumerator<T> enmr = values.GetEnumerator())
            {
                bool moveNext = enmr.MoveNext();
                var buff = new Buff<T>();

                while (moveNext)
                {
                    try
                    {
                        codec.BufferPos = 0;
                        moveNext = serialize(codec, enmr);

                        codec.BufferPos = 0;
                        buff.Reset();
                        deserialize(codec, buff, int.MaxValue);
                    }
                    catch (Exception x)
                    {
                        throw new SerializerException(
                            x, "codec.BufferPos={0}, codec.Buffer[pos-1]={1}, enmr.Value={2}",
                            codec.BufferPos,
                            codec.BufferPos > 0
                                ? codec.Buffer[codec.BufferPos - 1].ToString(CultureInfo.InvariantCulture)
                                : "n/a",
                            moveNext ? enmr.Current.ToString() : "none left");
                    }
                    ArraySegment<T> result = buff.Buffer;
                    for (int i = result.Offset; i < result.Count; i++)
                        yield return result.Array[i];
                }
            }
        }

        [Test]
        public void TypeByte()
        {
            Run(i => (byte) i, min: byte.MinValue, max: byte.MaxValue);
        }

        [Test]
        public void TypeDouble()
        {
            // double: +/- 5.0 x 10-324  to  +/- 1.7 x 10308, 15-16 digits precision

            const int maxDigits = 15;

            var min = (int)(-Math.Pow(10, maxDigits));
            var max = (int)(Math.Pow(10, maxDigits));

            Run(i => (double)i, min: min, max: max);
            Run(
                i => (double)i / 10, "*10", i => i.Multiplier = 10, (x, y) => Math.Abs(x - y) < 0.01,
                min, max);
            Run(
                i => (double)i / 100, "*100", i => i.Multiplier = 100, (x, y) => Math.Abs(x - y) < 0.001,
                min, max);

            // Very large numbers cannot be stored as double
            TestUtils.AssertException<OverflowException>(
                () =>
                Run(
                    i => (double)i,
                    min: (long)(-Math.Pow(10, maxDigits + 3)),
                    max: (long)(-Math.Pow(10, maxDigits + 3) + 10)));
        }

        [Test]
        public void TypeFloat()
        {
            // float: +/- 1.5 x 10-45 to +/- 3.4 x 1038, 7 digits precision
            
            const int maxDigits = 7;

            var min = (int) (-Math.Pow(10, maxDigits));
            var max = (int)(Math.Pow(10, maxDigits));

            Run(i => (float) i, min: min, max: max);
            Run(
                i => (float) i/10, "*10", i => i.Multiplier = 10, (x, y) => Math.Abs(x - y) < 0.01,
                min, max);
            Run(
                i => (float) i/100, "*100", i => i.Multiplier = 100, (x, y) => Math.Abs(x - y) < 0.001,
                min, max);

            // Very large numbers cannot be stored as float
            TestUtils.AssertException<OverflowException>(
                () =>
                Run(
                    i => (float) i,
                    min: (long) (-Math.Pow(10, maxDigits + 3)),
                    max: (long) (-Math.Pow(10, maxDigits + 3) + 10)));
        }

        [Test, Explicit, Category("Long test")]
        public void TypeInt()
        {
            Run(i => (int) i);
            Run(i => (int) i, "/10", i => i.Divider = 10, (x, y) => x/10*10 == y/10*10);
        }

        [Test, Explicit, Category("Long test")]
        public void TypeLong()
        {
            Run(i => i);
            Run(i => i, "/10", i => i.Divider = 10, (x, y) => x/10*10 == y/10*10);
        }

        [Test]
        public void TypeSbyte()
        {
            Run(i => (sbyte) i, min: sbyte.MinValue, max: sbyte.MaxValue);
        }

        [Test]
        public void TypeShort()
        {
            Run(i => (short) i, min: short.MinValue, max: short.MaxValue);
            Run(
                i => (short) i, "/10", i => i.Divider = 10, (x, y) => x/10*10 == y/10*10, short.MinValue,
                short.MaxValue);
        }

        [Test, Explicit, Category("Long test")]
        public void TypeUint()
        {
            Run(i => (uint) i);
            Run(i => (uint) i, "/10", i => i.Divider = 10, (x, y) => x/10*10 == y/10*10);
        }

        [Test, Explicit, Category("Long test")]
        public void TypeUlong()
        {
            Run(i => (ulong) i);
            Run(i => (ulong) i, "/10", i => i.Divider = 10, (x, y) => x/10*10 == y/10*10);
        }

        [Test]
        public void TypeUshort()
        {
            Run(i => (ushort) i, min: ushort.MinValue, max: ushort.MaxValue);
            Run(
                i => (ushort) i, "/10", i => i.Divider = 10, (x, y) => x/10*10 == y/10*10, ushort.MinValue,
                ushort.MaxValue);
        }
    }
}