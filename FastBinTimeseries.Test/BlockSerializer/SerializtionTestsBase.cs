using System;
using System.Collections.Generic;
using System.Globalization;
using NUnit.Framework;
using NYurik.FastBinTimeseries.Serializers;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test.BlockSerializer
{
    [TestFixture]
    public class SerializtionTestsBase : TestsBase
    {
        public IEnumerable<T> Values<T>(Func<long, T> converter, long min = long.MinValue, long max = long.MaxValue)
        {
            foreach (long i in StreamCodecTests.TestValuesGenerator())
                if (i >= min && i <= max)
                    yield return converter(i);
        }

        public IEnumerable<T> Range<T>(T min, T max, Func<T, T> inc)
            where T : IComparable<T>
        {
            T val = min;
            yield return val;

            while (val.CompareTo(max) < 0)
            {
                val = inc(val);
                yield return val;
            }
        }


        public void Run<T>(IEnumerable<T> values, string name = null,
                           Action<BaseField> updateSrlzr = null, Func<T, T, bool> comparer = null)
        {
            var codec = new CodecWriter(10000);
            var ds = DynamicSerializer<T>.CreateDefault();

            try
            {
                if (updateSrlzr != null)
                    updateSrlzr(ds.RootField);

                ds.MakeReadonly();

                TestUtils.CollectionAssertEqual(
                    // ReSharper disable PossibleMultipleEnumeration
                    values, RoundTrip(ds, codec, values),
                    // ReSharper restore PossibleMultipleEnumeration
                    typeof (T).Name + name, comparer);
            }
            catch (Exception x)
            {
                string msg = string.Format(
                    "codec.BufferPos={0}, codec.Buffer[pos-1]={1}",
                    codec.BufferPos,
                    codec.BufferPos > 0
                        ? codec.Buffer[codec.BufferPos - 1].ToString(CultureInfo.InvariantCulture)
                        : "n/a");
                if (x.GetType() == typeof (OverflowException))
                    throw new OverflowException(msg, x);

                throw new SerializerException(x, msg);
            }
        }

        private static IEnumerable<T> RoundTrip<T>(DynamicSerializer<T> ds, CodecWriter codec, IEnumerable<T> values)
        {
            using (IEnumerator<T> enmr = values.GetEnumerator())
            {
                bool moveNext = enmr.MoveNext();
                var buff = new Buffer<T>(new T[4]);

                while (moveNext)
                {
                    try
                    {
                        codec.BufferPos = 0;
                        moveNext = ds.Serialize(codec, enmr);

                        codec.BufferPos = 0;
                        buff.Count = 0;
                        ds.DeSerialize(new CodecReader(codec.UsedBuffer), buff, int.MaxValue);
                    }
                    catch (Exception x)
                    {
                        string msg = string.Format(
                            "codec.BufferPos={0}, codec.Buffer[pos-1]={1}, enmr.Value={2}",
                            codec.BufferPos,
                            codec.BufferPos > 0
                                ? codec.Buffer[codec.BufferPos - 1].ToString(CultureInfo.InvariantCulture)
                                : "n/a",
                            moveNext ? enmr.Current.ToString() : "none left");

                        if (x.GetType() == typeof (OverflowException))
                            throw new OverflowException(msg, x);

                        throw new SerializerException(x, msg);
                    }
                    ArraySegment<T> result = buff.AsArraySegment;
                    for (int i = result.Offset; i < result.Count; i++)
                        yield return result.Array[i];
                }
            }
        }
    }
}