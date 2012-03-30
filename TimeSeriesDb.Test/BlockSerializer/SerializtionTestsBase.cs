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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using NYurik.TimeSeriesDb.Serializers;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

// ReSharper disable PossibleMultipleEnumeration

namespace NYurik.TimeSeriesDb.Test.BlockSerializer
{
    public class SerializtionTestsBase : TestsBase
    {
        /// <summary>
        /// Yields values T based on the TestValuesGenerator within a range
        /// </summary>
        protected IEnumerable<T> Values<T>(Func<long, T> converter, long min = long.MinValue, long max = long.MaxValue)
        {
            IEnumerable<ulong> values = StreamCodecTests.TestValuesGenerator();
            return min != long.MinValue || max != long.MaxValue
                       ? YieldLimited(converter, min, max, values)
                       : values.Select(i => converter((long) i));
        }

        private static IEnumerable<T> YieldLimited<T>(Func<long, T> converter, long min, long max,
                                                      IEnumerable<ulong> valGenerator)
        {
            foreach (long i in valGenerator)
                if (i >= min && i <= max)
                    yield return converter(i);
        }

        /// <summary>
        /// Yields values T starting at min, and ending with max (inclusive), using the incrementing method
        /// </summary>
        protected IEnumerable<T> Range<T>(T min, T max, Func<T, T> inc)
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

        /// <summary>
        /// Perform a round trip encoding/decoding test for the given sequence of values.
        /// </summary>
        protected void Run<T>(IEnumerable<T> values, string name = null,
                              Action<BaseField> set = null, Func<T, T, bool> comp = null)
        {
            using (var codec = new CodecWriter(10000))
            {
                var ds = new DynamicSerializer<T>(null);

                try
                {
                    if (set != null)
                        set(ds.RootField);

                    ds.MakeReadonly();

                    TestUtils.CollectionAssertEqual(
                        values, RoundTrip(ds, codec, values), comp, "{0} {1}", typeof (T).Name, name);
                }
                catch (Exception x)
                {
                    string msg = string.Format(
                        "Name={0}, codec.Count={1}, codec.Buffer[pos-1]={2}",
                        name,
                        codec.Count,
                        codec.Count > 0
                            ? codec.Buffer[codec.Count - 1].ToString(CultureInfo.InvariantCulture)
                            : "n/a");
                    if (x.GetType() == typeof (OverflowException))
                        throw new OverflowException(msg, x);

                    throw new SerializerException(x, msg);
                }
            }
        }

        /// <summary>
        /// Encode all values using the given serializer, and than decode them back.
        /// </summary>
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
                        codec.Count = 0;
                        moveNext = ds.Serialize(codec, enmr);

                        codec.Count = 0;
                        buff.Count = 0;
                        using (var cdcRdr = new CodecReader(codec.AsArraySegment()))
                            ds.DeSerialize(cdcRdr, buff, int.MaxValue);
                    }
                    catch (Exception x)
                    {
                        string msg = string.Format(
                            "codec.Count={0}, codec.Buffer[pos-1]={1}, enmr.Value={2}",
                            codec.Count,
                            codec.Count > 0
                                ? codec.Buffer[codec.Count - 1].ToString(CultureInfo.InvariantCulture)
                                : "n/a",
                            moveNext ? enmr.Current.ToString() : "none left");

                        if (x.GetType() == typeof (OverflowException))
                            throw new OverflowException(msg, x);

                        throw new SerializerException(x, msg);
                    }

                    ArraySegment<T> result = buff.AsArraySegment();
                    for (int i = result.Offset; i < result.Count; i++)
                        yield return result.Array[i];
                }
            }
        }
    }
}