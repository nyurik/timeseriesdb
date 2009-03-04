using System;
using System.Collections.Generic;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    internal class TestUtils
    {
        public static long RoundUpToMultiple(long value, long multiple)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException("value", value, "Value must be >= 0");
            if (value == 0)
                return 0;
            return value - 1 + (multiple - (value - 1)%multiple);
        }

        public static void AreEqual<T>(T[] expected, T[] values) where T : IEquatable<T>
        {
            Assert.AreEqual(expected.Length, values.Length, "Array lengths");
            for (var i = 0; i < expected.Length; i++)
            {
                if (!expected[i].Equals(values[i]))
                    throw new Exception(
                        String.Format("Items in position {0} is {1}, but expected {2}",
                                      i, values[i], expected[i]));
            }
        }

        public static T[] Concatenate<T>(params T[][] arrays)
        {
            var res = new List<T>();
            foreach (var a in arrays)
                res.AddRange(a);
            return res.ToArray();
        }

        public static T[] GenerateData<T>(Func<long, T> converter, int count, int startFrom)
        {
            var result = new T[count];
            for (long i = 0; i < count; i++)
                result[i] = converter(i + startFrom);
            return result;
        }

        public static byte NewByte(long i)
        {
            return (byte) (i & 0xFF);
        }
    }
}