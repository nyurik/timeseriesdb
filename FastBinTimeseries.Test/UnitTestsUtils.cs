using System;
using System.Collections.Generic;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    internal class UnitTestsUtils
    {
        public static DateTime FirstTimeStamp = new DateTime(2000, 1, 1);

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

        public static byte CreateByte(long i)
        {
            return (byte) (i & 0xFF);
        }

        public static Struct3Byte CreateStruct3(long i)
        {
            return new Struct3Byte(
                (byte) ((i & 0xFF0000) >> 16), (byte) ((i & 0xFF00) >> 8),
                (byte) (i & 0xFF));
        }

        public static StructTimeValue CreateStructTimeValue(long i)
        {
            return new StructTimeValue((byte) (i & 0xFF), FirstTimeStamp.AddMinutes(i));
        }

        public static Struct3ByteUnion CreateStruct3Union(long i)
        {
            return new Struct3ByteUnion(
                (byte) ((i & 0xFF0000) >> 16), (byte) ((i & 0xFF00) >> 8),
                (byte) (i & 0xFF));
        }
    }
}