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
using JetBrains.Annotations;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    internal static class TestUtils
    {
        private static readonly LinkedList<CacheItem> Items = new LinkedList<CacheItem>();

        public static long RoundUpToMultiple(long value, long multiple)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException("value", value, "Value must be >= 0");
            if (value == 0)
                return 0;
            return value - 1 + (multiple - (value - 1)%multiple);
        }

        public static void AreNotEqual<T>(T[] expected, T[] values, string description)
        {
            AreNotEqual(new ArraySegment<T>(expected), new ArraySegment<T>(values), description);
        }

        public static void AreNotEqual<T>(ArraySegment<T> expected, ArraySegment<T> values)
        {
            AreNotEqual(expected, values, null);
        }

        public static void AreNotEqual<T>(ArraySegment<T> expected, ArraySegment<T> values, string description)
        {
            var s = new DefaultTypeSerializer<T>();
            Assert.IsFalse(s.BinaryArrayCompare(expected, values), description);
        }

        public static void AreEqual<T>(T[] expected, T[] values)
        {
            AreEqual(expected, values, null);
        }

        public static void AreEqual<T>(T[] expected, T[] values, string description)
        {
            AreEqual(new ArraySegment<T>(expected), new ArraySegment<T>(values), description);
        }

        public static void AreEqual<T>(ArraySegment<T> expected, ArraySegment<T> values)
        {
            AreEqual(expected, values, null);
        }

        public static void AreEqual<T>(ArraySegment<T> expected, ArraySegment<T> values, string description)
        {
            var s = new DefaultTypeSerializer<T>();
            Assert.IsTrue(s.BinaryArrayCompare(expected, values), description);
        }

        public static T[] Concatenate<T>(params T[][] arrays)
        {
            var res = new List<T>();
            foreach (var a in arrays)
                res.AddRange(a);
            return res.ToArray();
        }

        public static T[] GenerateData<T>(Func<long, T> converter, int count, int startFrom, int step = 1)
        {
            string key = string.Format("{0},{1},{2},{3}", typeof (T).FullName, startFrom, converter, step);

            T[] result;
            LinkedListNode<CacheItem> res = Items.Find(new CacheItem {Key = key});
            if (res != null)
            {
                Items.Remove(res);

                result = (T[]) res.Value.Value;
                if (result.Length >= count)
                {
                    if (result.Length > count)
                    {
                        T[] rOld = result;
                        result = new T[count];
                        Array.Copy(rOld, result, count);
                    }

                    Items.AddFirst(res);
                    return result;
                }
            }

            result = new T[count + 100];
            for (long i = 0; i < count + 100; i++)
                result[i] = converter(startFrom + step*i);

            Items.AddFirst(new CacheItem {Key = key, Value = result});
            if (Items.Count > 100)
                Items.RemoveLast();

            var rNew = new T[count];
            Array.Copy(result, rNew, count);
            return rNew;
        }

        public static IEnumerable<ArraySegment<T>> GenerateDataStream<T>(
            Func<long, T> converter, int segSize, int minValue, int maxValue, int step = 1)
        {
            if (segSize <= 0)
                yield break;

            for (long i = minValue; i < maxValue; i += segSize)
                yield return
                    new ArraySegment<T>(GenerateData(converter, (int) Math.Min(segSize, maxValue - i), (int) i, step));
        }

        public static byte NewByte(long i)
        {
            return (byte) (i & 0xFF);
        }

        [StringFormatMethod("format")]
        public static void AssertException<TEx>(Action operation, string format = null, params object[] args)
            where TEx : Exception
        {
            AssertException<TEx>(
                () =>
                    {
                        operation();
                        return null;
                    }, format, args);
        }

        [StringFormatMethod("format")]
        public static void AssertException<TEx>(Func<object> operation, string format = null, params object[] args)
            where TEx : Exception
        {
            try
            {
                object o = operation();
                string fmt = format == null ? "" : string.Format(format, args) + ": ";
                Assert.Fail(
                    "{0}Should have thrown {1}, but instead completed with result {2}", fmt, typeof (TEx).Name, o);
            }
            catch (TEx)
            {
                // Console.WriteLine("Successfully cought {0}: {1}", typeof (TEx).Name, ex.Message);
            }
        }

        [StringFormatMethod("format")]
        public static void CollectionAssertEqual<T>(
            IEnumerable<ArraySegment<T>> expected, IEnumerable<T> actual,
            string format = null, params object[] args)
        {
            CollectionAssertEqual(expected.StreamSegmentValues(), actual, null, format, args);
        }

        [StringFormatMethod("format")]
        public static void CollectionAssertEqual<T>(
            IEnumerable<T> expected, IEnumerable<T> actual,
            string format = null, params object[] args)
        {
            CollectionAssertEqual(expected, actual, null, format, args);
        }

        [StringFormatMethod("format")]
        public static void CollectionAssertEqual<T>(
            IEnumerable<T> expected, IEnumerable<T> actual,
            Func<T, T, bool> comparer,
            string format = null, params object[] args)
        {
            if (comparer == null)
                comparer = EqualityComparer<T>.Default.Equals;

            string msg = format != null ? string.Format(format + ": ", args) : "";

            int count = 0;
            T lastValue = default(T);

            string fmt = typeof (T) == typeof (float) || typeof (T) == typeof (double) ? ":r" : "";

            using (IEnumerator<T> e = expected.GetEnumerator())
            using (IEnumerator<T> a = actual.GetEnumerator())
            {
                while (true)
                {
                    bool eMoved = e.MoveNext();
                    if (eMoved != a.MoveNext())
                    {
                        if (eMoved)
                        {
                            Assert.Fail(
                                // ReSharper disable FormatStringProblem
                                "{0}After {1} items, actual list ended, while expected had more starting with {2" +
                                fmt + "}",
                                // ReSharper restore FormatStringProblem
                                msg, count, e.Current);
                        }
                        else
                        {
                            Assert.Fail(
                                // ReSharper disable FormatStringProblem
                                "{0}After {1} items, expected list ended, while actual had more starting with {2" +
                                fmt + "}",
                                // ReSharper restore FormatStringProblem
                                msg, count, a.Current);
                        }
                    }

                    if (!eMoved)
                        break;

                    if (!comparer(e.Current, a.Current))
                    {
                        // ReSharper disable FormatStringProblem
                        string failMsg =
                            string.Format(
                                "{0}After {1} items, expected value {2" + fmt + "} != actual value {3" + fmt + "}",
                                msg, count, e.Current, a.Current);
                        if (count > 0)
                            failMsg += string.Format(", lastValue = {0" + fmt + "}", lastValue);
                        // ReSharper restore FormatStringProblem

                        Assert.Fail(failMsg);
                    }

                    lastValue = a.Current;
                    count++;
                }
            }
        }

        #region Nested type: CacheItem

        private class CacheItem : IEquatable<CacheItem>
        {
            public string Key;
            public object Value;

            #region IEquatable<CacheItem> Members

            public bool Equals(CacheItem other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Equals(other.Key, Key);
            }

            #endregion

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != typeof (CacheItem)) return false;
                return Equals((CacheItem) obj);
            }

            public override int GetHashCode()
            {
                return (Key != null ? Key.GetHashCode() : 0);
            }
        }

        #endregion

        public static IEnumerable<ArraySegment<T>> GenerateSimpleData<T>(Func<long,T> factory, int minValue, int maxValue, int step = 1)
        {
            if(maxValue<minValue) throw new ArgumentException("max > min");
            if(step < 1) throw new ArgumentException("step < 1");
            if((maxValue-minValue)%step != 0) throw new ArgumentException("max does not fall in step");

            var arr = new T[(maxValue - minValue)/step + 1];
            for (int ind = 0, val = minValue; val <= maxValue; val += step)
                arr[ind++] = factory(val);

            return new[] {new ArraySegment<T>(arr)};
        }
    }
}