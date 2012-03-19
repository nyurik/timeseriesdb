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
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    internal static class TestUtils
    {
        private static readonly LinkedList<CacheItem> Items = new LinkedList<CacheItem>();

        private static readonly Dictionary<Type, Tuple<Delegate, object>> FuncCache =
            new Dictionary<Type, Tuple<Delegate, object>>();

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

        public static void AreNotEqual<T>(ArraySegment<T> expected, ArraySegment<T> values, string description = null)
        {
            var s = new DefaultTypeSerializer<T>();
            Assert.IsFalse(s.BinaryArrayCompare(expected, values), description);
        }

        public static void AreEqual<T>(T[] expected, T[] values, string description = null)
        {
            AreEqual(new ArraySegment<T>(expected), new ArraySegment<T>(values), description);
        }

        public static void AreEqual<T>(ArraySegment<T> expected, ArraySegment<T> values, string description = null)
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

        public static T[] GenerateData<T>(int count, int startFrom, int step = 1)
        {
            string key = String.Format("{0},{1},{2}", typeof (T).FullName, startFrom, step);
            T[] result;
            LinkedListNode<CacheItem> res = Items.Find(new CacheItem(key));
            if (res != null)
            {
                Items.Remove(res);

                result = (T[]) res.Value.Value;
                if (result.Length >= count)
                {
                    T[] rOld = result;
                    result = new T[count];
                    Array.Copy(rOld, result, count);

                    Items.AddFirst(res);
                    return result;
                }
            }

            var newObj = GetObjInfo<T>().Item1;
            result = new T[count + 100];
            for (long i = 0; i < count + 100; i++)
                result[i] = newObj(startFrom + step*i);

            Items.AddFirst(new CacheItem(key, result));
            if (Items.Count > 100)
                Items.RemoveLast();

            var rNew = new T[count];
            Array.Copy(result, rNew, count);
            return rNew;
        }

        public static IEnumerable<ArraySegment<T>> GenerateDataStream<T>(int segSize, int minValue, int maxValue,
                                                                         int step = 1)
        {
            if (segSize <= 0)
                yield break;

            for (long i = minValue; i < maxValue; i += segSize)
                yield return
                    new ArraySegment<T>(GenerateData<T>((int) Math.Min(segSize, maxValue - i), (int) i, step));
        }

        [StringFormatMethod("format")]
        public static void CollectionAssertEqual<T>(
            IEnumerable<ArraySegment<T>> expected, IEnumerable<T> actual,
            string format = null, params object[] args)
        {
            CollectionAssertEqual(expected.Stream(), actual, null, format, args);
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

            string msg = format != null ? String.Format(format + ": ", args) : "";

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
                            String.Format(
                                "{0}After {1} items, expected value {2" + fmt + "} != actual value {3" + fmt + "}",
                                msg, count, e.Current, a.Current);
                        if (count > 0)
                            failMsg += String.Format(", lastValue = {0" + fmt + "}", lastValue);
                        // ReSharper restore FormatStringProblem

                        Assert.Fail(failMsg);
                    }

                    lastValue = a.Current;
                    count++;
                }
            }
        }

        public static Tuple<Func<long, T>, T> GetObjInfo<T>()
        {
            Type type = typeof (T);
            Tuple<Delegate, object> val;

            if (!FuncCache.TryGetValue(type, out val))
            {
                if (type.IsPrimitive)
                {
                    switch (Type.GetTypeCode(type))
                    {
                        case TypeCode.Byte:
                            val =
                                Tuple.Create(
                                    (Delegate) (Func<long, byte>) (i => (byte) (i%byte.MaxValue)),
                                    (object) byte.MaxValue);
                            break;
                        default:
                            throw new NotImplementedException("Primitive type not supported: " + type.Name);
                    }
                }
                else
                {
                    ParameterExpression param = Expression.Parameter(typeof (long));
                    val =
                        Tuple.Create(
                            (Delegate) Expression.Lambda<Func<long, T>>(
                                Expression.Call(
                                    type.GetMethod(
                                        "New", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic),
                                    param),
                                param).Compile(),
// ReSharper disable PossibleNullReferenceException
                            type
                                .GetField(
                                    "MaxValue", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
// ReSharper restore PossibleNullReferenceException
                                .GetValue(null));
                }

                FuncCache.Add(type, val);
            }

            return Tuple.Create((Func<long, T>) val.Item1, (T) val.Item2);
        }

        #region Nested type: CacheItem

        private class CacheItem : IEquatable<CacheItem>
        {
            public readonly object Value;
            private readonly string _key;

            public CacheItem(string key, object value = null)
            {
                _key = key;
                Value = value;
            }

            #region IEquatable<CacheItem> Members

            public bool Equals(CacheItem other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Equals(other._key, _key);
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
                return (_key != null ? _key.GetHashCode() : 0);
            }
        }

        #endregion
    }
}