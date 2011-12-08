using System;
using System.Collections.Generic;
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

        public static T[] GenerateData<T>(Func<long, T> converter, int count, int startFrom)
        {
            string key = string.Format("{0},{1},{2}", typeof (T).FullName, startFrom, converter);

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
                result[i] = converter(i + startFrom);
            Items.AddFirst(new CacheItem {Key = key, Value = result});
            if (Items.Count > 100)
                Items.RemoveLast();

            var rNew = new T[count];
            Array.Copy(result, rNew, count);
            return rNew;
        }

        public static IEnumerable<Buffer<T>> GenerateDataStream<T>(Func<long, T> converter, int count, int startFrom, int maxValue)
        {
            if (count <= 0)
                yield break;

            for (int i = startFrom; i < maxValue; i += count)
                yield return new Buffer<T>(GenerateData(converter, count, i), count);
        }

        public static byte NewByte(long i)
        {
            return (byte) (i & 0xFF);
        }

        public static void AssertException<TEx>(Action operation)
            where TEx : Exception
        {
            AssertException<TEx>(
                () =>
                    {
                        operation();
                        Assert.Fail(
                            "Should have thrown an {0}, but completed successfully instead",
                            typeof (TEx).Name);
                        return null;
                    });
        }

        public static void AssertException<TEx>(Func<object> operation)
            where TEx : Exception
        {
            try
            {
                object o = operation();
                Assert.Fail("Should have thrown an {0}, but {1} was returned instead", typeof (TEx).Name, o);
            }
            catch (TEx)
            {
            }
        }

        public static void CollectionAssertEqual<T>(IEnumerable<T> expected, IEnumerable<T> actual, string name = null,
                                                    Func<T, T, bool> comparer = null)
        {
            if (comparer == null)
            {
                comparer = EqualityComparer<T>.Default.Equals;
            }

            string msg = name != null ? "In test " + name + ", " : "";

            int count = 0;
            T lastValue = default(T);

            string format = typeof (T) == typeof (float) || typeof (T) == typeof (double) ? ":r" : "";

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
                                "{0}After {1} items, actual list ended, while expected had more starting with {2" +
                                format + "}",
                                msg, count, e.Current);
                        }
                        else
                        {
                            Assert.Fail(
                                "{0}After {1} items, expected list ended, while actual had more starting with {2" +
                                format + "}",
                                msg, count, a.Current);
                        }
                    }

                    if (!eMoved)
                        break;

                    if (!comparer(e.Current, a.Current))
                    {
                        string failMsg =
                            string.Format(
                                "{0}After {1} items, expected value {2" + format + "} != actual value {3" + format + "}",
                                msg, count, e.Current, a.Current);
                        if (count > 0)
                            failMsg += string.Format(", lastValue = {0" + format + "}", lastValue);

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
    }
}