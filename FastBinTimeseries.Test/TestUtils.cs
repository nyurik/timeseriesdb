using System;
using System.Collections.Generic;
using NUnit.Framework;

namespace NYurik.FastBinTimeseries.Test
{
    internal class TestUtils
    {
        private static readonly LinkedList<CacheItem> items = new LinkedList<CacheItem>();

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
            for (int i = 0; i < expected.Length; i++)
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
            string key = string.Format("{0},{1},{2}", typeof (T).FullName, startFrom, converter);

            T[] result;
            LinkedListNode<CacheItem> res = items.Find(new CacheItem { Key = key });
            if (res != null)
            {
                items.Remove(res);
                
                result = (T[])res.Value.Value;
                if (result.Length >= count)
                {
                    if(result.Length > count)
                    {
                        var rOld = result;
                        result = new T[count];
                        Array.Copy(rOld, result, count);
                    }

                    items.AddFirst(res);
                    return result;
                }
            }

            result = new T[count+100];
            for (long i = 0; i < count+100; i++)
                result[i] = converter(i + startFrom);
            items.AddFirst(new CacheItem {Key = key, Value = result});
            if (items.Count > 100)
                items.RemoveLast();

            var rNew = new T[count];
            Array.Copy(result, rNew, count);
            return rNew;
        }

        public static byte NewByte(long i)
        {
            return (byte) (i & 0xFF);
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