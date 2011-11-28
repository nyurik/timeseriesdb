using System;
using System.Collections.Generic;
using System.Reflection;
using NUnit.Framework;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test.BlockSerializer
{
    [TestFixture]
    public class ComplexTypeTests : SerializtionTestsBase
    {
        private struct Strct : IEquatable<Strct>,IComparable<Strct>
        {
            [Field] private int _index;
            [Field] private double _b;
            [Field] private float _c;
            [Field] private UtcDateTime _timestamp;

            public Strct(int i)
            {
                _timestamp = new UtcDateTime(i+1);
                _index = i;
                _b = i + 2;
                _c = i/10.0f;
            }

            public int Index
            {
                get { return _index; }
            }

            public bool Equals(Strct other)
            {
                return other.Index == Index && other._b.Equals(_b) && other._c.Equals(_c) && other._timestamp.Equals(_timestamp);
            }

            public int CompareTo(Strct other)
            {
                return Index.CompareTo(other.Index);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (obj.GetType() != typeof (Strct)) return false;
                return Equals((Strct) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    int result = Index;
                    result = (result*397) ^ _b.GetHashCode();
                    result = (result*397) ^ _c.GetHashCode();
                    result = (result*397) ^ _timestamp.GetHashCode();
                    return result;
                }
            }
        }

        [Test]
        public void StrctTest()
        {
            Run(Range(new Strct(0), new Strct(2), i => new Strct(i.Index + 1)), "0+");
        }
    }
}