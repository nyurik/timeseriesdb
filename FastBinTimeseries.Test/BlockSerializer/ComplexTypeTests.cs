using System;
using JetBrains.Annotations;
using NUnit.Framework;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test.BlockSerializer
{
    [TestFixture]
    public class ComplexTypeTests : SerializtionTestsBase
    {
        private struct Strct : IEquatable<Strct>, IComparable<Strct>
        {
            [UsedImplicitly] private double _double1;
            [UsedImplicitly] private double _double2;
            [UsedImplicitly] private float _float;
            [UsedImplicitly] private int _index;
            [UsedImplicitly] private UtcDateTime _timestamp;

            public Strct(int i)
            {
                _timestamp = new UtcDateTime(i + 1);
                _index = i;
                _double1 = i*1000 + 1;
                _double2 = i*1000 + 2;
                _float = i/10.0f;
            }

            public int Index
            {
                get { return _index; }
            }

            #region Implementation

            public int CompareTo(Strct other)
            {
                return Index.CompareTo(other.Index);
            }

            public bool Equals(Strct other)
            {
                return other.Index == Index && other._double1.Equals(_double1) && other._float.Equals(_float)
                       && other._timestamp.Equals(_timestamp);
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
                    result = (result*397) ^ _double1.GetHashCode();
                    result = (result*397) ^ _float.GetHashCode();
                    result = (result*397) ^ _timestamp.GetHashCode();
                    return result;
                }
            }

            public override string ToString()
            {
                return string.Format("#{0}, @{1}, B={2}, C={3:r}", _index, _timestamp, _double1, _float);
            }

            #endregion
        }

        [Test]
        public void StrctTest()
        {
            Run(
                Range(new Strct(0), new Strct(1000), i => new Strct(i.Index + 1)), "0+",
                i =>
                    {
                        var s = (FieldsSerializer) i;
                        ((MultipliedDeltaSerializer) s["_float"].Serializer).Multiplier = 10;
                        ((MultipliedDeltaSerializer) s["_double1"].Serializer).Multiplier = 1;
                        ((MultipliedDeltaSerializer) s["_double2"].Serializer).Multiplier = 1;
                    });
        }
    }
}