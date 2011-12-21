#region COPYRIGHT
/*
 *     Copyright 2009-2011 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
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
using System.Linq;
using JetBrains.Annotations;
using NUnit.Framework;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test.BlockSerializer
{
    [TestFixture]
    public class ComplexTypeTests : SerializtionTestsBase
    {
        private struct Strct2 : IEquatable<Strct2>, IComparable<Strct2>
        {
            [UsedImplicitly] private double _double1;
            [UsedImplicitly] private double _double2;

            #region Implementation

            public Strct2(double double1, double double2)
            {
                _double1 = double1;
                _double2 = double2;
            }

            #region IComparable<Strct2> Members

            public int CompareTo(Strct2 other)
            {
                return _double1.CompareTo(other._double2);
            }

            #endregion

            #region IEquatable<Strct2> Members

            public bool Equals(Strct2 other)
            {
                return other._double1.Equals(_double1) && other._double2.Equals(_double2);
            }

            #endregion

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (obj.GetType() != typeof (Strct2)) return false;
                return Equals((Strct2) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (_double1.GetHashCode()*397) ^ _double2.GetHashCode();
                }
            }

            #endregion
        }

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

            #region IComparable<Strct> Members

            public int CompareTo(Strct other)
            {
                return Index.CompareTo(other.Index);
            }

            #endregion

            #region IEquatable<Strct> Members

            public bool Equals(Strct other)
            {
                return other.Index == Index && other._double1.Equals(_double1) && other._float.Equals(_float)
                       && other._timestamp.Equals(_timestamp);
            }

            #endregion

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
        public void Strct2SharedStateTest()
        {
            Run(
                Enumerable.Range(1, 100).Select(i => new Strct2(10000 + i*2, 10000 + i*2 + 1)), "0+",
                i =>
                    {
                        var s = (ComplexField) i;
                        ((ScaledDeltaField) s["_double1"].Field).Multiplier = 1;
                        ((ScaledDeltaField) s["_double2"].Field).Multiplier = 1;
                        s["_double2"].Field.StateName = s["_double1"].Field.StateName;
                    });
        }

        [Test]
        public void StrctTest()
        {
            Run(
                Range(new Strct(0), new Strct(1000), i => new Strct(i.Index + 1)), "0+",
                i =>
                    {
                        var s = (ComplexField) i;
                        ((ScaledDeltaField) s["_float"].Field).Multiplier = 10;
                        ((ScaledDeltaField) s["_double1"].Field).Multiplier = 1;
                        ((ScaledDeltaField) s["_double2"].Field).Multiplier = 1;
                    });
        }
    }
}