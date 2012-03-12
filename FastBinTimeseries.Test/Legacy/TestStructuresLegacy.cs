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
using System.Runtime.InteropServices;
using JetBrains.Annotations;
using NYurik.FastBinTimeseries.CommonCode;

// ReSharper disable InconsistentNaming
// ReSharper disable NonReadonlyFieldInGetHashCode

// ReSharper disable CheckNamespace
namespace NYurik.FastBinTimeseries.Test
// ReSharper restore CheckNamespace
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    [Obsolete]
    public struct _DatetimeBool_SeqPk1 : IEquatable<_DatetimeBool_SeqPk1>
    {
        private static UtcDateTime FirstTimeStamp = new UtcDateTime(2000, 1, 1);

        public UtcDateTime a;
        public bool b;

        #region Implementation

        public bool Equals(_DatetimeBool_SeqPk1 other)
        {
            return other.a.Equals(a) && other.b.Equals(b);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_DatetimeBool_SeqPk1)) return false;
            return Equals((_DatetimeBool_SeqPk1) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (a.GetHashCode()*397) ^ b.GetHashCode();
            }
        }

        public override string ToString()
        {
            return string.Format("{0:u}, {1}", a, b);
        }

        [UsedImplicitly]
        public static _DatetimeBool_SeqPk1 New(long i)
        {
            return new _DatetimeBool_SeqPk1
                       {
                           a = FirstTimeStamp.AddMinutes(i),
                           b = i%2 == 0,
                       };
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    [Obsolete]
    public struct _DatetimeByte_SeqPk1 : IEquatable<_DatetimeByte_SeqPk1>
    {
        private static UtcDateTime FirstTimeStamp = new UtcDateTime(2000, 1, 1);

        public UtcDateTime a;
        public byte b;

        #region Implementation

        public bool Equals(_DatetimeByte_SeqPk1 other)
        {
            return other.a.Equals(a) && other.b == b;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_DatetimeByte_SeqPk1)) return false;
            return Equals((_DatetimeByte_SeqPk1) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (a.GetHashCode()*397) ^ b.GetHashCode();
            }
        }

        public override string ToString()
        {
            return string.Format("{0:u}, {1}", a, b);
        }

        public static _DatetimeByte_SeqPk1 New(long i)
        {
            return new _DatetimeByte_SeqPk1
                       {
                           a = FirstTimeStamp.AddMinutes(i),
                           b = (byte) (i & 0xFF),
                       };
        }

        #endregion
    }
}