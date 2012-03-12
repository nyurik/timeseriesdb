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
using System.Text;
using JetBrains.Annotations;

// ReSharper disable InconsistentNaming
// ReSharper disable NonReadonlyFieldInGetHashCode

namespace NYurik.FastBinTimeseries.Test
{
    public struct _3Byte_noAttr : IEquatable<_3Byte_noAttr>
    {
        public byte a;
        public byte b;
        [Index] public byte c;

        #region Implementation

        public bool Equals(_3Byte_noAttr other)
        {
            return other.a == a && other.b == b && other.c == c;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_3Byte_noAttr)) return false;
            return Equals((_3Byte_noAttr) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = a.GetHashCode();
                result = (result*397) ^ b.GetHashCode();
                result = (result*397) ^ c.GetHashCode();
                return result;
            }
        }

        public override string ToString()
        {
            return string.Format("{0}, {1}, {2}", a, b, c);
        }

        [UsedImplicitly]
        public static _3Byte_noAttr New(long i)
        {
            return new _3Byte_noAttr
                       {
                           a = (byte) ((i & 0xFF0000) >> 16),
                           b = (byte) ((i & 0xFF00) >> 8),
                           c = (byte) (i & 0xFF),
                       };
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _LongBool_SeqPk1 : IEquatable<_LongBool_SeqPk1>
    {
        [Index] public long a;
        public bool b;

        #region Implementation

        public override string ToString()
        {
            return string.Format("{0}, {1}", a, b);
        }

        public bool Equals(_LongBool_SeqPk1 other)
        {
            return other.a == a && other.b.Equals(b);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_LongBool_SeqPk1)) return false;
            return Equals((_LongBool_SeqPk1) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (a.GetHashCode()*397) ^ b.GetHashCode();
            }
        }

        [UsedImplicitly]
        public static _LongBool_SeqPk1 New(long i)
        {
            return new _LongBool_SeqPk1 {a = i, b = i%2 == 0};
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _LongByte_SeqPk1 : IEquatable<_LongByte_SeqPk1>
    {
        [Index] public long a;
        public byte b;

        #region Implementation

        public override string ToString()
        {
            return string.Format("{0}, {1}", a, b);
        }

        public bool Equals(_LongByte_SeqPk1 other)
        {
            return other.a == a && other.b == b;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_LongByte_SeqPk1)) return false;
            return Equals((_LongByte_SeqPk1) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (a.GetHashCode()*397) ^ b.GetHashCode();
            }
        }

        [UsedImplicitly]
        public static _LongByte_SeqPk1 New(long i)
        {
            return new _LongByte_SeqPk1 {a = i, b = ((byte) (i & 0xFF))};
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _BoolLongBool_SeqPk1 : IEquatable<_BoolLongBool_SeqPk1>
    {
        public bool a;
        [Index] public long b;
        public bool c;

        #region Implementation

        public override string ToString()
        {
            return string.Format("{0}, {1}, {2}", a, b, c);
        }

        public bool Equals(_BoolLongBool_SeqPk1 other)
        {
            return other.a.Equals(a) && other.b == b && other.c.Equals(c);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_BoolLongBool_SeqPk1)) return false;
            return Equals((_BoolLongBool_SeqPk1) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = a.GetHashCode();
                result = (result*397) ^ b.GetHashCode();
                result = (result*397) ^ c.GetHashCode();
                return result;
            }
        }

        [UsedImplicitly]
        public static _BoolLongBool_SeqPk1 New(long i)
        {
            return new _BoolLongBool_SeqPk1 {a = i%2 == 1, b = i, c = i%2 == 0,};
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _ByteLongByte_SeqPk1 : IEquatable<_ByteLongByte_SeqPk1>
    {
        public byte a;
        [Index] public long b;
        public byte c;

        #region Implementation

        public override string ToString()
        {
            return string.Format("{0}, {1}, {2}", a, b, c);
        }

        public bool Equals(_ByteLongByte_SeqPk1 other)
        {
            return other.a == a && other.b == b && other.c == c;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_ByteLongByte_SeqPk1)) return false;
            return Equals((_ByteLongByte_SeqPk1) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = a.GetHashCode();
                result = (result*397) ^ b.GetHashCode();
                result = (result*397) ^ c.GetHashCode();
                return result;
            }
        }

        [UsedImplicitly]
        public static _ByteLongByte_SeqPk1 New(long i)
        {
            return new _ByteLongByte_SeqPk1 {a = ((byte) (i & 0xFF)), b = i, c = ((byte) ((i & 0xFF00) >> 8)),};
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _IntBool_SeqPk1 : IEquatable<_IntBool_SeqPk1>
    {
        [Index] public int a;
        public bool b;

        #region Implementation

        public bool Equals(_IntBool_SeqPk1 other)
        {
            return other.a == a && other.b.Equals(b);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_IntBool_SeqPk1)) return false;
            return Equals((_IntBool_SeqPk1) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (a*397) ^ b.GetHashCode();
            }
        }

        [UsedImplicitly]
        public static _IntBool_SeqPk1 New(long i)
        {
            unchecked
            {
                return new _IntBool_SeqPk1
                           {
                               a = (int) i,
                               b = i%2 == 0,
                           };
            }
        }

        #endregion
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct _3Byte_2Shrt_ExplPk1 : IEquatable<_3Byte_2Shrt_ExplPk1>
    {
        [FieldOffset(0)] public byte a;
        [FieldOffset(1)] public byte b;
        [Index] [FieldOffset(2)] public byte c;

        [FieldOffset(0)] public ushort ab;
        [FieldOffset(1)] public ushort bc;

        #region Implementation

        public bool Equals(_3Byte_2Shrt_ExplPk1 other)
        {
            return other.a == a && other.b == b && other.c == c && other.ab == ab && other.bc == bc;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_3Byte_2Shrt_ExplPk1)) return false;
            return Equals((_3Byte_2Shrt_ExplPk1) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = a.GetHashCode();
                result = (result*397) ^ b.GetHashCode();
                result = (result*397) ^ c.GetHashCode();
                result = (result*397) ^ ab.GetHashCode();
                result = (result*397) ^ bc.GetHashCode();
                return result;
            }
        }

        public override string ToString()
        {
            return string.Format("{0}, {1}, {2}", a, b, c);
        }

        [UsedImplicitly]
        public static _3Byte_2Shrt_ExplPk1 New(long i)
        {
            return new _3Byte_2Shrt_ExplPk1
                       {
                           a = (byte) ((i & 0xFF0000) >> 16),
                           b = (byte) ((i & 0xFF00) >> 8),
                           c = (byte) (i & 0xFF),
                       };
        }

        #endregion
    }

    //    /// Uncommenting this struct makes the _FixedByteBuff7 test fail if file was created without _FixedByteBuff4 present
    //    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    //    public struct _FixedByteBuff4
    //    {
    //        private const int ArrayLen = 4;
    //        public unsafe fixed byte a [ArrayLen];
    //    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _FixedByteBuff7 : IEquatable<_FixedByteBuff7>
    {
        [Index] private long Key;
        private const int ArrayLenA = 3;
        private const int ArrayLenB = 4;
        public unsafe fixed byte a [ArrayLenA];
        private unsafe fixed byte b [ArrayLenB];

        #region Implementation

        public override unsafe string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(Key);
            sb.Append(",");
            fixed (byte* pa = a)
                for (int i = 0; i < ArrayLenA; i++)
                    sb.AppendFormat("{0},", pa[i]);
            fixed (byte* pb = b)
                for (int i = 0; i < ArrayLenB; i++)
                    sb.AppendFormat("{0},", pb[i]);

            sb.Remove(sb.Length - 1, 1);
            return sb.ToString();
        }

        public unsafe bool Equals(_FixedByteBuff7 other)
        {
            if (Key != other.Key)
                return false;
            fixed (byte* pa = a)
                for (int i = 0; i < ArrayLenA; i++)
                    if (pa[i] != other.a[i])
                        return false;
            fixed (byte* pb = b)
                for (int i = 0; i < ArrayLenB; i++)
                    if (pb[i] != other.b[i])
                        return false;
            return true;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_FixedByteBuff7)) return false;
            return Equals((_FixedByteBuff7) obj);
        }

        public override unsafe int GetHashCode()
        {
            unchecked
            {
                fixed (byte* pa = a, pb = b)
                {
                    var result = (int) Key;
                    for (int i = 0; i < ArrayLenA; i++)
                        result = (result*397) ^ pa[i].GetHashCode();
                    for (int i = 0; i < ArrayLenB; i++)
                        result = (result*397) ^ pb[i].GetHashCode();
                    return result;
                }
            }
        }

        [UsedImplicitly]
        public static unsafe _FixedByteBuff7 New(long j)
        {
            var v = new _FixedByteBuff7 {Key = j};
            for (int i = 0; i < ArrayLenA; i++)
                v.a[i] = (byte) j++;
            for (int i = 0; i < ArrayLenB; i++)
                v.b[i] = (byte) j++;
            return v;
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _4Flds_ComplxIdx : IEquatable<_4Flds_ComplxIdx>
    {
        [Index] public _CmplxIdx Index;
        public int Field1;
        public uint Field2;
        public ulong Field3;

        #region Implementation

        public override string ToString()
        {
            return string.Format("{0}, {1}, {2}, {3}", Index, Field1, Field2, Field3);
        }

        public bool Equals(_4Flds_ComplxIdx other)
        {
            return other.Index.Equals(Index) && other.Field1 == Field1 && other.Field2 == Field2
                   && other.Field3 == Field3;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_4Flds_ComplxIdx)) return false;
            return Equals((_4Flds_ComplxIdx) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = Index.GetHashCode();
                result = (result*397) ^ Field1;
                result = (result*397) ^ Field2.GetHashCode();
                result = (result*397) ^ Field3.GetHashCode();
                return result;
            }
        }

        [UsedImplicitly]
        public static _4Flds_ComplxIdx New(long ix)
        {
            return new _4Flds_ComplxIdx
                       {
                           Index = new _CmplxIdx {Field1 = (int) ix, Field2 = (ulong) ix},
                           Field1 = (int) (-1*ix),
                           Field2 = (uint) ix,
                           Field3 = (ulong) (ix << 32),
                       };
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _CmplxIdx : IComparable<_CmplxIdx>
    {
        public int Field1;
        public ulong Field2;

        #region Implementation

        public bool Equals(_CmplxIdx other)
        {
            return other.Field1 == Field1 && other.Field2 == Field2;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_CmplxIdx)) return false;
            return Equals((_CmplxIdx) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Field1*397) ^ Field2.GetHashCode();
            }
        }

        public int CompareTo(_CmplxIdx other)
        {
            int comp = Field1.CompareTo(other.Field1);
            return comp == 0 ? Field2.CompareTo(other.Field2) : comp;
        }

        #endregion
    }

    public class _LongBool_Class : IEquatable<_LongBool_Class>
    {
        [Index] public long a;
        public bool b;

        #region Implementation

        public bool Equals(_LongBool_Class other)
        {
            return other.a == a && other.b.Equals(b);
        }

        public override string ToString()
        {
            return string.Format("{0}, {1}", a, b);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_LongBool_Class)) return false;
            return Equals((_LongBool_Class) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (a.GetHashCode()*397) ^ b.GetHashCode();
            }
        }

        [UsedImplicitly]
        public static _LongBool_Class New(long i)
        {
            return new _LongBool_Class {a = i, b = i%2 == 0};
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _4Flds_ComplxIdxClass : IEquatable<_4Flds_ComplxIdxClass>
    {
        [Index] public _CmplxIdxClass Index;
        public int Field1;
        public uint Field2;
        public ulong Field3;

        #region Implementation

        public override string ToString()
        {
            return string.Format("{0}, {1}, {2}, {3}", Index, Field1, Field2, Field3);
        }

        public bool Equals(_4Flds_ComplxIdxClass other)
        {
            return other.Index.Equals(Index) && other.Field1 == Field1 && other.Field2 == Field2
                   && other.Field3 == Field3;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_4Flds_ComplxIdxClass)) return false;
            return Equals((_4Flds_ComplxIdxClass) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = Index.GetHashCode();
                result = (result*397) ^ Field1;
                result = (result*397) ^ Field2.GetHashCode();
                result = (result*397) ^ Field3.GetHashCode();
                return result;
            }
        }

        [UsedImplicitly]
        public static _4Flds_ComplxIdxClass New(long ix)
        {
            return new _4Flds_ComplxIdxClass
                       {
                           Index = new _CmplxIdxClass {Field1 = (int) ix, Field2 = (ulong) ix},
                           Field1 = (int) (-1*ix),
                           Field2 = (uint) ix,
                           Field3 = (ulong) (ix << 32),
                       };
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public class _4FldsClass_ComplxIdxClass : IEquatable<_4FldsClass_ComplxIdxClass>
    {
        [Index] public _CmplxIdxClass Index;
        public int Field1;
        public uint Field2;
        public ulong Field3;

        #region Implementation

        public override string ToString()
        {
            return string.Format("{0}, {1}, {2}, {3}", Index, Field1, Field2, Field3);
        }

        public bool Equals(_4FldsClass_ComplxIdxClass other)
        {
            return other.Index.Equals(Index) && other.Field1 == Field1 && other.Field2 == Field2
                   && other.Field3 == Field3;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_4FldsClass_ComplxIdxClass)) return false;
            return Equals((_4FldsClass_ComplxIdxClass) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = Index.GetHashCode();
                result = (result*397) ^ Field1;
                result = (result*397) ^ Field2.GetHashCode();
                result = (result*397) ^ Field3.GetHashCode();
                return result;
            }
        }

        [UsedImplicitly]
        public static _4FldsClass_ComplxIdxClass New(long ix)
        {
            return new _4FldsClass_ComplxIdxClass
                       {
                           Index = new _CmplxIdxClass {Field1 = (int) ix, Field2 = (ulong) ix},
                           Field1 = (int) (-1*ix),
                           Field2 = (uint) ix,
                           Field3 = (ulong) (ix << 32),
                       };
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _CmplxIdxClass : IComparable<_CmplxIdxClass>
    {
        public int Field1;
        public ulong Field2;

        #region Implementation

        public bool Equals(_CmplxIdxClass other)
        {
            return other.Field1 == Field1 && other.Field2 == Field2;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (_CmplxIdxClass)) return false;
            return Equals((_CmplxIdxClass) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Field1*397) ^ Field2.GetHashCode();
            }
        }

        public int CompareTo(_CmplxIdxClass other)
        {
            int comp = Field1.CompareTo(other.Field1);
            return comp == 0 ? Field2.CompareTo(other.Field2) : comp;
        }

        #endregion
    }
}