using System;
using System.Runtime.InteropServices;

namespace NYurik.FastBinTimeseries.Test
{
    public struct _3Byte_noAttr : IEquatable<_3Byte_noAttr>
    {
        public byte a, b, c;

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
            return String.Format("{0}, {1}, {2}", a, b, c);
        }

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
    public struct _DatetimeByte_SeqPk1 : IEquatable<_DatetimeByte_SeqPk1>
    {
        public static DateTime FirstTimeStamp = new DateTime(2000, 1, 1);

        public DateTime a;
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
            return String.Format("{0:u}, {1}", a, b);
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

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _DatetimeBool_SeqPk1 : IEquatable<_DatetimeBool_SeqPk1>
    {
        public static DateTime FirstTimeStamp = new DateTime(2000, 1, 1);

        public DateTime a;
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
            return String.Format("{0:u}, {1}", a, b);
        }

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
    public struct _LongBool_SeqPk1 : IEquatable<_LongBool_SeqPk1>
    {
        public long a;
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

        public static _LongBool_SeqPk1 New(long i)
        {
            return new _LongBool_SeqPk1 {a = i, b = i%2 == 0};
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _LongByte_SeqPk1 : IEquatable<_LongByte_SeqPk1>
    {
        public long a;
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

        public static _LongByte_SeqPk1 New(long i)
        {
            return new _LongByte_SeqPk1 { a = i, b = ((byte)(i & 0xFF)) };
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _BoolLongBool_SeqPk1 : IEquatable<_BoolLongBool_SeqPk1>
    {

        public bool a;
        public long b;
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
        public long b;
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

        public static _ByteLongByte_SeqPk1 New(long i)
        {
            return new _ByteLongByte_SeqPk1 {a = ((byte) (i & 0xFF)), b = i, c = ((byte) ((i & 0xFF00) >> 8)),};
        }

        #endregion
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct _IntBool_SeqPk1 : IEquatable<_IntBool_SeqPk1>
    {
        public int a;
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
        [FieldOffset(2)] public byte c;

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
            return String.Format("{0}, {1}, {2}", a, b, c);
        }

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
}