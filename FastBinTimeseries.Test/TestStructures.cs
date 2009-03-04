using System;
using System.Runtime.InteropServices;

namespace NYurik.FastBinTimeseries.Test
{
    public struct Struct3Byte : IEquatable<Struct3Byte>
    {
        public byte a, b, c;

        public Struct3Byte(byte a, byte b, byte c)
        {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        #region IEquatable<Struct3Byte> Members

        public bool Equals(Struct3Byte other)
        {
            return other.a == a && other.b == b && other.c == c;
        }

        #endregion

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (Struct3Byte)) return false;
            return Equals((Struct3Byte) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var result = a.GetHashCode();
                result = (result*397) ^ b.GetHashCode();
                result = (result*397) ^ c.GetHashCode();
                return result;
            }
        }

        public override string ToString()
        {
            return string.Format("{0} / {1} / {2}", a, b, c);
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct StructTimeValue : IEquatable<StructTimeValue>
    {
        public DateTime timestamp;
        public byte a;

        public override string ToString()
        {
            return string.Format("{0} / {1:u}", a, timestamp);
        }

        public StructTimeValue(byte a, DateTime timestamp)
        {
            this.a = a;
            this.timestamp = timestamp;
        }

        public bool Equals(StructTimeValue other)
        {
            return other.a == a && other.timestamp.Equals(timestamp);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (StructTimeValue)) return false;
            return Equals((StructTimeValue) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (a.GetHashCode()*397) ^ timestamp.GetHashCode();
            }
        }
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct Struct3ByteUnion : IEquatable<Struct3ByteUnion>
    {
        [FieldOffset(0)] public byte a;
        [FieldOffset(1)] public byte b;
        [FieldOffset(2)] public byte c;

        [FieldOffset(0)] public ushort ab;
        [FieldOffset(1)] public ushort bc;

        public Struct3ByteUnion(byte a, byte b, byte c)
        {
            ab = 0;
            bc = 0;

            this.a = a;
            this.b = b;
            this.c = c;
        }

        public bool Equals(Struct3ByteUnion other)
        {
            return other.a == a && other.b == b && other.c == c;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (Struct3ByteUnion)) return false;
            return Equals((Struct3ByteUnion) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var result = a.GetHashCode();
                result = (result*397) ^ b.GetHashCode();
                result = (result*397) ^ c.GetHashCode();
                return result;
            }
        }

        public override string ToString()
        {
            return string.Format("{0} / {1} / {2}", a, b, c);
        }
    }
}