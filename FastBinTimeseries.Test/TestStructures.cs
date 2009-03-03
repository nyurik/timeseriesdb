using System;
using System.Runtime.InteropServices;

namespace NYurik.FastBinTimeseries.Test
{
    internal struct Struct3Byte : IEquatable<Struct3Byte>
    {
        public bool Equals(Struct3Byte other)
        {
            return other.a == a && other.b == b && other.c == c;
        }

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
                int result = a.GetHashCode();
                result = (result*397) ^ b.GetHashCode();
                result = (result*397) ^ c.GetHashCode();
                return result;
            }
        }

        public byte a, b, c;

        public Struct3Byte(byte a, byte b, byte c)
        {
            this.a = a;
            this.b = b;
            this.c = c;
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    internal struct Struc3ByteUnion
    {
        [FieldOffset(0)] public byte a;
        [FieldOffset(1)] public byte b;
        [FieldOffset(2)] public byte c;

        [FieldOffset(0)] public ushort ab;
        [FieldOffset(1)] public ushort bc;

        public Struc3ByteUnion(byte a, byte b, byte c)
        {
            ab = 0;
            bc = 0;

            this.a = a;
            this.b = b;
            this.c = c;
        }
    }
}