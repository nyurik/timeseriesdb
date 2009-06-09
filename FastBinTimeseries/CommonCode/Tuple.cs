using System;
using System.Runtime.InteropServices;

namespace NYurik.FastBinTimeseries.CommonCode
{
    public static class Tuple
    {
        public static Tuple<TFirst, TSecond> Create<TFirst, TSecond>(TFirst first, TSecond second)
        {
            return new Tuple<TFirst, TSecond>(first, second);
        }
    }

    [Serializable, StructLayout(LayoutKind.Sequential)]
    public struct Tuple<TFirst, TSecond> : IEquatable<Tuple<TFirst, TSecond>>
    {
        private readonly TFirst m_First;
        private readonly TSecond m_Second;

        [Obsolete]
        public TFirst First
        {
            get { return m_First; }
        }

        public TFirst Item1
        {
            get { return m_First; }
        }

        [Obsolete]
        public TSecond Second
        {
            get { return m_Second; }
        }

        public TSecond Item2
        {
            get { return m_Second; }
        }

        public Tuple(TFirst first, TSecond second)
        {
            m_First = first;
            m_Second = second;
        }

        public static bool operator ==(Tuple<TFirst, TSecond> left, Tuple<TFirst, TSecond> right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(Tuple<TFirst, TSecond> left, Tuple<TFirst, TSecond> right)
        {
            return !left.Equals(right);
        }

        public bool Equals(Tuple<TFirst, TSecond> other)
        {
            return Equals(other.m_First, m_First) && Equals(other.m_Second, m_Second);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (obj.GetType() != typeof (Tuple<TFirst, TSecond>)) return false;
            return Equals((Tuple<TFirst, TSecond>) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
// ReSharper disable CompareNonConstrainedGenericWithNull
                return ((m_First == null ? 0 : m_First.GetHashCode())*397) ^
                       (m_Second == null ? 0 : m_Second.GetHashCode());
// ReSharper restore CompareNonConstrainedGenericWithNull
            }
        }

        public override string ToString()
        {
            return string.Format("({0}, {1})", m_First, m_Second);

        }
    }
}