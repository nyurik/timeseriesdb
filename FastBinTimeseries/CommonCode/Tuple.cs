using System;
using System.Runtime.InteropServices;

namespace NYurik.FastBinTimeseries.CommonCode
{
    /// <summary>
    /// Let's hope some day this will be a part of .NET framework...
    /// </summary>
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
        private readonly TFirst _first;
        private readonly TSecond _second;

        public TFirst First
        {
            get { return _first; }
        }

        public TSecond Second
        {
            get { return _second; }
        }

        public Tuple(TFirst first, TSecond second)
        {
            _first = first;
            _second = second;
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
            return Equals(other._first, _first) && Equals(other._second, _second);
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
                return ((_first == null ? 0 : _first.GetHashCode())*397) ^
                       (_second == null ? 0 : _second.GetHashCode());
// ReSharper restore CompareNonConstrainedGenericWithNull
            }
        }

        public override string ToString()
        {
            return string.Format("({0}, {1})", _first, _second);
        }
    }
}