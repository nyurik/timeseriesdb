using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// Use this struct instead of the <see cref="DateTime"/> to store 
    /// the date in a 1-byte-packed structures.
    /// DateTime may not be used in serialization due to different packing on
    /// 32bit and 64bit architectures.
    /// </summary>
    [Serializable]
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct PackedDateTime : IFormattable
    {
        private static readonly Func<long, DateTime> FromBinaryRaw;
        private static readonly Func<DateTime, long> ToBinaryRaw;

        // ReSharper disable FieldCanBeMadeReadOnly.Local
        private long m_value;
        // ReSharper restore FieldCanBeMadeReadOnly.Local

        static PackedDateTime()
        {
            // Get internal DateTime binary serializer methods

            ParameterExpression longParam = Expression.Parameter(typeof (long), "v");
            FromBinaryRaw = Expression.Lambda<Func<long, DateTime>>(
                Expression.Call(
                    typeof (DateTime).GetMethod("FromBinaryRaw", BindingFlags.Static | BindingFlags.NonPublic),
                    longParam),
                longParam).Compile();

            ParameterExpression dtParam = Expression.Parameter(typeof (DateTime), "v");
            ToBinaryRaw = Expression.Lambda<Func<DateTime, long>>(
                Expression.Call(
                    dtParam,
                    typeof (DateTime).GetMethod("ToBinaryRaw", BindingFlags.Instance | BindingFlags.NonPublic)),
                dtParam).Compile();
        }

        public PackedDateTime(DateTime value)
        {
            m_value = ToBinaryRaw(value);
        }

        private PackedDateTime(long value)
        {
            m_value = value;
        }

        public static implicit operator DateTime(PackedDateTime value)
        {
            return FromBinaryRaw(value.m_value);
        }

        public static implicit operator PackedDateTime(DateTime value)
        {
            return new PackedDateTime(ToBinaryRaw(value));
        }

        public string ToString(string format, IFormatProvider formatProvider)
        {
            return FromBinaryRaw(m_value).ToString(format, formatProvider);
        }
    }
}