using System;
using System.Security.Cryptography;
using JetBrains.Annotations;

#if DEBUG_SERIALIZER

#endif

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class CodecBase : IDisposable
    {
        public const byte MaxBytesFor64 = 64/7 + 1;
        public const byte MaxBytesFor32 = 32/7 + 1;
        public const byte MaxBytesFor8 = 1;

        /// <summary>
        /// Each block will have at least this many bytes reserved for hash value + UInt32 (count)
        /// </summary>
        public const int ReservedSpace = MaxBytesFor32 + 1;

        private HashAlgorithm _hashAlgorithm;

        protected HashAlgorithm HashAlgorithm
        {
            get { return _hashAlgorithm ?? (_hashAlgorithm = new MD5CryptoServiceProvider()); }
        }

        protected int ValidateCount(ulong count)
        {
            if (count == 0 || count > int.MaxValue)
                throw new SerializerException("Invalid count - must be >0 && <= int.MaxValue");

            return (int) count;
        }

        #region Debug

#if DEBUG_SERIALIZER
        private const int DebugHistLength = 10;
        private readonly Tuple<string, int, double>[] _debugDoubleHist = new Tuple<string, int, double>[DebugHistLength];
        private readonly Tuple<string, int, float>[] _debugFloatHist = new Tuple<string, int, float>[DebugHistLength];
        private readonly Tuple<string, int, long>[] _debugLongHist = new Tuple<string, int, long>[DebugHistLength];

        [UsedImplicitly]
        internal void DebugLong(long v, int position, string name)
        {
            DebugValue(_debugLongHist, position, v, name);
        }

        [UsedImplicitly]
        internal void DebugFloat(float v, int position, string name)
        {
            DebugValue(_debugFloatHist, position, v, name);
        }

        [UsedImplicitly]
        internal void DebugDouble(double v, int position, string name)
        {
            DebugValue(_debugDoubleHist, position, v, name);
        }

        internal void DebugValue<T>(Tuple<string, int, T>[] values, int position, T v, string name)
        {
            Array.Copy(values, 1, values, 0, values.Length - 1);
            values[values.Length - 1] = Tuple.Create(name, position, v);
        }
#endif

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            HashAlgorithm ha = HashAlgorithm;
            _hashAlgorithm = null;
            if (ha != null)
                ha.Dispose();
        }

        #endregion
    }
}