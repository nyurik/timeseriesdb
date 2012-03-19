#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of TimeSeriesDb library
 * 
 *  TimeSeriesDb is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  TimeSeriesDb is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with TimeSeriesDb.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.Security.Cryptography;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb.Serializers.BlockSerializer
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

        protected int ValidateCount(long count)
        {
            if (count <= 0 || count > int.MaxValue)
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