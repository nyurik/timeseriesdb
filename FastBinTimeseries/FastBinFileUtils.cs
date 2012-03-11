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
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using JetBrains.Annotations;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers;

namespace NYurik.FastBinTimeseries
{
    public static class FastBinFileUtils
    {
        public static long RoundDownToMultiple(long value, long multiple)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException("value", value, "Value must be >= 0");
            return value - value%multiple;
        }

        public static int RoundDownToMultiple(int value, int multiple)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException("value", value, "Value must be >= 0");
            return value - value%multiple;
        }

        public static long RoundUpToMultiple(long value, long multiple)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException("value", value, "Value must be >= 0");
            if (value == 0)
                return 0;
            return value - 1 + (multiple - (value - 1)%multiple);
        }

        public static int RoundUpToMultiple(int value, int multiple)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException("value", value, "Value must be >= 0");
            if (value == 0)
                return 0;
            return value - 1 + (multiple - (value - 1)%multiple);
        }

        public static SerializerException GetItemSizeChangedException(
            IBinSerializer serializer, string tag, int itemSize)
        {
            return new SerializerException(
                "Serializer {1} ({2}){0} was created with ItemSize={3}, but now the ItemSize={4}",
                tag == null ? "" : " Tag='" + tag + "'",
                serializer.GetType().AssemblyQualifiedName,
                serializer.Version,
                itemSize,
                serializer.TypeSize);
        }

        public static Version ReadVersion(this BinaryReader reader)
        {
            int major = reader.ReadInt32();
            int minor = reader.ReadInt32();
            int build = reader.ReadInt32();
            int revision = reader.ReadInt32();

            return build < 0
                       ? new Version(major, minor)
                       : revision < 0
                             ? new Version(major, minor, build)
                             : new Version(major, minor, build, revision);
        }

        public static void WriteVersion(this BinaryWriter writer, Version ver)
        {
            writer.Write(ver.Major);
            writer.Write(ver.Minor);
            writer.Write(ver.Build);
            writer.Write(ver.Revision);
        }

        public static T ReadTypeAndInstantiate<T>(
            this BinaryReader reader, Func<string, Type> typeResolver, bool nonPublic)
            where T : class
        {
            string typeName;
            int fixedBufferSize;
            Type type = reader.ReadType(typeResolver, out typeName, out fixedBufferSize);
            if (type == null)
                throw new BinaryFileException("Unable to instantiate type {0}", typeName);

            var instance = Activator.CreateInstance(type, nonPublic) as T;
            if (instance == null)
            {
                string aqn = type.AssemblyQualifiedName;
                throw new BinaryFileException(
                    "Type {0}{1} cannot be cast into {2}", aqn,
                    aqn == typeName ? "" : " (re-mapped from " + typeName + ")",
                    typeof (T).AssemblyQualifiedName);
            }
            return instance;
        }

        public static Type ReadType(
            this BinaryReader reader, Func<string, Type> typeResolver,
            out string typeName, out int fixedBufferSize)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            typeName = reader.ReadString();

            if (typeName.StartsWith("!"))
            {
                // Special case - possibly storing the size of the fixed buffer as an integer
                if (int.TryParse(typeName.Substring(1), NumberStyles.None, null, out fixedBufferSize))
                    return null;
            }

            fixedBufferSize = -1;
            Type type = null;

            if (typeResolver != null)
                type = typeResolver(typeName);

            if (type == null)
                type = TypeUtils.GetTypeFromAnyAssemblyVersion(typeName);

            if (type == null)
            {
                // This file could have been created before FixedBuffer support, so check the type name if it looks like this:
                // NYurik.FastBinTimeseries.Test._FixedByteBuff3+<a>e__FixedBuffer0, NYurik.FastBinTimeseries.Test, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
                if (Regex.IsMatch(typeName, @"\+\<.*\>e__FixedBuffer[0-9]+"))
                    return null;

                throw new InvalidOperationException("Unable to find type " + typeName);
            }

            return type;
        }

        public static void WriteType(this BinaryWriter writer, [NotNull] Type type)
        {
            if (writer == null) throw new ArgumentNullException("writer");
            if (type == null) throw new ArgumentNullException("type");

            string aqn = type.AssemblyQualifiedName;
            if (aqn == null) throw new ArgumentOutOfRangeException("type", type, "AssemblyQualifiedName is null");
            writer.Write(aqn);
        }

        #region Internal

        internal static bool IsDefault<TInd>(TInd value)
            where TInd : IComparable<TInd>
        {
            // ReSharper disable CompareNonConstrainedGenericWithNull
            return typeof (TInd).IsClass
                       ? value == null
                       : value.CompareTo(default(TInd)) == 0;
            // ReSharper restore CompareNonConstrainedGenericWithNull
        }

        /// <summary>
        /// Fast memory copying - copies in blocks of 32 bytes, using either int or long (on 64bit machines)
        /// Calling the native RtlMemoryMove was slower
        /// </summary>
        internal static unsafe void CopyMemory(byte* pDestination, byte* pSource, uint byteCount)
        {
            const int blockSize = 32;
            if (byteCount >= blockSize)
            {
                if (NativeWinApis.Is64bit)
                {
                    do
                    {
                        ((long*)pDestination)[0] = ((long*)pSource)[0];
                        ((long*)pDestination)[1] = ((long*)pSource)[1];
                        ((long*)pDestination)[2] = ((long*)pSource)[2];
                        ((long*)pDestination)[3] = ((long*)pSource)[3];
                        pDestination += blockSize;
                        pSource += blockSize;
                        byteCount -= blockSize;
                    } while (byteCount >= blockSize);
                }
                else
                {
                    do
                    {
                        ((int*)pDestination)[0] = ((int*)pSource)[0];
                        ((int*)pDestination)[1] = ((int*)pSource)[1];
                        ((int*)pDestination)[2] = ((int*)pSource)[2];
                        ((int*)pDestination)[3] = ((int*)pSource)[3];
                        ((int*)pDestination)[4] = ((int*)pSource)[4];
                        ((int*)pDestination)[5] = ((int*)pSource)[5];
                        ((int*)pDestination)[6] = ((int*)pSource)[6];
                        ((int*)pDestination)[7] = ((int*)pSource)[7];
                        pDestination += blockSize;
                        pSource += blockSize;
                        byteCount -= blockSize;
                    } while (byteCount >= blockSize);
                }
            }

            while (byteCount > 0)
            {
                *(pDestination++) = *(pSource++);
                byteCount--;
            }
        }

        /// <summary>
        /// Fast memory comparison - compares in blocks of 32 bytes, using either int or long (on 64bit machines)
        /// </summary>
        internal static unsafe bool CompareMemory(byte* pSource1, byte* pSource2, uint byteCount)
        {
            const int blockSize = 32;
            if (byteCount >= blockSize)
            {
                if (NativeWinApis.Is64bit)
                {
                    do
                    {
                        if (((long*) pSource1)[0] != ((long*) pSource2)[0]
                            || ((long*) pSource1)[1] != ((long*) pSource2)[1]
                            || ((long*) pSource1)[2] != ((long*) pSource2)[2]
                            || ((long*) pSource1)[3] != ((long*) pSource2)[3]
                            )
                            return false;
                        pSource1 += blockSize;
                        pSource2 += blockSize;
                        byteCount -= blockSize;
                    } while (byteCount >= blockSize);
                }
                else
                {
                    do
                    {
                        if (((int*) pSource1)[0] != ((int*) pSource2)[0]
                            || ((int*) pSource1)[1] != ((int*) pSource2)[1]
                            || ((int*) pSource1)[2] != ((int*) pSource2)[2]
                            || ((int*) pSource1)[3] != ((int*) pSource2)[3]
                            || ((int*) pSource1)[4] != ((int*) pSource2)[4]
                            || ((int*) pSource1)[5] != ((int*) pSource2)[5]
                            || ((int*) pSource1)[6] != ((int*) pSource2)[6]
                            || ((int*) pSource1)[7] != ((int*) pSource2)[7]
                            )
                            return false;
                        pSource1 += blockSize;
                        pSource2 += blockSize;
                        byteCount -= blockSize;
                    } while (byteCount >= blockSize);
                }
            }

            while (byteCount > 0)
            {
                if (*(pSource1++) != *(pSource2++))
                    return false;
                byteCount--;
            }

            return true;
        }

        #endregion
    }
}