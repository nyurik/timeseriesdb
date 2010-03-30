using System;
using System.Collections.Generic;
using System.IO;
using NYurik.FastBinTimeseries.CommonCode;

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

        public static long RoundUpToMultiple(long value, long multiple)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException("value", value, "Value must be >= 0");
            if (value == 0)
                return 0;
            return value - 1 + (multiple - (value - 1)%multiple);
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
                        ((long*) pDestination)[0] = ((long*) pSource)[0];
                        ((long*) pDestination)[1] = ((long*) pSource)[1];
                        ((long*) pDestination)[2] = ((long*) pSource)[2];
                        ((long*) pDestination)[3] = ((long*) pSource)[3];
                        pDestination += blockSize;
                        pSource += blockSize;
                        byteCount -= blockSize;
                    } while (byteCount >= blockSize);
                }
                else
                {
                    do
                    {
                        ((int*) pDestination)[0] = ((int*) pSource)[0];
                        ((int*) pDestination)[1] = ((int*) pSource)[1];
                        ((int*) pDestination)[2] = ((int*) pSource)[2];
                        ((int*) pDestination)[3] = ((int*) pSource)[3];
                        ((int*) pDestination)[4] = ((int*) pSource)[4];
                        ((int*) pDestination)[5] = ((int*) pSource)[5];
                        ((int*) pDestination)[6] = ((int*) pSource)[6];
                        ((int*) pDestination)[7] = ((int*) pSource)[7];
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

        public static Exception GetUnknownVersionException(Version version, Type type)
        {
            return new ArgumentOutOfRangeException("version", version, "Unknown version for " + type.FullName);
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

        public static T ReadTypeAndInstantiate<T>(this BinaryReader reader, IDictionary<string, Type> typeMap, bool nonPublic) where T : class
        {
            if (reader == null) throw new ArgumentNullException("reader");

            Type type;
            bool typeFound = false;

            string typeName = reader.ReadString();
            if (typeMap == null || !typeMap.TryGetValue(typeName, out type))
            {
                type = TypeUtils.GetTypeFromAnyAssemblyVersion(typeName);
                typeFound = true;
            }

            if (type == null)
                throw new InvalidOperationException("Unable to find type " + typeName);

            var instance = Activator.CreateInstance(type, nonPublic) as T;
            if (instance == null)
                throw new InvalidOperationException(
                    String.Format("Type {0}{1} cannot be cast into {2}", type.AssemblyQualifiedName,
                                  typeFound ? "" : " (re-mapped from " + typeName + ")",
                                  typeof (T).AssemblyQualifiedName));
            return instance;
        }

        public static void WriteType(this BinaryWriter writer, object value)
        {
            if (writer == null) throw new ArgumentNullException("writer");
            if (value == null) throw new ArgumentNullException("value");
            writer.Write(value.GetType().AssemblyQualifiedName);
        }

        ///// <summary> Write version plus custom header generated by the writeHeaderMethod into the stream </summary>
        //public static Version WriteHeaderWithVersion(this BinaryWriter writer, Func<BinaryWriter, Version> writeHeaderMethod)
        //{
        //    // Record original postition and write dummy version
        //    long versionPos = writer.BaseStream.Position;
        //    WriteVersion(writer, VersionMaxValue);

        //    // Write real version and save final position
        //    Version version = writeHeaderMethod(writer);
        //    long latestPos = writer.BaseStream.Position;

        //    // Seek back, rerecord the proper version instead of the dummy one, and move back to the end
        //    writer.BaseStream.Seek(versionPos, SeekOrigin.Begin);
        //    WriteVersion(writer, version);
        //    writer.BaseStream.Seek(latestPos, SeekOrigin.Begin);

        //    return version;
        //}
    }
}