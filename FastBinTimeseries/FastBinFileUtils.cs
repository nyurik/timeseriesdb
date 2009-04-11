using System;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    internal static class FastBinFileUtils
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
        public static unsafe void CopyMemory(byte* pDestination, byte* pSource, uint byteCount)
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
        public static unsafe bool CompareMemory(byte* pSource1, byte* pSource2, uint byteCount)
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

        public static void ThrowUnknownVersion(Version version, Type type)
        {
            throw new ArgumentOutOfRangeException("version", version, "Unknown version for " + type.FullName);
        }

        public static Version ReadVersion(BinaryReader reader)
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

        public static void WriteVersion(BinaryWriter writer, Version ver)
        {
            writer.Write(ver.Major);
            writer.Write(ver.Minor);
            writer.Write(ver.Build);
            writer.Write(ver.Revision);
        }

        public static int ToInt32Checked(this long itemsCountLng)
        {
            if (itemsCountLng > Int32.MaxValue)
                throw new ArgumentException(
                    String.Format(
                        "Attempted to process {0} items at once, which is over the maximum of {1}.",
                        itemsCountLng, Int32.MaxValue));
            return (int) itemsCountLng;
        }
    }
}