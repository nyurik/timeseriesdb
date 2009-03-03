using System;
using System.IO;
using System.Reflection;

namespace NYurik.FastBinTimeseries
{
    internal static class Utilities
    {
        /// <summary>
        /// Get an array of attributes of a given type attached to an Enum value.
        /// </summary>
        /// <typeparam name="TAttr">Type of attributes to get</typeparam>
        /// <typeparam name="TEnum">Type of the Enum value</typeparam>
        /// <param name="value">Enum value</param>
        /// <returns>An array of attributes of a given type. May be empty</returns>
        public static TAttr[] GetEnumAttributes<TAttr, TEnum>(TEnum value)
            where TAttr : Attribute
        {
            var enumType = typeof (TEnum);
            var name = Enum.GetName(enumType, value);
            return Array.ConvertAll(
                enumType.GetField(name).GetCustomAttributes(typeof (TAttr), false), input => (TAttr) input);
        }

        /// <summary>
        /// Get an attribute of a given type attached to an Enum value.
        /// Throws an exception if more than one attribute of a given type was found.
        /// </summary>
        /// <typeparam name="TAttr">Type of the attribute to get</typeparam>
        /// <typeparam name="TEnum">Type of the Enum value</typeparam>
        /// <param name="value">Enum value</param>
        /// <returns>An attribute object or null if not found</returns>
        public static TAttr GetEnumSingleAttribute<TAttr, TEnum>(TEnum value)
            where TAttr : Attribute
        {
            var enumType = typeof (TEnum);
            var name = Enum.GetName(enumType, value);
            return ExtractSingleAttribute<TAttr>(enumType.GetField(name));
        }

        /// <summary>
        /// Get a single attribute (or null) of a given type attached to a value.
        /// The value might be a <see cref="Type"/> object or Property/Method/... info acquired through reflection.
        /// An exception is thrown if more than one attribute of a given type was found.
        /// </summary>
        /// <typeparam name="TAttr">Type of the attribute to get</typeparam>
        /// <param name="customAttrProvider">Enum value</param>
        /// <returns>An attribute object or null if not found</returns>
        public static TAttr ExtractSingleAttribute<TAttr>(ICustomAttributeProvider customAttrProvider)
            where TAttr : Attribute
        {
            var attributes = customAttrProvider.GetCustomAttributes(typeof (TAttr), true);
            if (attributes.Length > 0)
            {
                if (attributes.Length > 1)
                    throw new ArgumentException(
                        String.Format("Found {0} (>1) attributes {1} detected for {2}", attributes.Length,
                                      typeof (TAttr).Name, customAttrProvider));
                return (TAttr) attributes[0];
            }
            return null;
        }

        /// <summary>
        /// Load type using <see cref="Type.GetType(string)"/>, and if fails, 
        /// attempt to load same type from an assembly by assembly name, 
        /// without specifying assembly version or any other part of the signature
        /// </summary>
        /// <param name="typeName">
        /// The assembly-qualified name of the type to get. See System.Type.AssemblyQualifiedName.
        /// If the type is in the currently executing assembly or in Mscorlib.dll, it 
        /// is sufficient to supply the type name qualified by its namespace.
        /// </param>
        public static Type GetTypeFromAnyAssemblyVersion(string typeName)
        {
            try
            {
                // We were unable to resolve type object - possibly because of the version change
                // Try to load using just the assembly name, without any version/culture/public key info
                ResolveEventHandler resolve = OnAssemblyResolve;

                // Attach our custom assembly name resolver, attempt to resolve again, and detach it
                AppDomain.CurrentDomain.AssemblyResolve += resolve;
                _isGetTypeRunningOnThisThread = true;
                var type = Type.GetType(typeName);
                AppDomain.CurrentDomain.AssemblyResolve -= resolve;

                return type;
            }
            finally
            {
                _isGetTypeRunningOnThisThread = false;
            }
        }

        public static void ValidateArrayParams(Array buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException("buffer");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset", offset, "<0");
            if (count < 0)
                throw new ArgumentOutOfRangeException("count", count, "<0");
            if ((buffer.Length - offset) < count)
                throw new ArgumentException(
                    String.Format("Cannot access array of size {0} at offset {1} for {2} items",
                                  buffer.Length, offset, count));
        }

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
                if (Win32Apis.Is64bit)
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

        public static void ThrowUnknownVersion(Version version, Type type)
        {
            throw new ArgumentOutOfRangeException("version", version, "Unknown version for " + type.FullName);
        }

        #region Helper methods

        [ThreadStatic] private static bool _isGetTypeRunningOnThisThread;

        private static Assembly OnAssemblyResolve(object sender, ResolveEventArgs args)
        {
            Assembly assembly = null;

            // Only process events from the thread that started it, not any other thread
            if (_isGetTypeRunningOnThisThread)
            {
                // Extract assembly name, and checking it's the same as args.Name to prevent an infinite loop
                var an = new AssemblyName(args.Name);
                if (an.Name != args.Name)
                    assembly = ((AppDomain) sender).Load(an.Name);
            }

            return assembly;
        }

        #endregion

        public static Version ReadVersion(BinaryReader reader)
        {
            var major = reader.ReadInt32();
            var minor = reader.ReadInt32();
            var build = reader.ReadInt32();
            var revision = reader.ReadInt32();

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
    }
}