using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public static class LegacySupport
    {
        private static readonly Assembly MainAssembly = typeof (BinaryFile).Assembly;

        private static readonly AssemblyName MainAssemblyName = MainAssembly.GetName();

        private static readonly Dictionary<string, Type> LegacyTypes;

        static LegacySupport()
        {
            // public, non-abstract, non-static class
            LegacyTypes =
                (from type in typeof (LegacySupport).Assembly.GetTypes()
                 where type.IsPublic && !type.IsAbstract
                 select type).ToDictionary(i => i.FullName);
        }

        public static Type TypeResolver(string typeName)
        {
            return TypeUtils.TypeResolver(typeName, TypeResolver, TypeUtils.DefaultTypeResolver);
        }

        public static Type TypeResolver(TypeSpec spec, AssemblyName assemblyName)
        {
            Type type;
            if (assemblyName != null && assemblyName.Name == MainAssemblyName.Name &&
                LegacyTypes.TryGetValue(spec.Name, out type))
                return type;

            return null;
        }
    }
}