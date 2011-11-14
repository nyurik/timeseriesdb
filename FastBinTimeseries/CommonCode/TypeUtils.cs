using System;
using System.Reflection;

namespace NYurik.FastBinTimeseries.CommonCode
{
    public static class TypeUtils
    {
        public static string GetUnversionedNameAssembly(this Type type)
        {
            return type.FullName + ", " + type.Assembly.GetName().Name;
        }

        /// <summary>
        /// Load type using <see cref="Type.GetType(string)"/>, and if fails, 
        /// attempt to load same type from an assembly by assembly name, 
        /// without specifying assembly version or any other part of the signature
        /// </summary>
        /// <param name="typeName">
        /// The assembly-qualified name of the type to get (<see cref="System.Type.AssemblyQualifiedName"/>).
        /// If the type is in the currently executing assembly or in Mscorlib.dll, it 
        /// is sufficient to supply the type name qualified by its namespace.
        /// </param>
        public static Type GetTypeFromAnyAssemblyVersion(string typeName)
        {
            // If we were unable to resolve type object - possibly because of the version change
            // Try to load using just the assembly name, without any version/culture/public key info
            ResolveEventHandler assemblyResolve = OnAssemblyResolve;

            try
            {
                // Attach our custom assembly name resolver, attempt to resolve again, and detach it
                AppDomain.CurrentDomain.AssemblyResolve += assemblyResolve;
                _isGetTypeRunningOnThisThread = true;
                return Type.GetType(typeName);
            }
            finally
            {
                _isGetTypeRunningOnThisThread = false;
                AppDomain.CurrentDomain.AssemblyResolve -= assemblyResolve;
            }
        }

        /// <summary>
        /// Get a single attribute (or null) of a given type attached to a value.
        /// The value might be a <see cref="Type"/> object or Property/Method/... info acquired through reflection.
        /// An exception is thrown if more than one attribute of a given type was found.
        /// </summary>
        /// <typeparam name="TAttr">Type of the attribute to get</typeparam>
        /// <param name="customAttrProvider">Enum value</param>
        /// <returns>An attribute object or null if not found</returns>
        public static TAttr ExtractSingleAttribute<TAttr>(this ICustomAttributeProvider customAttrProvider)
            where TAttr : Attribute
        {
            object[] attributes = customAttrProvider.GetCustomAttributes<TAttr>(true);
            if (attributes.Length > 0)
            {
                if (attributes.Length > 1)
                    throw new ArgumentException(
                        String.Format(
                            "Found {0} (>1) attributes {1} detected for {2}", attributes.Length,
                            typeof (TAttr).Name, customAttrProvider));
                return (TAttr) attributes[0];
            }
            return null;
        }

        public static TAttribute[] GetCustomAttributes<TAttribute>(this ICustomAttributeProvider type, bool inherit)
            where TAttribute : Attribute
        {
            return Array.ConvertAll(
                type.GetCustomAttributes(typeof (TAttribute), inherit), i => (TAttribute) i);
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
    }
}