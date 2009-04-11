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
        /// The assembly-qualified name of the type to get. See System.Type.AssemblyQualifiedName.
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
                s_isGetTypeRunningOnThisThread = true;
                return Type.GetType(typeName);
            }
            finally
            {
                s_isGetTypeRunningOnThisThread = false;
                AppDomain.CurrentDomain.AssemblyResolve -= assemblyResolve;
            }
        }

        #region Helper methods

        [ThreadStatic] private static bool s_isGetTypeRunningOnThisThread;

        private static Assembly OnAssemblyResolve(object sender, ResolveEventArgs args)
        {
            Assembly assembly = null;

            // Only process events from the thread that started it, not any other thread
            if (s_isGetTypeRunningOnThisThread)
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