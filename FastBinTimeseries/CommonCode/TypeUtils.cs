#region COPYRIGHT

/*
 *     Copyright 2009-2011 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
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
            return Type.GetType(
                typeName,
                an =>
                    {
                        try
                        {
                            return Assembly.Load(new AssemblyName(an.FullName).Name);
                        }
                        catch
                        {
                            return null;
                        }
                    },
                null);
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
            TAttr[] attributes = customAttrProvider.GetCustomAttributes<TAttr>(true);
            if (attributes.Length > 0)
            {
                if (attributes.Length > 1)
                    throw new ArgumentException(
                        String.Format(
                            "Found {0} (>1) attributes {1} detected for {2}", attributes.Length,
                            typeof (TAttr).Name, customAttrProvider));
                return attributes[0];
            }
            return null;
        }

        public static TAttribute[] GetCustomAttributes<TAttribute>(this ICustomAttributeProvider type, bool inherit)
            where TAttribute : Attribute
        {
            return Array.ConvertAll(
                type.GetCustomAttributes(typeof (TAttribute), inherit), i => (TAttribute) i);
        }

        public static Type TypeResolver(string typeName, params Func<TypeSpec, AssemblyName, Type>[] typeResolvers)
        {
            return TypeSpec.Parse(typeName)
                .Resolve(
                    (ts, an) =>
                        {
                            foreach (var tr in typeResolvers)
                            {
                                Type t = tr(ts, an);
                                if (t != null)
                                    return t;
                            }
                            return null;
                        });
        }

        public static Type DefaultTypeResolver(TypeSpec spec, AssemblyName assemblyName)
        {
            string typeName = spec.Name;
            if (spec.AssemblyName != null)
                typeName += ", " + spec.AssemblyName;
            return GetTypeFromAnyAssemblyVersion(typeName);
        }
    }
}