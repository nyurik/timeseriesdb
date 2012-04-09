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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using JetBrains.Annotations;
using NYurik.TimeSeriesDb.Common;

namespace NYurik.TimeSeriesDb.Serializers
{
    public static class TypeUtils
    {
        public const BindingFlags AllInstanceMembers =
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        public const BindingFlags AllStaticMembers =
            BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

        /// <summary>
        /// In the list of types, find a generic type with the specified generic type definition,
        /// and return the generic type arguments.
        /// </summary>
        /// <param name="types">List of types to search in</param>
        /// <param name="genericType">Generic type to search for, e.g. IEnumerable&lt;&gt;</param>
        /// <returns>List of arguments</returns>
        public static Type[] FindGenericArguments(this IEnumerable<Type> types, Type genericType)
        {
            if (types == null) throw new ArgumentNullException("types");
            if (genericType == null) throw new ArgumentNullException("genericType");
            if (!genericType.IsGenericTypeDefinition)
                throw new ArgumentOutOfRangeException("genericType", genericType, "Must be a generic type definition");

            return (from i in types
                    where i.IsGenericType && i.GetGenericTypeDefinition() == genericType
                    select i.GetGenericArguments()).SingleOrDefault();
        }

        /// <summary>
        /// In the list of types, find a generic one argument type with the specified generic type definition,
        /// and return the first generic type argument.
        /// </summary>
        /// <param name="types">List of types to search in</param>
        /// <param name="genericType">Generic type to search for, e.g. IEnumerable&lt;&gt;</param>
        /// <returns>Generic Argument</returns>
        public static Type FindGenericArgument1(this IEnumerable<Type> types, Type genericType)
        {
            Type[] args = types.FindGenericArguments(genericType);
            return args == null ? null : args[0];
        }

        /// <summary>
        /// Gets a value indicating whether a type (or type's element type)
        /// instance can be null in the underlying data store.
        /// </summary>
        /// <param name="type">A <see cref="System.Type"/> instance. </param>
        /// <returns> True, if the type parameter is a closed generic nullable type; otherwise, False.</returns>
        /// <remarks>Arrays of Nullable types are treated as Nullable types.</remarks>
        public static bool IsNullable(this Type type)
        {
            while (type.IsArray)
                type = type.GetElementType();

            return (type.IsGenericType && type.GetGenericTypeDefinition() == typeof (Nullable<>));
        }

        /// <summary>
        /// Returns the underlying type argument of the specified type.
        /// </summary>
        /// <param name="type">A <see cref="System.Type"/> instance. </param>
        /// <returns><list>
        /// <item>The type argument of the type parameter,
        /// if the type parameter is a closed generic nullable type.</item>
        /// <item>The underlying Type if the type parameter is an enum type.</item>
        /// <item>Otherwise, the type itself.</item>
        /// </list>
        /// </returns>
        public static Type GetUnderlyingType(this Type type)
        {
            if (type == null) throw new ArgumentNullException("type");

            if (type.IsNullable())
                type = type.GetGenericArguments()[0];

            if (type.IsEnum)
                type = Enum.GetUnderlyingType(type);

            return type;
        }

        /// <summary>
        /// Determines whether the specified types are considered equal.
        /// </summary>
        /// <param name="parent">A <see cref="System.Type"/> instance. </param>
        /// <param name="child">A type possible derived from the <c>parent</c> type</param>
        /// <returns>True, when an object instance of the type <c>child</c>
        /// can be used as an object of the type <c>parent</c>; otherwise, false.</returns>
        /// <remarks>Note that nullable types does not have a parent-child relation to it's underlying type.
        /// For example, the 'int?' type (nullable int) and the 'int' type
        /// aren't a parent and it's child.</remarks>
        public static bool IsSameOrParent(Type parent, Type child)
        {
            if (parent == null) throw new ArgumentNullException("parent");
            if (child == null) throw new ArgumentNullException("child");

            if (parent == child ||
                child.IsEnum && Enum.GetUnderlyingType(child) == parent ||
                child.IsSubclassOf(parent))
            {
                return true;
            }

            if (parent.IsInterface)
            {
                return child.GetInterfaces().Any(t => t == parent);
            }

            return false;
        }

        /// <summary>
        /// Substitutes the elements of an array of types for the type parameters
        /// of the current generic type definition and returns a Type object
        /// representing the resulting constructed type.
        /// </summary>
        /// <param name="type">A <see cref="System.Type"/> instance.</param>
        /// <param name="typeArguments">An array of types to be substituted for
        /// the type parameters of the current generic type.</param>
        /// <returns>A Type representing the constructed type formed by substituting
        /// the elements of <paramref name="typeArguments"/> for the type parameters
        /// of the current generic type.</returns>
        /// <seealso cref="System.Type.MakeGenericType"/>
        public static Type TranslateGenericParameters([NotNull] this Type type, Type[] typeArguments)
        {
            if (type == null) throw new ArgumentNullException("type");

            // 'T paramName' case
            //
            if (type.IsGenericParameter)
            {
                if (typeArguments == null) throw new ArgumentNullException("typeArguments");
                return typeArguments[type.GenericParameterPosition];
            }

            // 'List<T> paramName' or something like that.
            //
            if (type.IsGenericType && type.ContainsGenericParameters)
            {
                Type[] genArgs = type.GetGenericArguments();

                for (int i = 0; i < genArgs.Length; ++i)
                    genArgs[i] = TranslateGenericParameters(genArgs[i], typeArguments);

                return type.GetGenericTypeDefinition().MakeGenericType(genArgs);
            }

            // Non-generic type.
            //
            return type;
        }

        public static List<TypeInfo> GenerateTypeSignature(this Type itemType)
        {
            var result = new List<TypeInfo>();
            GenerateTypeSignature(null, itemType, result, 0);
            return result;
        }

        private static void GenerateTypeSignature(
            FieldInfo fieldInfo, Type itemType, ICollection<TypeInfo> result,
            int level)
        {
            TypeInfo? ti = null;
            if (itemType.IsNested && fieldInfo != null)
            {
                object[] ca = fieldInfo.GetCustomAttributes(typeof (FixedBufferAttribute), false);
                if (ca.Length > 0)
                    ti = new TypeInfo(level, ((FixedBufferAttribute) ca[0]).Length);
            }

            result.Add(ti ?? new TypeInfo(level, itemType));

            FieldInfo[] fields = itemType.GetFields(AllInstanceMembers);
            if (fields.Length == 1 && fields[0].FieldType == itemType)
                return;

            foreach (FieldInfo fi in fields)
            {
                if (fi.FieldType == itemType)
                    throw new InvalidOperationException("More than one field refers back to " + itemType.FullName);
                GenerateTypeSignature(fi, fi.FieldType, result, level + 1);
            }
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
        /// An exception is thrown if more than one attribute of a given type is found.
        /// </summary>
        /// <typeparam name="TAttr">Type of the attribute to get</typeparam>
        /// <param name="customAttrProvider">Enum value</param>
        /// <returns>An attribute object or null if not found</returns>
        public static TAttr GetSingleAttribute<TAttr>(this ICustomAttributeProvider customAttrProvider)
            where TAttr : Attribute
        {
            TAttr[] attributes = customAttrProvider.GetCustomAttributes<TAttr>(true);
            if (attributes.Length > 0)
            {
                if (attributes.Length > 1)
                    throw new ArgumentException(
                        String.Format(
                            "{0} attributes [{1}] detected for {2}, only one is allowed",
                            attributes.Length, typeof (TAttr).Name, customAttrProvider));
                return attributes[0];
            }
            return null;
        }

        public static TAttribute[] GetCustomAttributes<TAttribute>(
            [NotNull] this ICustomAttributeProvider type, bool inherit)
            where TAttribute : Attribute
        {
            if (type == null) throw new ArgumentNullException("type");
            return Array.ConvertAll(
                type.GetCustomAttributes(typeof (TAttribute), inherit), i => (TAttribute) i);
        }

        public static Type ResolverFromAnyAssemblyVersion([NotNull] TypeSpec spec, AssemblyName an)
        {
            if (spec == null) throw new ArgumentNullException("spec");
            string typeName = spec.Name;
            if (spec.AssemblyName != null)
                typeName += ", " + spec.AssemblyName;
            return GetTypeFromAnyAssemblyVersion(typeName);
        }

        public static Type ParseAndResolve(string typeName, params Func<TypeSpec, Type>[] fullTypeResolvers)
        {
            return TypeSpec.Parse(typeName).Resolve(fullTypeResolvers);
        }

        public static Func<string, Type> CreateCachingResolver(params Func<TypeSpec, Type>[] fullTypeResolvers)
        {
            if (fullTypeResolvers == null || fullTypeResolvers.Length == 0)
                throw new ArgumentNullException("fullTypeResolvers");
            return new CachingTypeResolver(tn => ParseAndResolve(tn, fullTypeResolvers)).Resolve;
        }

        public static Func<string, Type> CreateCachingResolver(
            params Func<TypeSpec, AssemblyName, Type>[] typeResolvers)
        {
            if (typeResolvers == null || typeResolvers.Length == 0)
                throw new ArgumentNullException("typeResolvers");
            return new CachingTypeResolver(
                tn => ParseAndResolve(
                    tn, ts => TypeSpec.DefaultFullTypeResolver(ts, typeResolvers))).Resolve;
        }

        internal static void TraverseTypeTree(Type type, Action<FieldInfo, Type> validateType)
        {
            TraverseTypeTree(null, type, validateType, new Stack<Type>(4));
        }

        private static void TraverseTypeTree(FieldInfo fi, Type type, Action<FieldInfo, Type> validateType,
                                             Stack<Type> typesStack)
        {
            validateType(fi, type);

            try
            {
                typesStack.Push(type);

                if (!type.IsPrimitive)
                {
                    FieldInfo[] fields = type.GetFields(AllInstanceMembers);

                    foreach (FieldInfo f in fields)
                        if (!typesStack.Contains(f.FieldType))
                            TraverseTypeTree(f, f.FieldType, validateType, typesStack);
                        else
                            validateType(fi, null); // Indicate recursive field with already-traversed type
                }
            }
            catch (Exception ex)
            {
                throw new SerializerException(ex, "Error in subtype of type {0}. See InnerException.", type.FullName);
            }
            finally
            {
                typesStack.Pop();
            }
        }

        /// <summary>
        /// When used with <see cref="TraverseTypeTree(System.Type,System.Action{System.Reflection.FieldInfo,System.Type})"/>,
        /// recursively checks if the type with all of its members are expressable as a value type that may be cast to a pointer.
        /// Equivalent to what compiler does to check for CS0208 error of this statement:
        ///        fixed (int* p = new int[5]) {}
        /// 
        /// An unmanaged-type is any type that isn’t a reference-type and doesn’t contain reference-type fields 
        /// at any level of nesting. In other words, an unmanaged-type is one of the following:
        ///  * sbyte, byte, short, ushort, int, uint, long, ulong, char, float, double, decimal, or bool.
        ///  * Any enum-type.
        ///  * Any pointer-type.
        ///  * Any user-defined struct-type that contains fields of unmanaged-types only.
        /// 
        /// Strings are not in that list, even though you can use them in structs. 
        /// Fixed-size arrays of unmanaged-types are allowed.
        /// </summary>
        internal static void ValidateNoRefStruct(FieldInfo fi, Type type)
        {
            if ((!type.IsValueType && !type.IsPointer) || type.IsGenericType || type.IsGenericParameter || type.IsArray)
                throw new SerializerException(
                    "Type {0}{1} is not an unmanaged type", type.FullName, fi == null ? "" : " in field " + fi.Name);

            if (!type.IsPrimitive && !type.IsEnum && !type.IsPointer)
                for (Type p = type.DeclaringType; p != null; p = p.DeclaringType)
                    if (p.IsGenericTypeDefinition)
                        throw new SerializerException(
                            "Type {0}{1} contains a generic type definition declaring type {2}", type.FullName,
                            fi == null ? "" : " in field " + fi.Name, p.FullName);
            if (type.StructLayoutAttribute == null ||
                (type.StructLayoutAttribute.Value != LayoutKind.Explicit &&
                 type.StructLayoutAttribute.Value != LayoutKind.Sequential) || type.StructLayoutAttribute.Pack == 0)
            {
                throw new SerializerException(
                    "The type {0}{1} does not have a StructLayout attribute, or is set to Auto, or the Pack is 0",
                    type.FullName, fi == null ? "" : " in field " + fi.Name);
            }
        }

        public static TypeCode GetTypeCode(this Type type)
        {
            return Type.GetTypeCode(type);
        }

        #region Nested type: CachingTypeResolver

        private class CachingTypeResolver
        {
            private readonly ConcurrentDictionary<string, Type> _cache = new ConcurrentDictionary<string, Type>();
            private readonly Func<string, Type> _valueFactory;

            public CachingTypeResolver([NotNull] Func<string, Type> valueFactory)
            {
                if (valueFactory == null) throw new ArgumentNullException("valueFactory");
                _valueFactory = valueFactory;
            }

            public Type Resolve(string typeName)
            {
                return _cache.GetOrAdd(typeName, _valueFactory);
            }
        }

        #endregion

        #region Nested type: TypeInfo

        public struct TypeInfo : IEquatable<TypeInfo>
        {
            public readonly int FixedBufferSize;
            public readonly int Level;
            public readonly Type Type;

            public TypeInfo(int level, Type type)
            {
                Level = level;
                Type = type;
                FixedBufferSize = -1;
            }

            public TypeInfo(int level, int fixedBufferSize)
            {
                Level = level;
                Type = null;
                FixedBufferSize = fixedBufferSize;
            }

            #region IEquatable<TypeInfo> Members

            public bool Equals(TypeInfo other)
            {
                return other.Level == Level
                       && (other.Type == Type || Type == null || other.Type == null)
                       &&
                       (other.FixedBufferSize == FixedBufferSize || FixedBufferSize == -1 || other.FixedBufferSize == -1);
            }

            #endregion

            public static bool operator ==(TypeInfo left, TypeInfo right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(TypeInfo left, TypeInfo right)
            {
                return !left.Equals(right);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (obj.GetType() != typeof (TypeInfo)) return false;
                return Equals((TypeInfo) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    int result = Level;
                    // Do not include Type or FixedBufferSize -- see Equals() method
                    return result;
                }
            }

            public override string ToString()
            {
                return
                    Type != null
                        ? String.Format("Level: {0}, Type: {1}", Level, Type)
                        : String.Format("Level: {0}, FixedBufferSize: {1}", Level, FixedBufferSize);
            }
        }

        #endregion

        public static string ToDebugStr(this Type type)
        {
            if (type == null)
                return "Unknown (null)";
            var fn = type.FullName;
            if (fn == null)
                return "Unknown";
            if (fn.StartsWith("System."))
                return fn;
            return type.AssemblyQualifiedName;
        }

        public static Type PropOrFieldType(this MemberInfo mi)
        {
            var fi = mi as FieldInfo;
            if (fi != null) return fi.FieldType;
            var pi = mi as PropertyInfo;
            if (pi != null) return pi.PropertyType;
            throw new InvalidOperationException("MemberInfo is not a field nor a property");
        }
    }
}