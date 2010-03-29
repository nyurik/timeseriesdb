using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.InteropServices;
using NYurik.FastBinTimeseries;

namespace NYurik.EmitExtensions
{
    public static class TypeExtensions
    {
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
                Type[] interfaces = child.GetInterfaces();

                foreach (Type t in interfaces)
                    if (t == parent)
                        return true;
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
        public static Type TranslateGenericParameters(this Type type, Type[] typeArguments)
        {
            // 'T paramName' case
            //
            if (type.IsGenericParameter)
                return typeArguments[type.GenericParameterPosition];

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

        public static List<string> GenerateTypeSignature(this Type subItemType)
        {
            var result = new List<Type>();
            GenerateTypeSignature(subItemType, result);
            return result.ConvertAll(i => Marshal.SizeOf(i).ToString() + "_" + i.FullName);
        }

        private static void GenerateTypeSignature(Type subItemType, ICollection<Type> result)
        {
            result.Add(subItemType);

            FieldInfo[] fields = subItemType.GetFields(DynamicCodeFactory.AllInstanceMembers);
            if (fields.Length == 1 && fields[0].FieldType == subItemType)
                return;

            foreach (FieldInfo fi in fields)
            {
                if (fi.FieldType == subItemType)
                    throw new InvalidOperationException("More than one field refers back to " + subItemType.FullName);
                GenerateTypeSignature(fi.FieldType, result);
            }
        }
    }
}