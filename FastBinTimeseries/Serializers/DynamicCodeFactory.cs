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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Serializers
{
    public class DynamicCodeFactory
    {
        public static readonly Lazy<DynamicCodeFactory> Instance =
            new Lazy<DynamicCodeFactory>(() => new DynamicCodeFactory());

        private readonly ConcurrentDictionary<Type, BinSerializerInfo> _serializers =
            new ConcurrentDictionary<Type, BinSerializerInfo>();

        private readonly ConcurrentDictionary<FieldInfo, Delegate> _tsAccessorCache =
            new ConcurrentDictionary<FieldInfo, Delegate>();

//        private readonly ConcurrentDictionary<Type, Delegate> _tsComparatorCache =
//            new ConcurrentDictionary<Type, Delegate>();
//
        private readonly ConcurrentDictionary<Type, FieldInfo> _tsFieldsCache =
            new ConcurrentDictionary<Type, FieldInfo>();

        private DynamicCodeFactory()
        {
        }

        private static MethodInfo GetMethodInfo(Type baseType, string methodName)
        {
            MethodInfo methodToCall = baseType.GetMethod(methodName, TypeExtensions.AllInstanceMembers);
            if (methodToCall == null)
                throw new SerializerException(
                    "Method {0} not found in the base type {1}", methodName, baseType.FullName);
            return methodToCall;
        }

        /// <summary>
        /// Recursively checks if the type with all of its members are expressable as a value type that may be cast to a pointer.
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
        private static void ThrowIfNotUnmanagedType(Type type)
        {
            ThrowIfNotUnmanagedType(type, new Stack<Type>(4));
        }

        private static void ThrowIfNotUnmanagedType(Type type, Stack<Type> typesStack)
        {
            if ((!type.IsValueType && !type.IsPointer) || type.IsGenericType || type.IsGenericParameter || type.IsArray)
                throw new SerializerException("Type {0} is not an unmanaged type", type.FullName);

            if (!type.IsPrimitive && !type.IsEnum && !type.IsPointer)
                for (Type p = type.DeclaringType; p != null; p = p.DeclaringType)
                    if (p.IsGenericTypeDefinition)
                        throw new SerializerException(
                            "Type {0} contains a generic type definition declaring type {1}", type.FullName, p.FullName);
            if (type.StructLayoutAttribute == null
                || (type.StructLayoutAttribute.Value != LayoutKind.Explicit
                    && type.StructLayoutAttribute.Value != LayoutKind.Sequential)
                || type.StructLayoutAttribute.Pack == 0
                )
            {
                throw new SerializerException(
                    "The type {0} does not have a StructLayout attribute, or is set to Auto, or the Pack is 0",
                    type.FullName);
            }

            try
            {
                typesStack.Push(type);

                FieldInfo[] fields = type.GetFields(TypeExtensions.AllInstanceMembers);

                foreach (FieldInfo f in fields)
                    if (!typesStack.Contains(f.FieldType))
                        ThrowIfNotUnmanagedType(f.FieldType, typesStack);
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

        #region Serializer

        internal BinSerializerInfo CreateSerializer<T>()
        {
            return _serializers.GetOrAdd(
                typeof (T),
                t =>
                    {
                        // Parameter validation
                        ThrowIfNotUnmanagedType(t);

                        Type ifType = typeof (DefaultTypeSerializer<>).MakeGenericType(t);

                        // Create the abstract method overrides
                        return new BinSerializerInfo(
                            (int) CreateSizeOfMethod(t, ifType.Module).Invoke(null, null),
                            CreateSerializerMethod(
                                t, ifType, "DynProcessFileStream", "ProcessFileStreamPtr", typeof (FileStream)),
                            CreateSerializerMethod(
                                t, ifType, "DynProcessMemoryMap", "ProcessMemoryMapPtr", typeof (IntPtr)),
                            CreateMemComparerMethod(t, ifType)
                            );
                    });
        }

        private static DynamicMethod CreateSizeOfMethod(Type itemType, Module module)
        {
            var method = new DynamicMethod("SizeOf", typeof (int), null, module, true);
            ILGenerator emit = method.GetILGenerator();

            emit
                .@sizeof(itemType)
                .ret();

            return method;
        }

        private static DynamicMethod CreateSerializerMethod(Type itemType, Type baseType, string methodName,
                                                            string methodToCallName, Type firstParamType)
        {
            MethodInfo methodToCall = GetMethodInfo(baseType, methodToCallName);

            var method = new DynamicMethod(
                methodName,
                typeof (int),
                new[] {baseType, firstParamType, itemType.MakeArrayType(), typeof (int), typeof (int), typeof (bool)},
                baseType.Module,
                true);

            ILGenerator emit = method.GetILGenerator();

            //                .locals init (
            //                    [0] void& pinned bufPtr)
            //                    [1] int32 CS$1$0000)
            LocalBuilder bufPtr = emit.DeclareLocal(typeof (void).MakeByRefType(), true);
            LocalBuilder retVal = emit.DeclareLocal(typeof (int));


            Label l0019 = emit.DefineLabel();

            // Argument index: 
            // 0 - this
            // 1 - FileStream or IntPtr
            // 2 - void* bufPtr
            // 3 - int offset
            // 4 - int count
            // 5 - bool isWriting
            emit
                .ldarg_2() //         L_0000: ldarg.2 
                .ldc_i4_0() //        L_0001: ldc.i4.0 
                .ldelema(itemType) // L_0002: ldelema _MyItemType_
                .stloc(bufPtr) //     L_0007: stloc.0 
                .ldarg_0() //         L_0008: ldarg.0 
                .ldarg_1() //         L_0009: ldarg.1 
                .ldloc(bufPtr) //     L_000a: ldloc.0 
                .conv_i() //          L_000b: conv.i 
                .ldarg_3() //         L_000c: ldarg.3 
                .ldarg_s(4) //        L_000d: ldarg.s count
                .ldarg_s(5) //        L_000f: ldarg.s isWriting
                .call(methodToCall) //L_0011: call instance ... (our method)
                //.ldc_i4_0() //        L_0016: ldc.i4.0 
                //.conv_u() //          L_0017: conv.u 
                //.stloc(bufPtr) //     L_0018: stloc.0 
                //.ret() //             L_0019: ret 
                .stloc(retVal) //     L_0016: stloc.1 
                .leave_s(l0019) //    L_0017: leave.s L_0019
                .MarkLabelExt(l0019)
                .ldloc(retVal) //     L_0019: ldloc.1 
                .ret() //             L_001a: ret 
                ;

            return method;
        }

        private static DynamicMethod CreateMemComparerMethod(Type itemType, Type baseType)
        {
            MethodInfo methodToCall = GetMethodInfo(baseType, "CompareMemoryPtr");

            var method = new DynamicMethod(
                "DynCompareMemory",
                typeof (bool),
                new[]
                    {
                        baseType, itemType.MakeArrayType(), typeof (int), itemType.MakeArrayType(), typeof (int),
                        typeof (int)
                    },
                baseType.Module,
                true);

            ILGenerator emit = method.GetILGenerator();

            //.method private hidebysig instance bool Foo(uint8[] buffer1, int32 offset1, uint8[] buffer2, int32 offset2, int32 count) cil managed
            //{
            //    .maxstack 6
            //    .locals init (
            //        [0] uint8& pinned p1,
            //        [1] uint8& pinned p2,
            //        [2] bool CS$1$0000)
            emit.DeclareLocal(typeof (void).MakeByRefType(), true);
            emit.DeclareLocal(typeof (void).MakeByRefType(), true);
            emit.DeclareLocal(typeof (bool));

            // Argument index: 
            // 0 - this
            // 1 - void* bufPtr1
            // 2 - int offset1
            // 3 - void* bufPtr2
            // 4 - int offset2
            // 5 - int count
            Label l0022 = emit.DefineLabel();
            emit
                .ldarg_1() //           L_0000: ldarg.1 
                .ldc_i4_0() //          L_0001: ldc.i4.0 
                .ldelema(itemType) //   L_0002: ldelema _MyItemType_
                .stloc_0() //           L_0007: stloc.0 
                .ldarg_3() //           L_0008: ldarg.3 
                .ldc_i4_0() //          L_0009: ldc.i4.0 
                .ldelema(itemType) //   L_000a: ldelema _MyItemType_
                .stloc_1() //           L_000f: stloc.1 
                .ldarg_0() //           L_0010: ldarg.0 
                .ldloc_0() //           L_0011: ldloc.0 
                .conv_i() //            L_0012: conv.i 
                .ldarg_2() //           L_0013: ldarg.2 
                .ldloc_1() //           L_0014: ldloc.1 
                .conv_i() //            L_0015: conv.i 
                .ldarg_s(4) //          L_0016: ldarg.s offset2
                .ldarg_s(5) //          L_0018: ldarg.s count
                .call(methodToCall) //  L_001a: call instance ... (our method)
                .stloc_2() //           L_001f: stloc.2 
                .leave_s(l0022) //      L_0020: leave.s L_0022
                .MarkLabelExt(l0022)
                .ldloc_2() //           L_0022: ldloc.2 
                .ret() //               L_0023: ret 
                ;

            return method;
        }

        #endregion

        #region Index Field and Accessor

        /// <summary>
        /// Find default timestamp field's <see cref="FieldInfo"/> for type T.
        /// </summary>
        public FieldInfo GetIndexField<T>()
        {
            return GetIndexField(typeof (T));
        }

        /// <summary>
        /// Find default timestamp field's <see cref="FieldInfo"/> for <param name="type"/>.
        /// </summary>
        public FieldInfo GetIndexField(Type type)
        {
            FieldInfo res = FindIndexField(type);
            if (res == null)
                throw new SerializerException("No field of indexable type was found in type {0}", type.FullName);
            return res;
        }

        /// <summary>
        /// Find default timestamp field's <see cref="FieldInfo"/> for <param name="type"/>, or null if not found.
        /// </summary>
        public FieldInfo FindIndexField(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            FieldInfo res = _tsFieldsCache.GetOrAdd(
                type,
                t =>
                    {
                        FieldInfo[] fieldInfo = t.GetFields(TypeExtensions.AllInstanceMembers);
                        if (fieldInfo.Length < 1)
                            throw new SerializerException("No fields found in type {0}", t.FullName);

                        FieldInfo result = null;
                        bool foundTsAttribute = false;
                        bool foundMultiple = false;
                        foreach (FieldInfo fi in fieldInfo)
                        {
                            bool hasAttr = fi.ExtractSingleAttribute<TimestampAttribute>() != null;
                            if (hasAttr || fi.FieldType == typeof (UtcDateTime))
                            {
                                if (hasAttr)
                                {
                                    if (foundTsAttribute)
                                        throw new SerializerException(
                                            "More than one field has an attribute [{0}] attached in type {1}",
                                            typeof (TimestampAttribute).Name, t.FullName);
                                    foundTsAttribute = true;
                                    result = fi;
                                }
                                else if (!foundTsAttribute)
                                {
                                    if (result != null)
                                        foundMultiple = true;
                                    result = fi;
                                }
                            }
                        }

                        if (foundMultiple)
                            throw new InvalidOperationException(
                                "Must explicitly specify the fieldInfo because there is more than one UtcDateTime field in type " +
                                t.FullName);

                        return result;
                    }
                );
            return res;
        }

        /// <summary>
        /// Create a delegate that extracts a long index value from the struct of type T.
        /// </summary>
        /// <param name="tsField">Optionally provide the index field, otherwise will attempt to find default.</param>
        public Func<T, TInd> GetIndexAccessor<T, TInd>(FieldInfo tsField = null)
        {
            return
                (Func<T, TInd>)
                _tsAccessorCache.GetOrAdd(
                    tsField ?? GetIndexField<T>(),
                    fi =>
                        {
                            Type itemType = typeof (T);
                            if (fi.DeclaringType != itemType)
                                throw new InvalidOperationException(
                                    String.Format(
                                        "The field {0} does not belong to type {1}",
                                        fi.Name, itemType.FullName));

                            ParameterExpression vParam = Expression.Parameter(itemType, "v");
                            Expression expr = Expression.Field(vParam, fi);

                            return Expression.Lambda<Func<T, TInd>>(expr, vParam).Compile();
                        }
                    );
        }

        #endregion

        #region Nested type: BinSerializerInfo

        internal class BinSerializerInfo
        {
            public readonly DynamicMethod FileStreamMethod;
            public readonly DynamicMethod MemCompareMethod;
            public readonly DynamicMethod MemPtrMethod;
            public readonly int TypeSize;

            public BinSerializerInfo(int typeSize, DynamicMethod fileStreamMethod, DynamicMethod memPtrMethod,
                                     DynamicMethod memCompareMethod)
            {
                TypeSize = typeSize;
                FileStreamMethod = fileStreamMethod;
                MemPtrMethod = memPtrMethod;
                MemCompareMethod = memCompareMethod;
            }
        }

        #endregion
    }
}