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
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using NYurik.TimeSeriesDb.Common;

namespace NYurik.TimeSeriesDb.Serializers
{
    public class DynamicCodeFactory
    {
        public static readonly Lazy<DynamicCodeFactory> Instance =
            new Lazy<DynamicCodeFactory>(() => new DynamicCodeFactory());

        private readonly ConcurrentDictionary<FieldInfo, Delegate> _indAccessorCache =
            new ConcurrentDictionary<FieldInfo, Delegate>();

        private readonly ConcurrentDictionary<Type, FieldInfo> _indFieldsCache =
            new ConcurrentDictionary<Type, FieldInfo>();

        private readonly ConcurrentDictionary<Type, BinSerializerInfo> _serializers =
            new ConcurrentDictionary<Type, BinSerializerInfo>();

        private DynamicCodeFactory()
        {
        }

        private static MethodInfo GetMethodInfo(Type baseType, string methodName)
        {
            MethodInfo methodToCall = baseType.GetMethod(methodName, TypeUtils.AllInstanceMembers);
            if (methodToCall == null)
                throw new SerializerException(
                    "Method {0} not found in the base type {1}", methodName, baseType.FullName);
            return methodToCall;
        }

        #region Serializer

        internal BinSerializerInfo CreateSerializer<T>()
        {
            return CreateSerializer(typeof (T));
        }

        internal BinSerializerInfo CreateSerializer(Type type)
        {
            return _serializers.GetOrAdd(
                type,
                t =>
                    {
                        // Parameter validation
                        TypeUtils.TraverseTypeTree(t, TypeUtils.ValidateNoRefStruct);

                        Type ifType = typeof (DefaultTypeSerializer<>).MakeGenericType(t);

                        // Create the abstract method overrides
                        return new BinSerializerInfo(
                            GetTypeSize(t, ifType.Module),
                            CreateSerializerMethod(
                                t, ifType, "DynProcessFileStream", "ProcessFileStreamPtr", typeof (FileStream)),
                            CreateSerializerMethod(
                                t, ifType, "DynProcessMemoryMap", "ProcessMemoryMapPtr", typeof (IntPtr)),
                            CreateMemComparerMethod(t, ifType)
                            );
                    });
        }

        internal static int GetTypeSize(Type type, Module module)
        {
            return (int) CreateSizeOfMethod(type, module).Invoke(null, null);
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

        private static DynamicMethod CreateSerializerMethod(
            Type itemType, Type baseType, string methodName,
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
                throw new SerializerException("No field of indexable type was found in type " + type.FullName);
            return res;
        }

        /// <summary>
        /// Find default timestamp field's <see cref="FieldInfo"/> for <param name="type"/>, or null if not found.
        /// </summary>
        public FieldInfo FindIndexField(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            FieldInfo res = _indFieldsCache.GetOrAdd(
                type,
                t =>
                    {
                        FieldInfo[] fieldInfo = t.GetFields(TypeUtils.AllInstanceMembers);
                        if (fieldInfo.Length < 1)
                            throw new SerializerException("No fields found in type " + t.FullName);

                        FieldInfo result = null;
                        bool foundIndAttribute = false;
                        bool foundMultiple = false;
                        foreach (FieldInfo fi in fieldInfo)
                        {
                            bool hasAttr = fi.GetSingleAttribute<IndexAttribute>() != null;
                            if (hasAttr || fi.FieldType.GetSingleAttribute<IndexAttribute>() != null)
                            {
                                if (hasAttr)
                                {
                                    if (foundIndAttribute)
                                        throw new SerializerException(
                                            "More than one field has an attribute [{0}] attached in type {1}",
                                            typeof(IndexAttribute).Name, t.ToDebugStr());
                                    foundIndAttribute = true;
                                    result = fi;
                                }
                                else if (!foundIndAttribute)
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
        /// <param name="indexField">Optionally provide the index field, otherwise will attempt to find default.</param>
        public Func<TVal, TInd> GetIndexAccessor<TVal, TInd>(FieldInfo indexField = null)
        {
            return
                (Func<TVal, TInd>)
                _indAccessorCache.GetOrAdd(
                    indexField ?? GetIndexField<TVal>(),
                    fi =>
                        {
                            Type itemType = typeof (TVal);
                            if (fi.DeclaringType != itemType)
                                throw new InvalidOperationException(
                                    string.Format(
                                        "The field {0} does not belong to type {1}",
                                        fi.Name, itemType.FullName));

                            if (fi.FieldType != typeof (TInd))
                                throw new InvalidOperationException(
                                    string.Format(
                                        "The index field {0}.{1} is of type {2}, whereas {3} was expected",
                                        itemType.Name, fi.Name, fi.FieldType.ToDebugStr(),
                                        typeof (TInd).ToDebugStr()));

                            ParameterExpression vParam = Expression.Parameter(itemType, "v");
                            Expression expr = Expression.Field(vParam, fi);

                            return Expression.Lambda<Func<TVal, TInd>>(expr, vParam).Compile();
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

            public BinSerializerInfo(
                int typeSize, DynamicMethod fileStreamMethod, DynamicMethod memPtrMethod,
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