using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries
{
    public class DynamicCodeFactory
    {
        internal const BindingFlags AllInstanceMembers =
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        private static DynamicCodeFactory s_instance;

        private readonly SynchronizedDictionary<Type, BinSerializerInfo> _serializers =
            new SynchronizedDictionary<Type, BinSerializerInfo>();

        private readonly SynchronizedDictionary<FieldInfo, Delegate> _tsAccessorExpr =
            new SynchronizedDictionary<FieldInfo, Delegate>();

        private DynamicCodeFactory()
        {
        }

        public static DynamicCodeFactory Instance
        {
            get
            {
                // todo: switch to LazyInit<> in .Net 4.0
                if (s_instance == null)
                    lock (typeof (DynamicCodeFactory))
                        if (s_instance == null)
                            s_instance = new DynamicCodeFactory();
                return s_instance;
            }
        }

        private static MethodInfo GetMethodInfo(Type baseType, string methodName)
        {
            MethodInfo methodToCall = baseType.GetMethod(methodName, AllInstanceMembers);
            if (methodToCall == null)
                throw new ArgumentOutOfRangeException(
                    "methodName", methodName, "Method not found in the base type " + baseType.FullName);
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
        public static void ThrowIfNotUnmanagedType(Type type)
        {
            ThrowIfNotUnmanagedType(type, new Stack<Type>(4));
        }

        private static void ThrowIfNotUnmanagedType(Type type, Stack<Type> typesStack)
        {
            if ((!type.IsValueType && !type.IsPointer) || type.IsGenericType || type.IsGenericParameter || type.IsArray)
                throw new ArgumentException(String.Format("Type {0} is not an unmanaged type", type.FullName));

            if (!type.IsPrimitive && !type.IsEnum && !type.IsPointer)
                for (Type p = type.DeclaringType; p != null; p = p.DeclaringType)
                    if (p.IsGenericTypeDefinition)
                        throw new ArgumentException(
                            String.Format("Type {0} contains a generic type definition declaring type {1}",
                                          type.FullName, p.FullName));
            if (type.StructLayoutAttribute == null
                || (type.StructLayoutAttribute.Value != LayoutKind.Explicit
                    && type.StructLayoutAttribute.Value != LayoutKind.Sequential)
                || type.StructLayoutAttribute.Pack == 0
                )
            {
                throw new ArgumentOutOfRangeException(
                    "type", type.FullName,
                    "The type does not have a StructLayout attribute, or is set to Auto, or the Pack is 0");
            }

            try
            {
                typesStack.Push(type);

                FieldInfo[] fields = type.GetFields(AllInstanceMembers);

                foreach (FieldInfo f in fields)
                    if (!typesStack.Contains(f.FieldType))
                        ThrowIfNotUnmanagedType(f.FieldType, typesStack);
            }
            catch (Exception ex)
            {
                throw new ArgumentException(
                    String.Format("Error in subtype of type {0}. See InnerException.", type.FullName), ex);
            }
            finally
            {
                typesStack.Pop();
            }
        }

        #region Serializer

        internal BinSerializerInfo CreateSerializer<T>()
        {
            return _serializers.GetCreateValue(typeof (T), CreateDynamicSerializerType);
        }

        private static BinSerializerInfo CreateDynamicSerializerType(Type itemType)
        {
            // Parameter validation
            ThrowIfNotUnmanagedType(itemType);

            Type ifType = typeof (DefaultTypeSerializer<>).MakeGenericType(itemType);

            // Create the abstract method overrides
            return new BinSerializerInfo(
                (int) CreateSizeOfMethodIL(itemType, ifType.Module).Invoke(null, null),
                CreateSerializerMethodIL(
                    itemType, ifType, "DynProcessFileStream", "ProcessFileStreamPtr", typeof (FileStream)),
                CreateSerializerMethodIL(
                    itemType, ifType, "DynProcessMemoryMap", "ProcessMemoryMapPtr", typeof (IntPtr)),
                CreateMemComparerMethodIL(itemType, ifType)
                );
        }

        private static DynamicMethod CreateSizeOfMethodIL(Type itemType, Module module)
        {
            var method = new DynamicMethod("SizeOf", typeof (int), null, module, true);
            ILGenerator emit = method.GetILGenerator();

            emit
                .@sizeof(itemType)
                .ret();

            return method;
        }

        private static DynamicMethod CreateSerializerMethodIL(Type itemType, Type baseType, string methodName,
                                                              string methodToCallName, Type firstParamType)
        {
            MethodInfo methodToCall = GetMethodInfo(baseType, methodToCallName);

            var method = new DynamicMethod(
                methodName,
                typeof (void),
                new[] {baseType, firstParamType, itemType.MakeArrayType(), typeof (int), typeof (int), typeof (bool)},
                baseType.Module,
                true);

            ILGenerator emit = method.GetILGenerator();

            //                .locals init (
            //                    [0] void& pinned bufPtr)
            LocalBuilder bufPtr = emit.DeclareLocal(typeof (void).MakeByRefType(), true);

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
                .ldc_i4_0() //        L_0016: ldc.i4.0 
                .conv_u() //          L_0017: conv.u 
                .stloc(bufPtr) //     L_0018: stloc.0 
                .ret() //             L_0019: ret 
                ;

            return method;
        }

        private static DynamicMethod CreateMemComparerMethodIL(Type itemType, Type baseType)
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
            Label L_0022 = emit.DefineLabel();
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
                .leave_s(L_0022) //     L_0020: leave.s L_0022
                .MarkLabelExt(L_0022)
                .ldloc_2() //           L_0022: ldloc.2 
                .ret() //               L_0023: ret 
                ;

            return method;
        }

        #endregion

        #region Timestamp Accessor

        /// <summary>
        /// Create a delegate that extracts a timestamp from the struct of type T.
        /// A datetime field must be first in the struct.
        /// </summary>
        internal Func<T, UtcDateTime> CreateTSAccessor<T>(FieldInfo fieldInfo)
        {
            return (Func<T, UtcDateTime>) _tsAccessorExpr.GetCreateValue(fieldInfo, CreateAccessor<T>);
        }

        private static Delegate CreateAccessor<T>(FieldInfo fieldInfo)
        {
            Type itemType = typeof (T);
            if (fieldInfo.DeclaringType != itemType)
                throw new InvalidOperationException(
                    String.Format("The field {0} does not belong to type {1}",
                                  fieldInfo.Name, itemType.FullName));
            if (fieldInfo.FieldType != typeof (UtcDateTime))
                throw new InvalidOperationException(
                    String.Format("The field {0} in type {1} is not a UtcDateTime",
                                  fieldInfo.Name, itemType.FullName));

            ParameterExpression vParam = Expression.Parameter(itemType, "v");
            Expression<Func<T, UtcDateTime>> exprLambda = Expression.Lambda<Func<T, UtcDateTime>>(
                Expression.Field(vParam, fieldInfo), vParam);
            return exprLambda.Compile();
        }

        #endregion

        #region Nested type: BinSerializerInfo

        internal class BinSerializerInfo
        {
            public readonly DynamicMethod FileStreamMethod;
            public readonly DynamicMethod MemCompareMethod;
            public readonly DynamicMethod MemMapMethod;
            public readonly int TypeSize;

            public BinSerializerInfo(int typeSize, DynamicMethod fileStreamMethod, DynamicMethod memMapMethod,
                                     DynamicMethod memCompareMethod)
            {
                TypeSize = typeSize;
                FileStreamMethod = fileStreamMethod;
                MemMapMethod = memMapMethod;
                MemCompareMethod = memCompareMethod;
            }
        }

        #endregion
    }
}