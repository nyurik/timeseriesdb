using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using NYurik.EmitExtensions;

namespace NYurik.FastBinTimeseries
{
    public class DynamicCodeFactory
    {
        internal const BindingFlags FieldBindingFlags =
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        private static DynamicCodeFactory _instance;
        private readonly Dictionary<Type, BinSerializerInfo> _serializers = new Dictionary<Type, BinSerializerInfo>();
        private readonly Dictionary<FieldInfo, Delegate> _tsAccessorExpr = new Dictionary<FieldInfo, Delegate>();

        private DynamicCodeFactory()
        {
        }

        public static DynamicCodeFactory Instance
        {
            get
            {
                // todo: switch to LazyInit<> in .Net 4.0
                if (_instance == null)
                    lock (typeof (DynamicCodeFactory))
                        if (_instance == null)
                            _instance = new DynamicCodeFactory();
                return _instance;
            }
        }

        private static MethodInfo GetMethodInfo(Type baseType, string methodName)
        {
            var methodToCall = baseType.GetMethod(methodName,
                                                  BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            if (methodToCall == null)
                throw new ArgumentOutOfRangeException(
                    "methodName", methodName, "Method not found in the base type " + baseType.FullName);
            return methodToCall;
        }

        #region Serializer

        internal BinSerializerInfo CreateSerializer<T>()
        {
            var itemType = typeof (T);

            BinSerializerInfo info;
            if (!_serializers.TryGetValue(itemType, out info))
                _serializers[itemType] = info = CreateDynamicSerializerType(itemType);

            return info;
        }

        private static BinSerializerInfo CreateDynamicSerializerType(Type itemType)
        {
            // Parameter validation
            itemType.ThrowIfNotUnmanagedType();
            if ((itemType.Attributes & TypeAttributes.ExplicitLayout) != TypeAttributes.ExplicitLayout &&
                (itemType.Attributes & TypeAttributes.SequentialLayout) != TypeAttributes.SequentialLayout)
                throw new ArgumentOutOfRangeException(
                    "itemType", itemType.FullName,
                    "The type does not have a StructLayout attribute, or the attribute is set to Auto");

            var ifType = typeof (DefaultTypeSerializer<>).MakeGenericType(itemType);

            // Create the abstract method overrides
            return new BinSerializerInfo(
                (int) CreateSizeOfMethodIL(itemType, ifType.Module).Invoke(null, null),
                CreateSerializerMethodIL(
                    itemType, ifType, "DynProcessFileStream", "ProcessFileStreamPtr", typeof (FileStream)),
                CreateSerializerMethodIL(
                    itemType, ifType, "DynProcessMemoryMap", "ProcessMemoryMapPtr", typeof (IntPtr))
                );
        }

        private static DynamicMethod CreateSizeOfMethodIL(Type itemType, Module module)
        {
            var method = new DynamicMethod("SizeOf", typeof (int), null, module, true);
            var emit = method.GetILGenerator();

            emit
                .@sizeof(itemType)
                .ret();

            return method;
        }

        private static DynamicMethod CreateSerializerMethodIL(Type itemType, Type baseType, string methodName,
                                                              string methodToCallName, Type firstParamType)
        {
            var methodToCall = GetMethodInfo(baseType, methodToCallName);

            var method = new DynamicMethod(
                methodName,
                typeof (void),
                new[] {baseType, firstParamType, itemType.MakeArrayType(), typeof (int), typeof (int), typeof (bool)},
                baseType.Module,
                true);

            var emit = method.GetILGenerator();

            //                .locals init (
            //                    [0] void& pinned bufPtr)
            var bufPtr = emit.DeclareLocal(typeof (void).MakeByRefType(), true);

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
                .ldelema(itemType) // L_0002: ldelema NYurik.FastBinTimeseries.PrototypeStruct
                .stloc(bufPtr) //     L_0007: stloc.0 
                .ldarg_0() //         L_0008: ldarg.0 
                .ldarg_1() //         L_0009: ldarg.1 
                .ldloc(bufPtr) //     L_000a: ldloc.0 
                .conv_i() //          L_000b: conv.i 
                .ldarg_3() //         L_000c: ldarg.3 
                .ldarg_s(4) //        L_000d: ldarg.s count
                .ldarg_s(5) //        L_000f: ldarg.s isWriting
                .call(methodToCall) //L_0011: call instance ... (parent method)
                .ldc_i4_0() //        L_0016: ldc.i4.0 
                .conv_u() //          L_0017: conv.u 
                .stloc(bufPtr) //     L_0018: stloc.0 
                .ret() //             L_0019: ret 
                ;

            return method;
        }

        #endregion

        #region Timestamp Accessor

        /// <summary>
        /// Create a delegate that extracts a timestamp from the struct of type T.
        /// A datetime field must be first in the struct.
        /// </summary>
        internal Func<T, DateTime> CreateTSAccessor<T>(FieldInfo fieldInfo)
        {
            var itemType = typeof (T);
            if (fieldInfo.DeclaringType != itemType)
                throw new InvalidOperationException(String.Format("The field {0} does not belong to type {1}",
                                                                  fieldInfo.Name, itemType.FullName));
            if (fieldInfo.FieldType != typeof (DateTime))
                throw new InvalidOperationException(String.Format("The field {0} in type {1} is not a DateTime",
                                                                  fieldInfo.Name, itemType.FullName));

            Delegate tsAccessorType;
            if (!_tsAccessorExpr.TryGetValue(fieldInfo, out tsAccessorType))
            {
                var vParam = Expression.Parameter(itemType, "v");
                var exprLambda = Expression.Lambda<Func<T, DateTime>>(
                    Expression.Field(vParam, fieldInfo), vParam);
                _tsAccessorExpr[fieldInfo] = tsAccessorType = exprLambda.Compile();
            }

            return (Func<T, DateTime>) tsAccessorType;
        }

        #endregion

        #region Nested type: BinSerializerInfo

        internal class BinSerializerInfo
        {
            public readonly DynamicMethod FileStreamMethod;
            public readonly DynamicMethod MemMapMethod;
            public readonly int TypeSize;

            public BinSerializerInfo(int typeSize, DynamicMethod fileStreamMethod, DynamicMethod memMapMethod)
            {
                TypeSize = typeSize;
                FileStreamMethod = fileStreamMethod;
                MemMapMethod = memMapMethod;
            }
        }

        #endregion
    }
}