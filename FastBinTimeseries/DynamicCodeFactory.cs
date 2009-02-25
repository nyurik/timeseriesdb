using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using NYurik.EmitExtensions;

namespace NYurik.FastBinTimeseries
{
    public class DynamicCodeFactory
    {
        private static DynamicCodeFactory _instance;
        private readonly Dictionary<Type, BinSerializerInfo> _serializers = new Dictionary<Type, BinSerializerInfo>();
        private readonly Dictionary<Type, Delegate> _tsAccessorExpr = new Dictionary<Type, Delegate>();

        private DynamicCodeFactory()
        {
        }

        public static DynamicCodeFactory Instance
        {
            get
            {
                if (_instance == null)
                    lock (typeof (DynamicCodeFactory))
                        if (_instance == null)
                            _instance = new DynamicCodeFactory();
                return _instance;
            }
        }

        public void Save()
        {
            //_assemblyBuilder.Save(_moduleBuilder.ScopeName);
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

            var ifType = typeof (BuiltInTypeSerializer<>).MakeGenericType(itemType);

            // Create the abstract method overrides
            return new BinSerializerInfo(
                Marshal.SizeOf(itemType),
                CreateSerializerMethodIL(
                    itemType, ifType, "DynProcessFileStream", "ProcessFileStreamPtr", typeof (FileStream)),
                CreateSerializerMethodIL(
                    itemType, ifType, "DynProcessMemoryMap", "ProcessMemoryMapPtr", typeof (IntPtr))
                );
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
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        internal Func<T, DateTime> CreateTSAccessor<T>()
        {
            var itemType = typeof (T);
            var fieldInfo = itemType.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            if (fieldInfo.Length < 1)
                throw new InvalidOperationException("No fields found in type " + itemType.FullName);
            if (fieldInfo[0].FieldType != typeof (DateTime))
                throw new InvalidOperationException(string.Format("The field[0] {0} in type {1} is not a DateTime",
                                                                  fieldInfo[0].Name, itemType.FullName));

            Delegate tsAccessorType;
            if (!_tsAccessorExpr.TryGetValue(itemType, out tsAccessorType))
            {
                var vParam = Expression.Parameter(itemType, "v");
                var exprLambda = Expression.Lambda<Func<T, DateTime>>(
                    Expression.Field(vParam, fieldInfo[0]), vParam);
                _tsAccessorExpr[itemType] = tsAccessorType = exprLambda.Compile();
            }

            return (Func<T, DateTime>) tsAccessorType;
        }

        #endregion

        #region Nested type: BinSerializerInfo

        internal class BinSerializerInfo
        {
            public readonly DynamicMethod FileStreamMethod;
            public readonly int ItemSize;
            public readonly DynamicMethod MemMapMethod;

            public BinSerializerInfo(int itemSize, DynamicMethod fileStreamMethod, DynamicMethod memMapMethod)
            {
                ItemSize = itemSize;
                FileStreamMethod = fileStreamMethod;
                MemMapMethod = memMapMethod;
            }
        }

        #endregion
    }
}