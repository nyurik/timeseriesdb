using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries;

// Generated types must access this assembly's internal types

[assembly : InternalsVisibleTo(DynamicCodeFactory.DynamicAssemblyName)]

namespace NYurik.FastBinTimeseries
{
    public class DynamicCodeFactory
    {
        public const string DynamicAssemblyName = "DynamicBinaryFileCode";
        private const string dllSuffix = ".dll";
        private static readonly string Namespace = typeof (DynamicCodeFactory).Namespace;

        private static DynamicCodeFactory _instance;
        private readonly AssemblyBuilder _assemblyBuilder;
        private readonly ModuleBuilder _moduleBuilder;
        private readonly Dictionary<Type, Type> _serializerTypes = new Dictionary<Type, Type>();
        private readonly Dictionary<Type, Delegate> _tsAccessorExpr = new Dictionary<Type, Delegate>();

        private DynamicCodeFactory(string assemblyName, bool canSaveToDisk)
        {
            var asmblyName = new AssemblyName {Name = assemblyName};

            _assemblyBuilder =
                Thread.GetDomain().DefineDynamicAssembly
                    (asmblyName,
                     canSaveToDisk ? AssemblyBuilderAccess.RunAndSave : AssemblyBuilderAccess.Run);

            _moduleBuilder = _assemblyBuilder.DefineDynamicModule(assemblyName + dllSuffix);
        }

        public static DynamicCodeFactory Instance
        {
            get
            {
                if (_instance == null)
                    lock (typeof (DynamicCodeFactory))
                        if (_instance == null)
                            _instance = new DynamicCodeFactory(DynamicAssemblyName, true);
                return _instance;
            }
        }

        public void Save()
        {
            _assemblyBuilder.Save(_moduleBuilder.ScopeName);
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

        internal IBinSerializer<T> CreateSerializer<T>()
        {
            var itemType = typeof (T);

            Type serializerType;
            if (!_serializerTypes.TryGetValue(itemType, out serializerType))
                _serializerTypes[itemType] = serializerType = CreateDynamicSerializerType(itemType);

            return (IBinSerializer<T>) Activator.CreateInstance(serializerType);
        }

        private Type CreateDynamicSerializerType(Type itemType)
        {
            // Parameter validation
            itemType.ThrowIfNotUnmanagedType();
            var attrs = itemType.GetCustomAttributes(typeof (StructLayoutAttribute), false);
            if (attrs.Length != 1 || ((StructLayoutAttribute)attrs[0]).Value == LayoutKind.Auto)
                throw new ArgumentOutOfRangeException(
                    "itemType", itemType.FullName,
                    "The type does not have a StructLayout attribute, or the attribute is set to Auto");

            itemType.ThrowIfNotAccessible(DynamicAssemblyName, "DynamicCodeFactory.DynamicAssemblyName");

            var ifType = typeof (BuiltInTypeSerializer<>).MakeGenericType(itemType);

            // Type should have a unique name, in case we later want to add more types
            var typeName = itemType.FullName;

            // Replace with '_' any symbols which should not be used in the class/assembly names
            typeName = Regex.Replace(typeName, @"[^a-zA-Z0-9_]", "_");
            var typeHlpr = _moduleBuilder.DefineType(
                Namespace + ".TypeSerializer_" + typeName, TypeAttributes.Public, ifType);

            // Create public default constructor
            var baseCtor = ifType.GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic, null,
                                                   new[] {typeof (int)}, null);
            var ctor = typeHlpr.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            var emit = ctor.GetILGenerator();
            emit
                .ldarg_0() //           L_0000: ldarg.0 
                .@sizeof(itemType) //   L_0001: sizeof NYurik.FastBinTimeseries.PrototypeStruct
                .call(baseCtor) //      L_0007: call <parent constructor (int)>
                .ret() //               L_000c: ret 
                ;

            // Create the abstract method overrides
            CreateSerializerMethodIL(itemType, ifType, typeHlpr, "ProcessFileStream", "ProcessFileStreamPtr");
            CreateSerializerMethodIL(itemType, ifType, typeHlpr, "ProcessMemoryMap", "ProcessMemoryMapPtr");

            return typeHlpr.CreateType();
        }

        private static void CreateSerializerMethodIL(Type itemType, Type baseType, TypeBuilder typeBuilder,
                                                     string methodName,
                                                     string methodToCallName)
        {
            var methodToCall = GetMethodInfo(baseType, methodToCallName);
            var baseMethodInfo = GetMethodInfo(baseType, methodName);

            var method = typeBuilder.DefineMethod(baseMethodInfo);
            //            var method = typeBuilder.DefineMethodOverride(baseMethodInfo);
            //            var methodParams = baseMethodInfo.GetParameters();
            //
            //            var method = typeBuilder.DefineMethod(
            //                baseMethodInfo.Name,
            //                MethodAttributes.Public | MethodAttributes.Virtual,
            //                baseMethodInfo.ReturnType,
            //                Array.ConvertAll(methodParams, i => i.ParameterType));

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
    }
}