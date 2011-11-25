using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using NYurik.EmitExtensions;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public abstract class BaseSerializer : Initializable
    {
        public static readonly MethodInfo WriteSignedValueMethod;
        public static readonly MethodInfo ReadSignedValueMethod;
        public static readonly MethodInfo DebugLongMethod, DebugFloatMethod;

        static BaseSerializer()
        {
            WriteSignedValueMethod = typeof (StreamCodec).GetMethod(
                "WriteSignedValue", TypeExtensions.AllInstanceMembers);
            ReadSignedValueMethod = typeof (StreamCodec).GetMethod(
                "ReadSignedValue", TypeExtensions.AllInstanceMembers);
            DebugLongMethod = typeof (StreamCodec).GetMethod("DebugLong", TypeExtensions.AllInstanceMembers);
            DebugFloatMethod = typeof (StreamCodec).GetMethod("DebugFloat", TypeExtensions.AllInstanceMembers);
        }

        protected BaseSerializer([NotNull] Type valueType)
        {
            if (valueType == null)
                throw new ArgumentNullException("valueType");

            ValueType = valueType;
        }

        public Type ValueType { get; private set; }

        public virtual void Validate()
        {
            IsInitialized = true;
        }

        public Expression GetSerializer(
            Expression valueExp, Expression codec, List<ParameterExpression> stateVariables, List<Expression> initBlock)
        {
            EnsureValidation();
            return GetSerializerExp(valueExp, codec, stateVariables, initBlock);
        }

        public void GetDeSerializer(
            Expression valueExp, Expression codec, List<ParameterExpression> stateVariables,
            List<Expression> initBlock, List<Expression> deltaBlock)
        {
            EnsureValidation();
            GetDeSerializerExp(valueExp, codec, stateVariables, initBlock, deltaBlock);
        }

        protected abstract Expression GetSerializerExp(
            Expression valueExp, Expression codec, List<ParameterExpression> stateVariables, List<Expression> initBlock);

        protected abstract void GetDeSerializerExp(
            Expression valueExp, Expression codec, List<ParameterExpression> stateVariables,
            List<Expression> initBlock, List<Expression> deltaBlock);

        private void EnsureValidation()
        {
            if (!IsInitialized)
                Validate();
            if (!IsInitialized)
                throw new SerializerException(
                    "Derived serializer {0} must call base when validating", GetType().AssemblyQualifiedName);
        }
    }
}