#if DEBUG
//#define DEBUG_SERIALIZER
#endif

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public abstract class BaseSerializer : Initializable
    {
        private string _name;

        /// <param name="valueType">Type of value to store</param>
        /// <param name="name">Name of the value (for debugging)</param>
        protected BaseSerializer([NotNull] Type valueType, string name = null)
        {
            if (valueType == null)
                throw new ArgumentNullException("valueType");

            ValueType = valueType;
            _name = name;
        }

        public Type ValueType { get; private set; }

        public string Name
        {
            get { return _name; }
            set
            {
                ThrowOnInitialized();
                _name = value;
            }
        }

        protected MethodCallExpression WriteSignedValue(Expression codec, Expression value)
        {
            return Expression.Call(codec, "WriteSignedValue", null, value);
        }

        protected MethodCallExpression ReadSignedValue(Expression codec)
        {
            return Expression.Call(codec, "ReadSignedValue", null);
        }

        protected MethodCallExpression ThrowOverflow(Expression codec, Expression value)
        {
            return Expression.Call(codec, "ThrowOverflow", new[] {value.Type}, value);
        }

        public static Expression DebugLong(Expression codec, Expression value, string name = null)
        {
#if DEBUG_SERIALIZER
            var prm = value as ParameterExpression;
            return Expression.Call(
                codec, "DebugLong", null, value,
                Expression.Constant(name ?? (prm != null ? prm.Name : null), typeof (string)));
#else
            return Expression.Empty();
#endif
        }

        public static Expression DebugFloat(Expression codec, Expression value, string name = null)
        {
#if DEBUG_SERIALIZER
            var prm = value as ParameterExpression;
            return Expression.Call(
                codec, "DebugFloat", null, value,
                Expression.Constant(name ?? (prm != null ? prm.Name : null), typeof (string)));
#else
            return Expression.Empty();
#endif
        }

        public virtual void Validate()
        {
            ThrowOnInitialized();
            IsInitialized = true;
        }

        public Expression GetSerializer(
            Expression valueExp, Expression codec, List<ParameterExpression> stateVariables, List<Expression> initBlock)
        {
            EnsureValidation();

            if (ValueType != valueExp.Type)
                throw new SerializerException(
                    "Serializer received an unexpected value of type {0}, instead of {1}",
                    valueExp.Type.FullName, ValueType.FullName);

            return GetSerializerExp(valueExp, codec, stateVariables, initBlock);
        }

        public void GetDeSerializer(Expression codec, List<ParameterExpression> stateVariables,
                                    out Expression readInitValue, out Expression readNextValue)
        {
            EnsureValidation();
            GetDeSerializerExp(codec, stateVariables, out readInitValue, out readNextValue);

            if (ValueType != readInitValue.Type)
                throw new SerializerException(
                    "DeSerializer 'init' has unexpected type {0}, instead of {1}",
                    readInitValue.Type.FullName, ValueType.FullName);

            if (ValueType != readNextValue.Type)
                throw new SerializerException(
                    "DeSerializer 'next' has unexpected type {0}, instead of {1}",
                    readInitValue.Type.FullName, ValueType.FullName);
        }

        protected abstract Expression GetSerializerExp(
            Expression valueExp, Expression codec, List<ParameterExpression> stateVariables, List<Expression> initBlock);

        protected abstract void GetDeSerializerExp(Expression codec, List<ParameterExpression> stateVariables,
                                                   out Expression readInitValue, out Expression readNextValue);

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