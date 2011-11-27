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

        public static Expression DebugLong(Expression codec, Expression value)
        {
#if DEBUG
            return Expression.Call(codec, "DebugLong", null, value);
#else
            return Expression.Empty();
#endif
        }

        public static Expression DebugFloat(Expression codec, Expression value)
        {
#if DEBUG
            return Expression.Call(codec, "DebugFloat", null, value);
#else
            return Expression.Empty();
#endif
        }

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