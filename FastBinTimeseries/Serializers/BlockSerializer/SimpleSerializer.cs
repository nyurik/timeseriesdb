using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    internal class SimpleSerializer : BaseSerializer
    {
        private TypeCode _typeCode;

        public SimpleSerializer([NotNull] Type valueType, string name)
            : base(valueType, name)
        {
        }

        protected override Expression GetSerializerExp(Expression valueExp, Expression codec,
                                                       List<ParameterExpression> stateVariables,
                                                       List<Expression> initBlock)
        {
            ThrowOnNotInitialized();
            MethodCallExpression writeMethod;
            switch (_typeCode)
            {
                case TypeCode.SByte:
                    writeMethod = Expression.Call(
                        codec, "WriteByte", null,
                        Expression.Convert(valueExp, typeof (byte)));
                    break;
                case TypeCode.Byte:
                    writeMethod = Expression.Call(codec, "WriteByte", null, valueExp);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            initBlock.Add(writeMethod);
            return writeMethod;
        }

        protected override void GetDeSerializerExp(Expression codec, List<ParameterExpression> stateVariables,
                                                   out Expression readInitValue, out Expression readNextValue)
        {
            ThrowOnNotInitialized();

            Expression readMethod;
            switch (_typeCode)
            {
                case TypeCode.SByte:
                    readMethod = Expression.Convert(Expression.Call(codec, "ReadByte", null), typeof (sbyte));
                    break;
                case TypeCode.Byte:
                    readMethod = Expression.Call(codec, "ReadByte", null);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            readInitValue = readMethod;
            readNextValue = readMethod;
        }

        public override void Validate()
        {
            ThrowOnInitialized();
            _typeCode = Type.GetTypeCode(ValueType);
            switch (_typeCode)
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                    break;
                default:
                    throw new SerializerException(
                        "Value {0} has an unsupported type {0}", Name, ValueType.AssemblyQualifiedName);
            }

            base.Validate();
        }
    }
}