using System;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    internal class SimpleField : BaseField
    {
        private TypeCode _typeCode;

        public SimpleField([NotNull] IStateStore serializer, [NotNull] Type valueType, string stateName)
            : base(serializer, valueType, stateName)
        {
        }

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
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

            return new Tuple<Expression, Expression>(writeMethod, writeMethod);
        }

        protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
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

            return new Tuple<Expression, Expression>(readMethod, readMethod);
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
                        "Value {0} has an unsupported type {0}", StateName, ValueType.AssemblyQualifiedName);
            }

            base.Validate();
        }
    }
}