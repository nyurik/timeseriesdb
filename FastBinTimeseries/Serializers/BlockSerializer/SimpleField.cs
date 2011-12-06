using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class SimpleField : BaseField
    {
        protected SimpleField()
        {
        }

        public SimpleField([NotNull] IStateStore serializer, [NotNull] Type valueType, string stateName)
            : base(Version10, serializer, valueType, stateName)
        {
        }

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
        {
            MethodCallExpression writeMethod;
            switch (ValueTypeCode)
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
            Expression readMethod;
            switch (ValueTypeCode)
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

        public override int GetMaxByteSize()
        {
            return 1;
        }

        protected override void InitExistingField(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            base.InitExistingField(reader, typeMap);
            if (Version != Version10)
                throw new IncompatibleVersionException(GetType(), Version);
        }

        protected override void MakeReadonly()
        {
            ThrowOnInitialized();
            switch (ValueTypeCode)
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                    break;
                default:
                    throw new SerializerException(
                        "Value {0} has an unsupported type {0}", StateName, ValueType.AssemblyQualifiedName);
            }

            base.MakeReadonly();
        }
    }
}