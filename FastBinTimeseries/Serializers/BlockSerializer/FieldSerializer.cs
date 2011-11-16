using System;
using System.Linq.Expressions;
using System.Reflection;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public abstract class TypeSerializer
    {
        public static readonly ParameterExpression FldSerializerExp = Expression.Parameter(typeof (StreamCodec));
        public readonly FieldInfo Field;
        public readonly MemberExpression FieldExp;
        public readonly ConstantExpression IndexExp;

        protected TypeSerializer(byte index, FieldInfo field)
        {
            IndexExp = Expression.Constant(index);
            Field = field;
            FieldExp = Expression.Field(Expression.Parameter(field.DeclaringType), Field);
        }

        public Expression GetSerializerExpr2()
        {
            return GetSerializerExpr();
        }

        protected abstract Expression GetSerializerExpr();

        public Expression GetDeSerializerExpr2()
        {
            return Expression.Assign(FieldExp, GetDeSerializerExpr());
        }

        protected abstract Expression GetDeSerializerExpr();
    }

    internal class IntSerializer : TypeSerializer
    {
        public IntSerializer(byte index, FieldInfo field)
            : base(index, field)
        {
        }

        protected override Expression GetSerializerExpr()
        {
            return Expression.Call(
                FldSerializerExp,
                FldSerializerExp.Type.GetMethod("Write" + Field.FieldType.Name),
                IndexExp,
                FieldExp);
        }

        protected override Expression GetDeSerializerExpr()
        {
            return
                Expression.Call(
                    FldSerializerExp, FldSerializerExp.Type.GetMethod("Read" + Field.FieldType.Name),
                    IndexExp);
        }
    }

    internal class FloatSerializer : TypeSerializer
    {
        public readonly ConstantExpression PrescisionMultExp;

        public FloatSerializer(byte index, FieldInfo field, int prescision)
            : base(index, field)
        {
            PrescisionMultExp = Expression.Constant(Math.Pow(10, prescision));
        }

        protected override Expression GetSerializerExpr()
        {
            return Expression.Call(
                FldSerializerExp, FldSerializerExp.Type.GetMethod("WriteInt64"),
                Expression.Convert(
                    Expression.Multiply(
                        PrescisionMultExp,
                        FieldExp),
                    typeof (long)), IndexExp);
        }

        protected override Expression GetDeSerializerExpr()
        {
            return Expression.Divide(
                Expression.Call(
                    FldSerializerExp, FldSerializerExp.Type.GetMethod("ReadInt64"), IndexExp),
                PrescisionMultExp);
        }
    }

    [AttributeUsage(AttributeTargets.Field, Inherited = false, AllowMultiple = false)]
    public sealed class FieldAttribute : Attribute
    {
        public FieldAttribute()
        {
        }

        public FieldAttribute(Type serializer)
        {
            Serializer = serializer;
        }

        public Type Serializer { get; private set; }
    }

    public class FieldSerializer<T>
    {
        public int Serialize(T[] data, int i, byte[] buffer)
        {
            throw new NotImplementedException();
        }

        public int Deserialize(T[] data, int i, byte[] buffer)
        {
            throw new NotImplementedException();
        }
    }
}