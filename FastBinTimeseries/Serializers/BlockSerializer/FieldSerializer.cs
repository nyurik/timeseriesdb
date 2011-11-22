using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using NYurik.EmitExtensions;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public abstract class TypeSerializer
    {
        public static readonly ParameterExpression FldSerializerExp = Expression.Parameter(typeof (StreamCodec));
        public readonly FieldInfo Field;
        public readonly MemberExpression FieldExp;
        public readonly ConstantExpression IndexExp;
        public static readonly MethodInfo WriteSignedValueMethod;

        static TypeSerializer()
        {
            WriteSignedValueMethod = FldSerializerExp.Type.GetMethod(
                "WriteSignedValue", TypeExtensions.AllInstanceMembers);
        }

        protected TypeSerializer(byte index, FieldInfo field)
        {
            IndexExp = Expression.Constant(index);
            Field = field;
            FieldExp = Expression.Field(Expression.Parameter(field.DeclaringType), Field);
        }

        protected internal abstract Tuple<Expression, Expression> GetSerializerExpr();

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

        protected internal override Tuple<Expression, Expression> GetSerializerExpr()
        {
            throw new NotImplementedException();
//            return Expression.Call(
//                FldSerializerExp,
//                FldSerializerExp.Type.GetMethod("Write" + Field.FieldType.Name),
//                IndexExp,
//                FieldExp);
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
            PrescisionMultExp = Expression.Constant((float) prescision);
        }

        protected internal override Tuple<Expression,Expression> GetSerializerExpr()
        {
            var getValExpr = Expression.Convert(Expression.Multiply(PrescisionMultExp, FieldExp), typeof (long));
            var stateVarExpr = Expression.Variable(typeof (long), "float");
            var stateVar2Expr = Expression.Variable(typeof (long), "float2");

            Expression init =
                Expression.Block(
                    Expression.Assign(stateVarExpr, getValExpr),
                    Expression.Call(FldSerializerExp, WriteSignedValueMethod, stateVarExpr));


            Expression delta =
                Expression.Block(
                    Expression.Assign(stateVar2Expr, getValExpr),
                    Expression.Call(FldSerializerExp, WriteSignedValueMethod,
                                    Expression.Subtract(stateVar2Expr, stateVarExpr)),
                    Expression.Assign(stateVarExpr, stateVar2Expr)
                    );
            

            return Tuple.Create(delta, delta);
        }

        protected override Expression GetDeSerializerExpr()
        {
            return Expression.Divide(
                Expression.Call(FldSerializerExp, FldSerializerExp.Type.GetMethod("ReadInt64"), IndexExp),
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
        public IEnumerable<ArraySegment<DeltaBlock>> Serialize(DeltaBlock lastBlock, IEnumerable<ArraySegment<T>> newData)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<ArraySegment<T>> Deserialize(IEnumerable<ArraySegment<DeltaBlock>> data)
        {
            throw new NotImplementedException();
        }
    }
}