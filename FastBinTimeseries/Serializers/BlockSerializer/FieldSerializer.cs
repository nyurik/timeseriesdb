using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using NYurik.EmitExtensions;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class FieldSerializer<T>
    {
        public IEnumerable<ArraySegment<DeltaBlock>> Serialize(DeltaBlock lastBlock,
                                                               IEnumerable<ArraySegment<T>> newData)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<ArraySegment<T>> Deserialize(IEnumerable<ArraySegment<DeltaBlock>> data)
        {
            throw new NotImplementedException();
        }
    }

    public abstract class TypeSerializer
    {
        public static readonly MethodInfo WriteSignedValueMethod;
        public readonly FieldInfo Field;

        static TypeSerializer()
        {
            WriteSignedValueMethod = typeof (StreamCodec).GetMethod(
                "WriteSignedValue", TypeExtensions.AllInstanceMembers);
        }

        protected TypeSerializer(FieldInfo field)
        {
            Field = field;
        }

        protected internal abstract Expression GetSerializerExp(
            Expression valueT, ParameterExpression codec, List<ParameterExpression> stateVariables, List<Expression> initBlock);

        protected abstract Expression GetDeSerializerExp();
    }

    internal class IntSerializer : TypeSerializer
    {
        public IntSerializer(FieldInfo field)
            : base(field)
        {
        }

        protected internal override Expression GetSerializerExp(Expression valueT, ParameterExpression codec, List<ParameterExpression> stateVariables, List<Expression> initBlock)
        {
            throw new NotImplementedException();
//            return Expression.Call(
//                FldSerializerExp,
//                FldSerializerExp.Type.GetMethod("Write" + Field.FieldType.Name),
//                IndexExp,
//                FieldExp);
        }

        protected override Expression GetDeSerializerExp()
        {
            throw new NotImplementedException();
//            return
//                Expression.Call(
//                    FldSerializerExp, FldSerializerExp.Type.GetMethod("Read" + Field.FieldType.Name),
//                    IndexExp);
        }
    }

    internal class FloatSerializer : TypeSerializer
    {
        public readonly int Multiplier;

        public FloatSerializer(FieldInfo field, int multiplier)
            : base(field)
        {
           Multiplier = multiplier;
        }

        protected internal override Expression GetSerializerExp(Expression valueT, ParameterExpression codec, List<ParameterExpression> stateVariables, List<Expression> initBlock)
        {
            //
            // long stateVar;
            //
            ParameterExpression stateVarExp = Expression.Variable(typeof (long), "state_" + Field.Name);
            stateVariables.Add(stateVarExp);

            //
            // valueGetter(): (long)(T.Field * Multiplier)
            //
            UnaryExpression getValExp = Expression.Convert(
                Expression.Multiply(
                    Expression.Field(valueT, Field), Expression.Constant((float) Multiplier)),
                typeof (long));

            //
            // stateVar = valueGetter();
            // codec.WriteSignedValue(stateVar);
            //
            initBlock.Add(Expression.Assign(stateVarExp, getValExp));
            // ReSharper disable PossiblyMistakenUseOfParamsMethod
            initBlock.Add(Expression.Call(codec, WriteSignedValueMethod, stateVarExp));
            // ReSharper restore PossiblyMistakenUseOfParamsMethod

            //
            // stateVar2 = valueGetter();
            // delta = stateVar2 - stateVar
            // stateVar = stateVar2;
            // return codec.WriteSignedValue(delta);
            //
            ParameterExpression stateVar2Exp = Expression.Variable(typeof (long), "state2_" + Field.Name);
            ParameterExpression deltaExp = Expression.Variable(typeof(long), "delta_" + Field.Name);
            return
                Expression.Block(
                    typeof (bool),
                    new[] {stateVar2Exp, deltaExp},
                    Expression.Assign(stateVar2Exp, getValExp),
                    Expression.Assign(deltaExp, Expression.Subtract(stateVar2Exp, stateVarExp)),
                    Expression.Assign(stateVarExp, stateVar2Exp),
                    // ReSharper disable PossiblyMistakenUseOfParamsMethod
                    Expression.Call(codec, WriteSignedValueMethod, deltaExp)
                    // ReSharper restore PossiblyMistakenUseOfParamsMethod
                    );
        }

        protected override Expression GetDeSerializerExp()
        {
            throw new NotImplementedException();
//            return Expression.Divide(
//                Expression.Call(FldSerializerExp, FldSerializerExp.Type.GetMethod("ReadInt64"), IndexExp),
//                MultiplierExp);
        }
    }
}