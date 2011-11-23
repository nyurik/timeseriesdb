using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    internal static class DynamicSerializer<T>
    {
        ///
        /// Expected code:
        /// 
        /// bool Func(StreamCodec codec, IEnumerator&ltT> enumerator)
        /// {
        ///     bool moveNext;
        ///     T current = enumerator.Current;
        ///     
        ///     var state1, state2, ...;
        /// 
        ///     codec.Write(state1);
        ///     codec.Write(state2);
        ///     ...
        /// 
        ///     while (true) {
        /// 
        ///         moveNext = enumerator.MoveNext();
        ///         if (!moveNext)
        ///             break;
        /// 
        ///         int codecPos = codec.BufferPos;
        ///         if (! (codec.Write(delta1) && codec.Write(delta2) && ...) ) {
        ///             codec.BufferPos = codecPos;
        ///             break;
        ///         } 
        ///     }
        /// 
        ///     return moveNext;
        /// }
        public static Func<StreamCodec, IEnumerator<T>, bool> GenerateSerializer(
            [NotNull] IEnumerable<TypeSerializer> fieldSerializers)
        {
            if (fieldSerializers == null) throw new ArgumentNullException("fieldSerializers");

            // param: codec
            ParameterExpression codecExp = Expression.Parameter(typeof (StreamCodec), "codec");

            // param: IEnumerator<T> data
            ParameterExpression enumeratorExp = Expression.Parameter(typeof (IEnumerator<T>), "enumerator");

            // bool moveNext;
            ParameterExpression moveNextExp = Expression.Variable(typeof (bool), "moveNext");

            // T current;
            ParameterExpression currentExp = Expression.Variable(typeof (T), "current");

            // current = enumerator.Current;
            BinaryExpression setCurrentValue = Expression.Assign(
                currentExp, Expression.PropertyOrField(enumeratorExp, "Current"));

            LabelTarget breakOuter = Expression.Label();

            var stateVariables = new List<ParameterExpression> {currentExp, moveNextExp};
            var initBlock = new List<Expression>();
            var methodBody = new List<Expression>();

            Expression orCondExp = null;
            foreach (TypeSerializer srl in fieldSerializers)
            {
                Expression t = srl.GetSerializerExp(currentExp, codecExp, stateVariables, initBlock);

                Expression exp = Expression.IsTrue(t);
                orCondExp = orCondExp == null ? exp : Expression.And(orCondExp, exp);
            }

            if (orCondExp == null)
                throw new SerializerException("No field serializers have been defined");


            // current = enumerator.Current;
            methodBody.Add(setCurrentValue);

            // var state1 = current.Field1; codec.Write(state1); ...
            methodBody.AddRange(initBlock);


            // int codecPos;
            ParameterExpression codecPosExp = Expression.Parameter(typeof (int), "codecPos");

            // codec.BufferPos
            MemberExpression codecBufferPos = Expression.PropertyOrField(codecExp, "BufferPos");

            // while (true)
            methodBody.Add(
                Expression.Loop(
                    Expression.Block(
                        new Expression[]
                            {
                                // moveNext = enumerator.MoveNext();
                                Expression.Assign(
                                    moveNextExp,
                                    Expression.Call(enumeratorExp, typeof (IEnumerator).GetMethod("MoveNext"))),
                                // if
                                Expression.IfThen(
                                    // (!moveNext)
                                    Expression.IsFalse(moveNextExp),
                                    // break;
                                    Expression.Break(breakOuter)),
                                // current = enumerator.Current;
                                setCurrentValue,
                                Expression.Block(
                                    // int codecPos;
                                    new[] {codecPosExp},
                                    // codecPos = codec.BufferPos
                                    Expression.Assign(codecPosExp, codecBufferPos),
                                    // if (!(delta1() && delta2() && ...))
                                    Expression.IfThen(
                                        // ReSharper disable AssignNullToNotNullAttribute
                                        Expression.Not(orCondExp),
                                        // ReSharper restore AssignNullToNotNullAttribute
                                        Expression.Block(
                                            Expression.Assign(codecBufferPos, codecPosExp),
                                            Expression.Break(breakOuter)
                                            )
                                        ))
                            }),
                    breakOuter));

            // return moveNext;
            methodBody.Add(moveNextExp);

            Expression<Func<StreamCodec, IEnumerator<T>, bool>> serializeExp =
                Expression.Lambda<Func<StreamCodec, IEnumerator<T>, bool>>(
                    Expression.Block(stateVariables, methodBody),
                    "Serialize",
                    // parameter codec, IEnumerator<T>
                    new[] {codecExp, enumeratorExp}
                    );

            return serializeExp.Compile();
        }

        public static Func<StreamCodec, IEnumerator<T>, bool> DeSerialize(IEnumerable<TypeSerializer> serializers)
        {
            throw new NotImplementedException();
        }
    }
}