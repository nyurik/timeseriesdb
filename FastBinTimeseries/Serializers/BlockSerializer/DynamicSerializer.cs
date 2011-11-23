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
        /// Generated code:
        /// 
        /// 
        /// * codec - writes output to this codec using Write*() methods
        /// * enumerator to go through the input T values.
        ///     MoveNext() had to be called on it before passing it in.
        /// * returns false if no more items, or true if there are more items but the codec buffer is full
        /// 
        /// bool Serialize(StreamCodec codec, IEnumerator&ltT> enumerator)
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

            LabelTarget breakLabel = Expression.Label();

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
                                    Expression.Break(breakLabel)),
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
                                            Expression.Break(breakLabel)
                                            )
                                        ))
                            }),
                    breakLabel));

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

        ///
        /// Generated code:
        /// 
        /// * codec to read the values from
        /// * resultBuffer will get all the generated values
        /// * maxItemCount - maximum number of items to be deserialized
        /// 
        /// void DeSerialize(StreamCodec codec, Buff&lt;T> resultBuffer, int maxItemCount)
        /// {
        ///     int itemCount = codec.ItemCount;
        ///     if (itemCount > maxItemCount)
        ///         itemCount = maxItemCount;
        ///     
        ///     var state1, state2, ...;
        /// 
        ///     codec.Read(state1);
        ///     codec.Read(state2);
        ///     ...
        /// 
        ///     while (true) {
        /// 
        ///         itemCount--;
        ///         if (itemCount == 0)
        ///             break;
        /// 
        ///         @if T is a struct
        ///             T newValue = new T();
        ///         @else
        ///             T newValue = FormatterServices.GetUninitializedObject(typeof(T)); 
        /// 
        ///         newValue.Field1 = ReadField1(codec, ref state1);
        ///         newValue.Field2 = ReadField2(codec, ref state2);
        ///         ...
        /// 
        ///         resultBuffer.Add(newValue);
        ///     }
        /// }
        /// 
        public static Action<StreamCodec, Buff<T>, int> DeSerialize(IEnumerable<TypeSerializer> serializers)
        {
            throw new NotImplementedException();
        }
    }

    internal class Buff<T>
    {
        private int _count;
        private T[] _buffer;

        public Buff()
        {
            _buffer = new T[4];
        }

        public void Add(T value)
        {
            if (_count == _buffer.Length)
            {
                var tmp = new T[_buffer.Length*2];
                Array.Copy(_buffer, tmp, _buffer.Length);
                _buffer = tmp;
            }
            _buffer[_count++] = value;
        }

        public ArraySegment<T> Buffer
        {
            get { return new ArraySegment<T>(_buffer, 0, _count); }
        }

        public void Reset()
        {
            Array.Clear(_buffer, 0, _count);
            _count = 0;
        }
    }
}
