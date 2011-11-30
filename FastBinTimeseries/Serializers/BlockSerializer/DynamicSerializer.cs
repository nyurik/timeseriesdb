using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using JetBrains.Annotations;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public interface IStateStore
    {
        ParameterExpression GetOrCreateStateVar(string name, Type valueType, out bool wasCreated);
        BaseField GetDefaultField([NotNull] Type valueType, [NotNull] string name);
    }

    public abstract class DynamicSerializer : Initializable, IStateStore
    {
        protected readonly Dictionary<string, ParameterExpression> StateVariables =
            new Dictionary<string, ParameterExpression>();

        private BaseField _rootField;

        public BaseField RootField
        {
            get { return _rootField; }
            set
            {
                ThrowOnInitialized();
                if (value == null) throw new ArgumentNullException("value");
                _rootField = value;
            }
        }

        #region IStateStore Members

        ParameterExpression IStateStore.GetOrCreateStateVar(string name, Type valueType, out bool wasCreated)
        {
            ThrowOnInitialized();
            ParameterExpression stateVar;
            if (StateVariables.TryGetValue(name, out stateVar))
            {
                if (stateVar.Type != valueType)
                    throw new SerializerException(
                        "State '{0}' was requested as type {1}, but was previously created as type {2}",
                        name, valueType, stateVar.Type);
                wasCreated = false;
                return stateVar;
            }

            stateVar = Expression.Variable(valueType, name);
            StateVariables.Add(name, stateVar);
            wasCreated = true;
            return stateVar;
        }

        public BaseField GetDefaultField(Type valueType, string name)
        {
            if (valueType == null)
                throw new ArgumentNullException("valueType");
            if (name == null)
                throw new ArgumentNullException("name");
            if (valueType.IsArray)
                throw new SerializerException("Arrays are not supported ({0})", valueType);

            if (valueType.IsPrimitive)
            {
                switch (Type.GetTypeCode(valueType))
                {
                    case TypeCode.SByte:
                    case TypeCode.Byte:
                        return new SimpleField(this, valueType, name);

                    case TypeCode.Char:
                    case TypeCode.Int16:
                    case TypeCode.UInt16:
                    case TypeCode.Int32:
                    case TypeCode.UInt32:
                    case TypeCode.Int64:
                    case TypeCode.UInt64:
                    case TypeCode.Single:
                    case TypeCode.Double:
                    case TypeCode.Decimal:
                        return new ScaledDeltaField(this, valueType, name);

                    default:
                        throw new SerializerException("Unsupported primitive type {0}", valueType);
                }
            }

            if (valueType == typeof (UtcDateTime))
                return new UtcDateTimeField(this, name);

            return new ComplexField(this, valueType, name);
        }

        #endregion

        protected virtual void Validate()
        {
            IsInitialized = true;
        }
    }

    public class DynamicSerializer<T> : DynamicSerializer
    {
        private Action<StreamCodec, Buff<T>, int> _deSerialize;
        private Func<StreamCodec, IEnumerator<T>, bool> _serialize;

        public DynamicSerializer()
        {
            RootField = GetDefaultField(typeof (T), "root");
        }

        public Func<StreamCodec, IEnumerator<T>, bool> Serialize
        {
            get
            {
                Validate();
                return _serialize;
            }
        }

        public Action<StreamCodec, Buff<T>, int> DeSerialize
        {
            get
            {
                Validate();
                return _deSerialize;
            }
        }

        protected override void Validate()
        {
            if (IsInitialized)
                return;

            try
            {
                // A bit hacky way: StateVariables is reset for each call to Generate*Serializer()
                StateVariables.Clear();

                ParameterExpression[] parameters;
                ParameterExpression[] localVars;
                IEnumerable<Expression> methodBody = GenerateSerializer(out parameters, out localVars);

                Expression<Func<StreamCodec, IEnumerator<T>, bool>> serializeExp =
                    Expression.Lambda<Func<StreamCodec, IEnumerator<T>, bool>>(
                        Expression.Block(
                            StateVariables.Values.Concat(localVars),
                            methodBody),
                        "Serialize", parameters);

                _serialize = serializeExp.Compile();

                StateVariables.Clear();
                methodBody = GenerateDeSerializer(out parameters, out localVars);

                Expression<Action<StreamCodec, Buff<T>, int>> deSerializeExp =
                    Expression.Lambda<Action<StreamCodec, Buff<T>, int>>(
                        Expression.Block(StateVariables.Values.Concat(localVars), methodBody),
                        "DeSerialize", parameters);

                _deSerialize = deSerializeExp.Compile();
            }
            finally
            {
                StateVariables.Clear();
            }

            base.Validate();
        }

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
        ///     int count = 1;
        ///     int codecPos;
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
        ///         codecPos = codec.BufferPos;
        ///         if (! (codec.Write(delta1) && codec.Write(delta2) && ...) ) {
        ///             codec.BufferPos = codecPos;
        ///             break;
        ///         }
        /// 
        ///         count++;
        ///     }
        /// 
        ///     codec.WriteHeader(count);
        /// 
        ///     return moveNext;
        /// }
        private IEnumerable<Expression> GenerateSerializer(out ParameterExpression[] parameters,
                                                           out ParameterExpression[] localVars)
        {
            // param: codec
            ParameterExpression codecParam = Expression.Parameter(typeof (StreamCodec), "codec");

            // param: IEnumerator<T> data
            ParameterExpression enumeratorParam = Expression.Parameter(typeof (IEnumerator<T>), "enumerator");

            // bool moveNext;
            ParameterExpression moveNextVar = Expression.Variable(typeof (bool), "moveNext");

            // int count;
            ParameterExpression countVar = Expression.Variable(typeof (int), "count");

            // T current;
            ParameterExpression currentVar = Expression.Variable(typeof (T), "current");

            // current = enumerator.Current;
            BinaryExpression setCurrentExp = Expression.Assign(
                currentVar, Expression.PropertyOrField(enumeratorParam, "Current"));

            LabelTarget breakLabel = Expression.Label("loopBreak");


            Tuple<Expression, Expression> srl = RootField.GetSerializer(currentVar, codecParam);

            // int codecPos;
            ParameterExpression codecPosExp = Expression.Parameter(typeof (int), "codecPos");

            // codec.BufferPos
            MemberExpression codecBufferPos = Expression.PropertyOrField(codecParam, "BufferPos");

            // parameter codec, IEnumerator<T>
            parameters = new[] {codecParam, enumeratorParam};

            // local variables used by the method body (excluding the state variables)
            localVars = new[] {currentVar, countVar, moveNextVar};

            return
                new[]
                    {
                        // count = 1;
                        Expression.Assign(countVar, Expression.Constant(1)),
                        // current = enumerator.Current;
                        setCurrentExp,
                        // codec.SkipHeader();
                        Expression.Call(codecParam, "SkipHeader", null),
                        // var state1 = current.Field1; codec.Write(state1); ...
                        srl.Item1,
                        // while (true)
                        Expression.Loop(
                            Expression.Block(
                                new Expression[]
                                    {
                                        // moveNext = enumerator.MoveNext();
                                        Expression.Assign(
                                            moveNextVar,
                                            Expression.Call(enumeratorParam, typeof (IEnumerator).GetMethod("MoveNext")))
                                        ,
                                        // if
                                        Expression.IfThen(
                                            // (!moveNext)
                                            Expression.IsFalse(moveNextVar),
                                            // break;
                                            Expression.Break(breakLabel)),
                                        // current = enumerator.Current;
                                        setCurrentExp,
                                        Expression.Block(
                                            // int codecPos;
                                            new[] {codecPosExp},
                                            // codecPos = codec.BufferPos
                                            Expression.Assign(codecPosExp, codecBufferPos),
                                            // if (!writeDeltas)
                                            Expression.IfThen(
                                                Expression.Not(srl.Item2),
                                                Expression.Block(
                                                    // codec.BufferPos = codecPos;
                                                    Expression.Assign(codecBufferPos, codecPosExp),
                                                    // break;
                                                    Expression.Break(breakLabel)
                                                    )
                                                )),
                                        // count++;
                                        Expression.PreIncrementAssign(countVar)
                                    }),
                            breakLabel),
                        // codec.WriteHeader();
                        Expression.Call(codecParam, "WriteHeader", null, countVar),
                        // return moveNext;
                        moveNextVar,
                    };
        }

        ///
        /// Generated code:
        /// 
        /// * codec to read the values from
        /// * result will get all the generated values
        /// * maxItemCount - maximum number of items to be deserialized
        /// 
        /// void DeSerialize(StreamCodec codec, Buff&lt;T> result, int maxItemCount)
        /// {
        ///     int count = codec.ReadHeader();
        ///     if (count > maxItemCount)
        ///         count = maxItemCount;
        ///     
        ///     var state1, state2, ...;
        /// 
        ///     result.Add(ReadField(codec, ref state));
        /// 
        ///     while (true) {
        /// 
        ///         count--;
        ///         if (count == 0)
        ///             break;
        /// 
        ///         result.Add(ReadField(codec, ref state));
        ///     }
        /// }
        /// 
        private IEnumerable<Expression> GenerateDeSerializer(out ParameterExpression[] parameters,
                                                             out ParameterExpression[] localVars)
        {
            // param: codec
            ParameterExpression codecParam = Expression.Parameter(typeof (StreamCodec), "codec");

            // param: Buff<T> result
            ParameterExpression resultParam = Expression.Parameter(typeof (Buff<T>), "result");

            // param: int maxItemCount
            ParameterExpression maxItemCountParam = Expression.Parameter(typeof (int), "maxItemCount");

            // int count;
            ParameterExpression countVar = Expression.Variable(typeof (int), "count");

            LabelTarget breakLabel = Expression.Label("loopBreak");

            Tuple<Expression, Expression> srl = RootField.GetDeSerializer(codecParam);

            // parameter StreamCodec codec, Buff&lt;T> result, int maxItemCount
            parameters = new[] {codecParam, resultParam, maxItemCountParam};

            localVars = new[] {countVar};

            return
                new Expression[]
                    {
                        // count = codec.ReadHeader();
                        Expression.Assign(countVar, Expression.Call(codecParam, "ReadHeader", null)),
                        // if (maxItemCount < count)
                        //     count = maxItemCount;
                        Expression.IfThen(
                            Expression.LessThan(maxItemCountParam, countVar),
                            Expression.Assign(countVar, maxItemCountParam)),
                        // result.Add(ReadField(codec, ref state));
                        Expression.Call(resultParam, "Add", null, srl.Item1),
                        // while (true)
                        Expression.Loop(
                            // {
                            Expression.Block(
                                new Expression[]
                                    {
                                        // count--;
                                        Expression.PreDecrementAssign(countVar),
                                        // if (
                                        Expression.IfThen(
                                            // count == 0)
                                            Expression.Equal(countVar, Expression.Constant(0)),
                                            // break;
                                            Expression.Break(breakLabel)),
                                        // result.Add(ReadField(codec, ref state));
                                        Expression.Call(resultParam, "Add", null, srl.Item2)
                                    }),
                            breakLabel)
                    };
        }
    }

    public class Buff<T>
    {
        private T[] _buffer;
        private int _count;

        public Buff()
        {
            _buffer = new T[4];
        }

        public ArraySegment<T> Buffer
        {
            get { return new ArraySegment<T>(_buffer, 0, _count); }
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

        public void Reset()
        {
            Array.Clear(_buffer, 0, _count);
            _count = 0;
        }
    }
}