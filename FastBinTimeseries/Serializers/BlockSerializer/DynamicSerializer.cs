using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
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

        public virtual void MakeReadonly()
        {
            IsInitialized = true;
        }
    }

    public class DynamicSerializer<T> : DynamicSerializer
    {
        private Action<CodecReader, Buffer<T>, int> _deSerialize;
        private Func<CodecWriter, IEnumerator<T>, bool> _serialize;

        private DynamicSerializer()
        {
        }

        /// * codec - writes output to this codec using Write*() methods
        /// * enumerator to go through the input T values.
        ///     MoveNext() had to be called on it and returned true before passing it in.
        /// * returns false if no more items, or true if there are more items but the codec buffer is full
        public Func<CodecWriter, IEnumerator<T>, bool> Serialize
        {
            get
            {
                ThrowOnNotInitialized();
                return _serialize;
            }
        }

        /// * codec to read the values from
        /// * result will get all the generated values
        /// * maxItemCount - maximum number of items to be deserialized
        public Action<CodecReader, Buffer<T>, int> DeSerialize
        {
            get
            {
                ThrowOnNotInitialized();
                return _deSerialize;
            }
        }

        public int GetMinimumBlockSize()
        {
            ThrowOnNotInitialized();
            // TODO: calculate!
            return 1024;
        }

        public override void MakeReadonly()
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

                Expression<Func<CodecWriter, IEnumerator<T>, bool>> serializeExp =
                    Expression.Lambda<Func<CodecWriter, IEnumerator<T>, bool>>(
                        Expression.Block(
                            StateVariables.Values.Concat(localVars),
                            methodBody),
                        "Serialize", parameters);

                _serialize = serializeExp.Compile();

                StateVariables.Clear();
                methodBody = GenerateDeSerializer(out parameters, out localVars);

                Expression<Action<CodecReader, Buffer<T>, int>> deSerializeExp =
                    Expression.Lambda<Action<CodecReader, Buffer<T>, int>>(
                        Expression.Block(StateVariables.Values.Concat(localVars), methodBody),
                        "DeSerialize", parameters);

                _deSerialize = deSerializeExp.Compile();
            }
            finally
            {
                StateVariables.Clear();
            }

            base.MakeReadonly();
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
        /// bool Serialize(CodecWriter codec, IEnumerator&ltT> enumerator)
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
        ///         codecPos = codec.Count;
        ///         if (! (codec.Write(delta1) && codec.Write(delta2) && ...) ) {
        ///             codec.Count = codecPos;
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
            ParameterExpression codecParam = Expression.Parameter(typeof (CodecWriter), "codec");

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

            // codec.Count
            MemberExpression codecBufferPos = Expression.PropertyOrField(codecParam, "Count");

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
                                            // codecPos = codec.Count
                                            Expression.Assign(codecPosExp, codecBufferPos),
                                            // if (!writeDeltas)
                                            Expression.IfThen(
                                                Expression.Not(srl.Item2),
                                                Expression.Block(
                                                    // codec.Count = codecPos;
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
        /// void DeSerialize(CodecReader codec, Buffer&lt;T> result, int maxItemCount)
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
            ParameterExpression codecParam = Expression.Parameter(typeof (CodecReader), "codec");

            // param: Buffer<T> result
            ParameterExpression resultParam = Expression.Parameter(typeof (Buffer<T>), "result");

            // param: int maxItemCount
            ParameterExpression maxItemCountParam = Expression.Parameter(typeof (int), "maxItemCount");

            // int count;
            ParameterExpression countVar = Expression.Variable(typeof (int), "count");

            LabelTarget breakLabel = Expression.Label("loopBreak");

            Tuple<Expression, Expression> srl = RootField.GetDeSerializer(codecParam);

            // parameter CodecReader codec, Buffer&lt;T> result, int maxItemCount
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

        public static DynamicSerializer<T> CreateDefault()
        {
            var srl = new DynamicSerializer<T>();
            srl.RootField = srl.GetDefaultField(typeof (T), "root");
            return srl;
        }

        public static DynamicSerializer<T> CreateFromReader(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            var srl = new DynamicSerializer<T>();
            srl.RootField = BaseField.FieldFromReader(srl, reader, typeMap);
            srl.MakeReadonly();
            return srl;
        }

        public void WriteCustomHeader(BinaryWriter writer)
        {
            RootField.InitNew(writer);
            MakeReadonly();
        }
    }
}