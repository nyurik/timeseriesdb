#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of TimeSeriesDb library
 * 
 *  TimeSeriesDb is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  TimeSeriesDb is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with TimeSeriesDb.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb.Serializers.BlockSerializer
{
    public abstract class ScaledDeltaBaseField : BaseField
    {
        private DeltaType _deltaType;

        protected ScaledDeltaBaseField()
        {
        }

        protected ScaledDeltaBaseField(
            Version version, [NotNull] IStateStore stateStore, [NotNull] Type fieldType, string stateName)
            : base(version, stateStore, fieldType, stateName)
        {
        }

        /// <summary> When the field value can only increase or only decrease, set this value to store deltas as unsigned integer.
        /// This results in some storage space gains - for instance a delta between 64 and 127 will now need 1 byte instead of 2.
        /// </summary>
        public DeltaType DeltaType
        {
            get { return _deltaType; }
            set
            {
                ThrowOnInitialized();
                if (!Enum.IsDefined(typeof (DeltaType), value))
                    throw new ArgumentOutOfRangeException("value", value, "This value is not defined in DeltaType enum");
                _deltaType = value;
            }
        }

        public override int MaxByteSize
        {
            get
            {
                // TODO: optimize to make this number smaller depending on the field type and scaling parameters
                return CodecBase.MaxBytesFor64;
            }
        }

        protected override void InitNewField(BinaryWriter writer)
        {
            base.InitNewField(writer);
            writer.Write((byte) DeltaType);
        }

        protected override void InitExistingField(BinaryReader reader, Func<string, Type> typeResolver)
        {
            base.InitExistingField(reader, typeResolver);
            DeltaType = (DeltaType) reader.ReadByte();
        }

        protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
        {
            //
            // long stateVar;
            //
            bool needToInit;
            ParameterExpression stateVarExp = StateStore.GetOrCreateStateVar(StateName, typeof (long), out needToInit);

            Expression stateToValueExp = StateToValue(stateVarExp);

            //
            // Only if the state needs to be initialized:
            //
            // stateVar = codec.ReadValue();
            // return stateVar * scale;
            //
            Expression initExp =
                needToInit
                    ? Expression.Block(
                        Expression.Assign(stateVarExp, ReadSignedValue(codec)),
                        DebugValueExp(codec, stateVarExp, "DeltaFld ReadInit"),
                        stateToValueExp)
                    : null;

            //
            // stateVar += codec.ReadValue();
            // return stateVar * scale;
            //
            Expression deltaExp =
                Expression.Block(
                    Expression.AddAssign(stateVarExp, ReadDeltaExp(codec)),
                    DebugValueExp(codec, stateVarExp, "DeltaFld ReadDelta"),
                    stateToValueExp);

            return new Tuple<Expression, Expression>(initExp ?? deltaExp, deltaExp);
        }

        protected Expression ReadDeltaExp(Expression codec)
        {
            //
            // How to read value - depending on delta type
            //
            Expression readValExp;
            switch (DeltaType)
            {
                case DeltaType.Signed:
                    readValExp = ReadSignedValue(codec);
                    break;
                case DeltaType.Positive:
                    readValExp = Expression.Convert(ReadUnsignedValue(codec), typeof (long));
                    break;
                case DeltaType.Negative:
                    readValExp = Expression.Negate(Expression.Convert(ReadUnsignedValue(codec), typeof (long)));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return readValExp;
        }

        protected Expression ValidateDeltaExp(
            Expression codec, ParameterExpression deltaExp, ParameterExpression valueExp)
        {
            //
            // DeltaType.Positive: if (delta < 0) throw SerializerException();
            // DeltaType.Negative: if (delta > 0) throw SerializerException();
            //
            switch (DeltaType)
            {
                case DeltaType.Positive:
                    return Expression.IfThen(
                        Expression.LessThan(deltaExp, Const((long) 0)),
                        ThrowSerializer(
                            codec,
                            "Value {0} is smaller than previous value in a positive delta field",
                            valueExp));
                case DeltaType.Negative:
                    return
                        Expression.IfThen(
                            Expression.GreaterThan(deltaExp, Const((long) 0)),
                            ThrowSerializer(
                                codec,
                                "Value {0} is larger than previous value in a negative delta field",
                                valueExp));
                default:
                    return null;
            }
        }

        protected Expression WriteDeltaValue(Expression codec, ParameterExpression varDeltaExp)
        {
            //          
            // DeltaType.Signed: return codec.WriteSignedValue(delta);
            // DeltaType.Positive: return codec.WriteUnsignedValue(delta);
            // DeltaType.Negative: return codec.WriteUnsignedValue(-delta);
            //
            switch (DeltaType)
            {
                case DeltaType.Signed:
                    return WriteSignedValue(codec, varDeltaExp);
                case DeltaType.Positive:
                    return WriteUnsignedValue(codec, Expression.Convert(varDeltaExp, typeof (ulong)));
                case DeltaType.Negative:
                    return WriteUnsignedValue(codec, Expression.Convert(Expression.Negate(varDeltaExp), typeof (ulong)));
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        protected abstract Expression StateToValue([NotNull] Expression stateVar);

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
        {
            //
            // long stateVar;
            //
            bool needToInit;
            ParameterExpression stateVarExp = StateStore.GetOrCreateStateVar(StateName, typeof (long), out needToInit);

            Expression getValExp = ValueToState(codec, valueExp);

            Expression validationExp = OnWriteValidation(codec, valueExp, stateVarExp);

            //
            // stateVar = valueGetter();
            // DEBUG: stateVar
            // 
            // codec.WriteSignedValue(stateVar);
            //
            Expression initExp =
                needToInit
                    ? Expression.Block(
                        Expression.Assign(stateVarExp, getValExp),
                        DebugValueExp(codec, stateVarExp, "ScaledFld WriteInit"),
                        validationExp ?? Expression.Empty(),
                        // The first item is always stored as a signed long
                        WriteSignedValue(codec, stateVarExp))
                    : null;

            ParameterExpression varState2Exp = Expression.Variable(typeof (long), "state2");
            ParameterExpression varDeltaExp = Expression.Variable(typeof (long), "delta");

            //
            // stateVar2 = valueGetter();
            // delta = stateVar2 - stateVar
            //
            var exprs =
                new List<Expression>
                    {
                        Expression.Assign(varState2Exp, getValExp),
                        Expression.Assign(varDeltaExp, Expression.Subtract(varState2Exp, stateVarExp))
                    };

            Expression exp = ValidateDeltaExp(codec, varDeltaExp, varState2Exp);
            if (exp != null)
                exprs.Add(exp);

            //
            // stateVar = stateVar2;
            // DEBUG: DebugValue(stateVar);
            // Optional: ValidateState
            //
            exprs.Add(Expression.Assign(stateVarExp, varState2Exp));
            exprs.Add(DebugValueExp(codec, stateVarExp, "ScaledFld WriteDelta"));
            if (validationExp != null)
                exprs.Add(validationExp);

            exprs.Add(WriteDeltaValue(codec, varDeltaExp));

            Expression deltaExp = Expression.Block(
                typeof (bool),
                new[] {varState2Exp, varDeltaExp},
                exprs);

            return new Tuple<Expression, Expression>(initExp ?? deltaExp, deltaExp);
        }

        protected virtual Expression OnWriteValidation(
            Expression codec, Expression valueExp, ParameterExpression stateVarExp)
        {
            return null;
        }

        protected abstract Expression ValueToState(Expression codec, Expression value);
    }

    public enum DeltaType : byte
    {
        Signed,
        Positive,
        Negative,
    }
}