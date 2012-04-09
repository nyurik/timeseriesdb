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
using System.IO;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb.Serializers.BlockSerializer
{
    public class ScaledDeltaIntField : ScaledDeltaBaseField
    {
        private long _divider = 1;

        [UsedImplicitly]
        protected ScaledDeltaIntField()
        {
        }

        /// <summary>
        /// Integer and Float delta serializer.
        /// </summary>
        /// <param name="stateStore">Serializer with the state</param>
        /// <param name="fieldType">Type of value to store</param>
        /// <param name="stateName">Name of the value (for debugging)</param>
        public ScaledDeltaIntField([NotNull] IStateStore stateStore, [NotNull] Type fieldType, string stateName)
            : base(Versions.Ver0, stateStore, fieldType, stateName)
        {
        }

        /// <summary> Value is divided by this parameter before storage </summary>
        public long Divider
        {
            get { return _divider; }
            set
            {
                ThrowOnInitialized();

                var max = GetMaxDivider();
                if (value < 1 || (ulong)value > max)
                    throw new ArgumentOutOfRangeException(
                        "value", value,
                        string.Format(
                            "Divider for value {0} ({1}) must be >= 1 and <= {2}",
                            StateName, FieldType.FullName, max));
                
                _divider = value;
            }
        }

        private ulong GetMaxDivider()
        {
            switch (FieldType.GetTypeCode())
            {
                case TypeCode.Char:
                    return char.MaxValue;
                case TypeCode.Int16:
                    return (ulong) Int16.MaxValue;
                case TypeCode.UInt16:
                    return UInt16.MaxValue;
                case TypeCode.Int32:
                    return Int32.MaxValue;
                case TypeCode.UInt32:
                    return UInt32.MaxValue;
                case TypeCode.Int64:
                    return Int64.MaxValue;
                case TypeCode.UInt64:
                    return UInt64.MaxValue;
                default:
                    throw new SerializerException(
                        "Value {0} has an unsupported type {1}", StateName, FieldType.ToDebugStr());
            }
        }

        protected override void InitNewField(BinaryWriter writer)
        {
            base.InitNewField(writer);
            writer.Write(Divider);
        }

        protected override void InitExistingField(BinaryReader reader, Func<string, Type> typeResolver)
        {
            base.InitExistingField(reader, typeResolver);
            Divider = reader.ReadInt64();
        }

        protected override bool IsValidVersion(Version ver)
        {
            return ver == Versions.Ver0;
        }

        protected override Expression ValueToState(Expression codec, Expression value)
        {
            //
            // valueGetter(): (#long) (value / divider)
            //
            Expression getValExp =
                Divider != 1
                    ? Expression.Divide(value, Const(Divider, FieldType))
                    : value;

            // do not convertCheck, ulong would not fit otherwise
            return getValExp.Type != typeof (long)
                       ? Expression.Convert(getValExp, typeof (long))
                       : getValExp;
        }

        /// <summary>
        /// valueGetter: (T)state * divider
        /// </summary>
        protected override Expression StateToValue(Expression stateVar)
        {
            if (stateVar == null) throw new ArgumentNullException("stateVar");

            Expression getValExp = stateVar.Type != FieldType
                                       ? Expression.Convert(stateVar, FieldType)
                                       : stateVar;
            return Divider != 1
                       ? Expression.Multiply(getValExp, Const(Divider, FieldType))
                       : getValExp;
        }

        protected override bool Equals(BaseField baseOther)
        {
            var other = (ScaledDeltaIntField)baseOther;
            return _divider == other._divider;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                // ReSharper disable NonReadonlyFieldInGetHashCode
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ _divider.GetHashCode();
                return hashCode;
                // ReSharper restore NonReadonlyFieldInGetHashCode
            }
        }
    }
}