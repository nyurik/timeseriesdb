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
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb.Serializers.BlockSerializer
{
    public class SimpleField : BaseField
    {
        [UsedImplicitly]
        protected SimpleField()
        {
        }

        public SimpleField([NotNull] IStateStore serializer, [NotNull] Type valueType, string stateName)
            : base(Version10, serializer, valueType, stateName)
        {
        }

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
        {
            switch (ValueTypeCode)
            {
                case TypeCode.Boolean:
                    valueExp = Expression.Condition(
                        valueExp, Expression.Constant((byte) 1), Expression.Constant((byte) 0));
                    break;
                case TypeCode.SByte:
                    valueExp = Expression.Convert(valueExp, typeof (byte));
                    break;
                case TypeCode.Byte:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            var writeMethod = Expression.Call(codec, "WriteByte", null, valueExp);

            return new Tuple<Expression, Expression>(writeMethod, writeMethod);
        }

        protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
        {
            Expression readMethod = Expression.Call(codec, "ReadByte", null);
            switch (ValueTypeCode)
            {
                case TypeCode.Boolean:
                    readMethod = Expression.Condition(
                        Expression.Equal(readMethod, Expression.Constant((byte) 0)),
                        Expression.Constant(false), Expression.Constant(true));
                    break;
                case TypeCode.SByte:
                    readMethod = Expression.Convert(readMethod, typeof (sbyte));
                    break;
                case TypeCode.Byte:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return new Tuple<Expression, Expression>(readMethod, readMethod);
        }

        public override int GetMaxByteSize()
        {
            return CodecBase.MaxBytesFor8;
        }

        protected override bool IsValidVersion(Version ver)
        {
            return ver == Version10;
        }

        protected override void MakeReadonly()
        {
            ThrowOnInitialized();
            switch (ValueTypeCode)
            {
                case TypeCode.Boolean:
                case TypeCode.SByte:
                case TypeCode.Byte:
                    break;
                default:
                    throw new SerializerException(
                        "Value {0} has an unsupported type {1}", StateName, ValueType.AssemblyQualifiedName);
            }

            base.MakeReadonly();
        }
    }
}