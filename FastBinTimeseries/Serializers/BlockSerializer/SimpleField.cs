#region COPYRIGHT

/*
 *     Copyright 2009-2011 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of FastBinTimeseries library
 * 
 *  FastBinTimeseries is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  FastBinTimeseries is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with FastBinTimeseries.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using System.IO;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class SimpleField : BaseField
    {
        protected SimpleField()
        {
        }

        public SimpleField([NotNull] IStateStore serializer, [NotNull] Type valueType, string stateName)
            : base(Version10, serializer, valueType, stateName)
        {
        }

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
        {
            MethodCallExpression writeMethod;
            switch (ValueTypeCode)
            {
                case TypeCode.SByte:
                    writeMethod = Expression.Call(
                        codec, "WriteByte", null,
                        Expression.Convert(valueExp, typeof (byte)));
                    break;
                case TypeCode.Byte:
                    writeMethod = Expression.Call(codec, "WriteByte", null, valueExp);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return new Tuple<Expression, Expression>(writeMethod, writeMethod);
        }

        protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
        {
            Expression readMethod;
            switch (ValueTypeCode)
            {
                case TypeCode.SByte:
                    readMethod = Expression.Convert(Expression.Call(codec, "ReadByte", null), typeof (sbyte));
                    break;
                case TypeCode.Byte:
                    readMethod = Expression.Call(codec, "ReadByte", null);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return new Tuple<Expression, Expression>(readMethod, readMethod);
        }

        public override int GetMinByteSize()
        {
            return CodecBase.MaxBytesFor8;
        }

        public override int GetMaxByteSize()
        {
            return CodecBase.MaxBytesFor8;
        }

        protected override void InitExistingField(BinaryReader reader, Func<string, Type> typeResolver)
        {
            base.InitExistingField(reader, typeResolver);
            if (Version != Version10)
                throw new IncompatibleVersionException(GetType(), Version);
        }

        protected override void MakeReadonly()
        {
            ThrowOnInitialized();
            switch (ValueTypeCode)
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                    break;
                default:
                    throw new SerializerException(
                        "Value {0} has an unsupported type {0}", StateName, ValueType.AssemblyQualifiedName);
            }

            base.MakeReadonly();
        }
    }
}