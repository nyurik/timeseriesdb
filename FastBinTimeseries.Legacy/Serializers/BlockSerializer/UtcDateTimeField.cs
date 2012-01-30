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
using System.Reflection;
using JetBrains.Annotations;
using NYurik.FastBinTimeseries.CommonCode;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class UtcDateTimeField : BaseField
    {
        private ScaledDeltaField _deltaField;

        protected UtcDateTimeField()
        {
        }

        /// <summary>
        /// Integer and Float delta serializer.
        /// </summary>
        public UtcDateTimeField([NotNull] IStateStore serializer, string stateName)
            : base(Version10, serializer, typeof (UtcDateTime), stateName)
        {
            _deltaField = new ScaledDeltaField(serializer, typeof (long), stateName);
        }

        /// <summary>Value is divided by this parameter before storage</summary>
        public TimeSpan TimeDivider
        {
            get { return TimeSpan.FromTicks(_deltaField.Divider); }
            set { _deltaField.Divider = ValidateDivider(value); }
        }

        private static long ValidateDivider(TimeSpan value)
        {
            if (value < TimeSpan.Zero)
                throw new SerializerException(
                    "Divider ({0}) must be positive", value);
            if (value > TimeSpan.FromDays(1))
                throw new SerializerException("Divider {0} is > 1 day", value);
            if (value > TimeSpan.Zero && TimeSpan.TicksPerDay%value.Ticks != 0)
                throw new SerializerException(
                    "TimeSpan.TicksPerDay must be divisible by time slice {0}", value);
            return value == TimeSpan.Zero ? 1 : value.Ticks;
        }

        public override int GetMinByteSize()
        {
            return _deltaField.GetMinByteSize();
        }

        public override int GetMaxByteSize()
        {
            return _deltaField.GetMaxByteSize();
        }

        protected override void InitNewField(BinaryWriter writer)
        {
            base.InitNewField(writer);
            _deltaField.InitNew(writer);
        }

        protected override void InitExistingField(BinaryReader reader, Func<string, Type> typeResolver)
        {
            base.InitExistingField(reader, typeResolver);
            if (Version != Version10)
                throw new IncompatibleVersionException(GetType(), Version);

            BaseField fld = FieldFromReader(StateStore, reader, typeResolver);
            _deltaField = fld as ScaledDeltaField;
            if (_deltaField == null)
                throw new SerializerException(
                    "Unexpected field {0} was deserialized instead of {1}", fld,
                    typeof (ScaledDeltaField).AssemblyQualifiedName);

            ValidateDivider(TimeDivider);
        }

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
        {
            return _deltaField.GetSerializer(Expression.Property(valueExp, "Ticks"), codec);
        }

        protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
        {
            Tuple<Expression, Expression> res = _deltaField.GetDeSerializer(codec);

            ConstructorInfo ctor = typeof (UtcDateTime).GetConstructor(new[] {typeof (long)});
            if (ctor == null)
                throw new SerializerException("UtcDateTime(long) constructor was not found");

            return new Tuple<Expression, Expression>(
                Expression.New(ctor, res.Item1),
                Expression.New(ctor, res.Item2));
        }
    }
}