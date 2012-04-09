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
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using JetBrains.Annotations;

namespace NYurik.TimeSeriesDb.Serializers.BlockSerializer
{
    public class ComplexField : BaseField
    {
        #region Mode enum

        [Flags]
        public enum Mode
        {
            Auto = 0x0,

            NonPublic = 0x01,

            Fields = 0x02,
            Properties = 0x04,

            Constructor = 0x08,

//            SearchUnderscore = 0x10,
//            SearchMUnderscore = 0x20,
//            SearchLowercase = 0x40,
//            SearchCapitalized = 0x80,
        }

        #endregion

        private ConstructorInfo _constructor;
        private IList<SubFieldInfo> _fields;

        [UsedImplicitly]
        protected ComplexField()
        {
        }

        public ComplexField([NotNull] IStateStore stateStore, [NotNull] Type fieldType, string stateName)
            : base(Versions.Ver1, stateStore, fieldType, stateName)
        {
            if (fieldType.IsArray || fieldType.IsPrimitive)
                throw new SerializerException("Unsupported type {0}", fieldType);
        }

        public SubFieldInfo this[string memberInfoName]
        {
            get
            {
                PopulateFieldsImpl(Mode.Auto);
                return Fields.FirstOrDefault(i => i.MemberInfo.Name == memberInfoName);
            }
            set
            {
                ThrowOnInitialized();
                for (int i = 0; i < Fields.Count; i++)
                {
                    if (Fields[i].MemberInfo.Name == memberInfoName)
                    {
                        if (value == null)
                            Fields.RemoveAt(i);
                        else
                            Fields[i] = value;
                        return;
                    }
                }

                if (value != null)
                    Fields.Add(value);
            }
        }

        public override int MaxByteSize
        {
            get { return _fields == null ? 0 : _fields.Sum(fld => fld.Field.MaxByteSize); }
        }

        public IList<SubFieldInfo> Fields
        {
            get { return _fields; }
            set
            {
                ThrowOnInitialized();
                _fields = value == null ? null : value.ToList();
            }
        }

        public ConstructorInfo Constructor
        {
            get { return _constructor; }
            set
            {
                ThrowOnInitialized();
                _constructor = value;
            }
        }

        public void PopulateFields(Mode mode = Mode.Auto)
        {
            // User should not call this method after initialization
            ThrowOnInitialized();
            PopulateFieldsImpl(mode);
        }

        private void PopulateFieldsImpl(Mode mode)
        {
            if (_fields != null)
                return;

            ThrowOnInitialized();

            if (mode == Mode.Auto)
            {
                mode = 0;
                if (_constructor != null)
                    mode |= Mode.Constructor;
                else if (FieldType.GetFields(TypeUtils.AllInstanceMembers).Any(fi => fi.IsInitOnly))
                    mode |= Mode.NonPublic | Mode.Constructor;
                else
                    mode |= Mode.NonPublic | Mode.Fields;
            }

            IEnumerable<MemberInfo> members;

            if ((mode & Mode.Fields) == 0 && (mode & Mode.Properties) == 0)
                mode |= Mode.Fields | Mode.Properties;

            BindingFlags flags = BindingFlags.Instance | BindingFlags.Public;
            if ((mode & Mode.NonPublic) != 0)
                flags |= BindingFlags.NonPublic;

            if ((mode & Mode.Constructor) != 0)
            {
                if (_constructor == null)
                    _constructor = ChooseCtor();

                members = _constructor.GetParameters()
                    .Select(i => FindMatchingMember(i, flags, mode));
            }
            else
            {
                members = (mode & Mode.Fields) != 0
                              ? FieldType.GetFields(flags).Where(i => !i.IsInitOnly)
                              : Enumerable.Empty<MemberInfo>();

                if ((mode & Mode.Properties) != 0)
                    members = members.Concat(FieldType.GetProperties(flags).Where(i => i.CanWrite));
            }

            IList<SubFieldInfo> fields = new List<SubFieldInfo>();
            foreach (MemberInfo mi in members)
            {
                bool isField = mi.MemberType == MemberTypes.Field;
                var mType = mi.PropOrFieldType();

                string name = StateName + "." + mi.Name;

                if (isField && mType.IsNested)
                {
                    object[] ca = mi.GetCustomAttributes(typeof (FixedBufferAttribute), false);
                    if (ca.Length > 0)
                    {
                        // ((FixedBufferAttribute)ca[0]).Length;
                        throw new NotImplementedException("Fixed arrays are not supported at this time");
                    }
                }

                BaseField fld = StateStore.CreateField(mType, name, true);
                fields.Add(new SubFieldInfo(mi, fld));
            }

            _fields = fields;
        }

        protected override void InitNewField(BinaryWriter writer)
        {
            base.InitNewField(writer);

            writer.Write(_fields.Count);
            foreach (SubFieldInfo field in _fields)
                field.InitNew(writer);
            writer.Write(_constructor != null);
        }

        protected override void InitExistingField(BinaryReader reader, Func<string, Type> typeResolver)
        {
            base.InitExistingField(reader, typeResolver);

            var fields = new SubFieldInfo[reader.ReadInt32()];
            for (int i = 0; i < fields.Length; i++)
                fields[i] = new SubFieldInfo(StateStore, reader, typeResolver);
            _fields = fields;
            if (Version >= Versions.Ver1)
                if (reader.ReadBoolean())
                {
                    var flds = fields.Select(i => i.MemberInfo.PropOrFieldType()).ToList();

                    var ctor = FieldType.GetConstructors(TypeUtils.AllInstanceMembers)
                        .SingleOrDefault(ci => ci.GetParameters().Select(i => i.ParameterType).SequenceEqual(flds));

                    if (ctor == null)
                        throw new SerializerException(
                            "Unable to find constructor {0}({1})", FieldType.FullName,
                            string.Join(", ", flds.Select(i => i.ToDebugStr())));

                    _constructor = ctor;
                }
        }

        protected override bool IsValidVersion(Version ver)
        {
            return ver == Versions.Ver0 || ver == Versions.Ver1;
        }

        protected override void MakeReadonly()
        {
            // In case the subFields have not yet been populated, do it now
            PopulateFieldsImpl(Mode.Auto);
            _fields = new ReadOnlyCollection<SubFieldInfo>(Fields);
            base.MakeReadonly();
        }

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
        {
            // result = writeDelta1() && writeDelta2() && ...
            var initExp = new List<Expression>();

            Expression nextExp = null;
            foreach (SubFieldInfo member in _fields)
            {
                Tuple<Expression, Expression> t = member.Field.GetSerializer(
                    Expression.MakeMemberAccess(valueExp, member.MemberInfo), codec);

                initExp.Add(t.Item1);

                Expression exp = Expression.IsTrue(t.Item2);
                nextExp = nextExp == null ? exp : Expression.And(nextExp, exp);
            }

            return
                new Tuple<Expression, Expression>(
                    Expression.Block(initExp),
                    nextExp ?? Expression.Constant(true));
        }

        protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
        {
            return _constructor != null
                       ? GetConstructorDeSerializerExp(codec)
                       : GetFieldAssignDeSerializerExp(codec);
        }

        private Tuple<Expression, Expression> GetConstructorDeSerializerExp(Expression codec)
        {
            var readAllInit = new List<Expression>();
            var readAllNext = new List<Expression>();

            foreach (SubFieldInfo member in Fields)
            {
                Tuple<Expression, Expression> srl = member.Field.GetDeSerializer(codec);
                readAllInit.Add(srl.Item1);
                readAllNext.Add(srl.Item2);
            }

            return new Tuple<Expression, Expression>(
                Expression.New(_constructor, readAllInit),
                Expression.New(_constructor, readAllNext));
        }

        private Tuple<Expression, Expression> GetFieldAssignDeSerializerExp(Expression codec)
        {
            // T current;
            ParameterExpression currentVar = Expression.Variable(FieldType, "current");

            // (class)  T current = (T) FormatterServices.GetUninitializedObject(typeof(T));
            // (struct) T current = default(T);
            BinaryExpression assignNewT = Expression.Assign(
                currentVar,
                FieldType.IsValueType
                    ? (Expression) Expression.Default(FieldType)
                    : Expression.Convert(
                        Expression.Call(
                            typeof (FormatterServices), "GetUninitializedObject", null,
                            Expression.Constant(FieldType)), FieldType));

            var readAllInit = new List<Expression> {assignNewT};
            var readAllNext = new List<Expression> {assignNewT};

            foreach (SubFieldInfo member in Fields)
            {
                Tuple<Expression, Expression> srl = member.Field.GetDeSerializer(codec);

                Expression field = Expression.MakeMemberAccess(currentVar, member.MemberInfo);
                readAllInit.Add(Expression.Assign(field, srl.Item1));
                readAllNext.Add(Expression.Assign(field, srl.Item2));
            }

            // newly created struct/class will be the result of both expressions
            readAllInit.Add(currentVar);
            readAllNext.Add(currentVar);

            return new Tuple<Expression, Expression>(
                Expression.Block(new[] {currentVar}, readAllInit),
                Expression.Block(new[] {currentVar}, readAllNext));
        }

        protected override bool Equals(BaseField baseOther)
        {
            var otherFld = ((ComplexField) baseOther)._fields;
            return ReferenceEquals(_fields, otherFld) ||
                   (_fields != null && otherFld != null && _fields.SequenceEqual(otherFld));
        }

        public override int GetHashCode()
        {
            unchecked
            {
                // ReSharper disable NonReadonlyFieldInGetHashCode
                var hashCode = base.GetHashCode();
                if (_fields != null)
                    foreach (var f in _fields)
                        hashCode = (hashCode*397) ^ f.GetHashCode();
                return hashCode;
                // ReSharper restore NonReadonlyFieldInGetHashCode
            }
        }

        #region Field Detection

        private ConstructorInfo ChooseCtor()
        {
            var ctors = FieldType.GetConstructors(TypeUtils.AllInstanceMembers);

            ConstructorInfo foundAttr = null;
            ConstructorInfo foundPubl = null;
            ConstructorInfo foundPriv = null;
            bool failedPubl = false, failedPriv = false;

            foreach (var ci in ctors)
            {
                var attr = ci.GetSingleAttribute<CtorFieldAttribute>();
                if (attr != null)
                {
                    if (foundAttr != null)
                        throw new SerializerException(
                            "More than one constructor has [{0}] on type {1}",
                            typeof (CtorFieldAttribute).Name, FieldType.ToDebugStr());

                    foundAttr = ci;
                }
                else if (ci.GetParameters().Length > 0)
                {
                    if (ci.IsPublic)
                    {
                        if (foundPubl != null)
                            failedPubl = true;
                        else
                            foundPubl = ci;
                    }
                    else
                    {
                        if (foundPriv != null)
                            failedPriv = true;
                        else
                            foundPriv = ci;
                    }
                }
            }

            if (foundAttr != null)
                return foundAttr;

            if (failedPubl || (foundPubl == null && failedPriv))
            {
                throw new SerializerException(
                    "Unable to auto-choose constructor without [{0}], more than one {1}constructor with parameters is found in type {2}.",
                    typeof (CtorFieldAttribute).Name, failedPubl ? "public " : "", FieldType.ToDebugStr());
            }

            if (foundPubl != null)
                return foundPubl;

            if (foundPriv != null)
                return foundPriv;

            throw new SerializerException(
                "No constructor with parameters is found that can be used for deserialization in type {2}.",
                FieldType.ToDebugStr());
        }

        private MemberInfo FindMatchingMember(
            ParameterInfo par, BindingFlags flags, Mode mode)
        {
            var attr = par.GetSingleAttribute<CtorFieldMapToAttribute>();
            bool isExact = attr != null;
            var name = isExact ? attr.FieldOrPropertyName : par.Name;
            IEnumerable<string> names;

            var inclFields = (mode & Mode.Fields) != 0;
            var inclProperties = (mode & Mode.Properties) != 0;

            if (!isExact)
            {
                var nrmName = name;

                if (inclFields)
                {
                    if (nrmName.StartsWith("m_", StringComparison.OrdinalIgnoreCase))
                        nrmName = nrmName.Substring(2);
                    else if (nrmName.StartsWith("_", StringComparison.Ordinal))
                        nrmName = nrmName.Substring(1);
                }

                bool empty = nrmName.Length == 0;
                if (!empty && char.IsUpper(nrmName, 0))
                    nrmName = char.ToLowerInvariant(nrmName[0]) + nrmName.Substring(1);

                var nms = new List<string>();

                if (inclFields)
                {
                    nms.Add("_" + nrmName);
                    nms.Add("m_" + nrmName);
                }

                if (!empty)
                {
                    nms.Add(nrmName);
                    nrmName = char.ToUpperInvariant(nrmName[0]) + nrmName.Substring(1);
                    nms.Add(nrmName);

                    if (inclFields)
                    {
                        nms.Add("_" + nrmName);
                        nms.Add("m_" + nrmName);
                    }
                }

                names = nms;
            }
            else
            {
                names = new[] {name};
            }

            MemberInfo result = null;

            if (inclProperties)
                foreach (var n in names)
                    FindMatchingMember(ref result, par, false, n, names, flags);
            if (result != null)
                return result;

            if (inclFields)
                foreach (var n in names)
                    FindMatchingMember(ref result, par, true, n, names, flags);
            if (result != null)
                return result;

            throw new SerializerException(
                "Constructor {0}.{1}(...{2} {3}...) does not map to {4} {5} {6}{7} with type {8}",
                FieldType.FullName, FieldType.Name,
                par.ParameterType, par.Name,
                (flags & BindingFlags.NonPublic) != 0 ? "public and private" : "public",
                inclProperties && inclFields ? "fields or properties" : (inclFields ? "fields" : "properties"),
                isExact ? "" : "similar to ", name, par.ParameterType.ToDebugStr());
        }

        private void FindMatchingMember(
            ref MemberInfo result, ParameterInfo par, bool inclFields, string name,
            IEnumerable<string> names, BindingFlags flags)
        {
            var results = FieldType.GetMember(
                name, inclFields ? MemberTypes.Field : MemberTypes.Property, flags);

            MemberInfo found = null;
            foreach (var mi in results)
            {
                var t = mi.PropOrFieldType();
                if (t != par.ParameterType)
                    continue;
                if (found != null)
                    throw new SerializerException(
                        "[{0}] {1}.{2}(...{3} {4}...) maps to more than one {5} {6}",
                        typeof (CtorFieldAttribute).Name, FieldType.FullName, FieldType.Name,
                        par.ParameterType, par.Name, inclFields ? "field" : "property", name);
                found = mi;
            }

            if (found == null)
                return;

            if (result != null)
                throw new SerializerException(
                    "[{0}] {1}.{2}(...{3} {4}...) could map to more than one {5} like {6}",
                    typeof (CtorFieldAttribute).Name, FieldType.FullName, FieldType.Name,
                    par.ParameterType, par.Name, inclFields ? "field" : "property",
                    names == null ? name : string.Join(",", names));

            result = found;
        }

        #endregion
    }
}