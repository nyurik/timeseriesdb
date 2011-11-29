using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using JetBrains.Annotations;
using NYurik.EmitExtensions;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class ComplexField : BaseField
    {
        private IList<SubFieldInfo> _fields;

        public ComplexField([NotNull] IStateStore stateStore, [NotNull] Type valueType, string stateName)
            : base(stateStore, valueType, stateName)
        {
            if (valueType.IsArray || valueType.IsPrimitive)
                throw new SerializerException("Unsupported type {0}", valueType);

            FieldInfo[] fis = valueType.GetFields(TypeExtensions.AllInstanceMembers);
            _fields = new List<SubFieldInfo>(fis.Length);
            foreach (FieldInfo fi in fis)
            {
                _fields.Add(
                    new SubFieldInfo(
                        fi,
                        stateStore.GetDefaultField(fi.FieldType, stateName + "." + fi.Name)));
            }
        }

        public SubFieldInfo this[string memberInfoName]
        {
            get { return Fields.FirstOrDefault(i => i.MemberInfo.Name == memberInfoName); }
        }

        public IList<SubFieldInfo> Fields
        {
            get { return _fields; }
            set
            {
                ThrowOnInitialized();
                _fields = value.ToList();
            }
        }

        public override void Validate()
        {
            ThrowOnInitialized();
            foreach (SubFieldInfo ms in _fields)
                ms.Validate();
            _fields = new ReadOnlyCollection<SubFieldInfo>(_fields);
            base.Validate();
        }

        protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
        {
            ThrowOnNotInitialized();

            // result = writeDelta1() && writeDelta2() && ...
            var initExp = new List<Expression>();

            Expression nextExp = null;
            foreach (SubFieldInfo member in _fields)
            {
                Tuple<Expression, Expression> t = member.Field.GetSerializer(
                    GetterFactory(member.MemberInfo, valueExp), codec);

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
            ThrowOnNotInitialized();

            // T current;
            ParameterExpression currentVar = Expression.Variable(ValueType, "current");

            // (class)  T current = FormatterServices.GetUninitializedObject(typeof(T));
            // (struct) T current = default(T);
            BinaryExpression assignNewT = Expression.Assign(
                currentVar,
                ValueType.IsValueType
                    ? (Expression) Expression.Default(ValueType)
                    : Expression.Call(
                        typeof (FormatterServices), "GetUninitializedObject", null,
                        Expression.Constant(ValueType)));

            var readAllInit = new List<Expression> {assignNewT};
            var readAllNext = new List<Expression> {assignNewT};

            foreach (SubFieldInfo member in _fields)
            {
                Tuple<Expression, Expression> srl = member.Field.GetDeSerializer(codec);

                Expression field = GetterFactory(member.MemberInfo, currentVar);
                readAllInit.Add(Expression.Assign(field, srl.Item1));
                readAllNext.Add(Expression.Assign(field, srl.Item2));
            }

            readAllInit.Add(currentVar);
            readAllNext.Add(currentVar);

            return new Tuple<Expression, Expression>(
                Expression.Block(new[] {currentVar}, readAllInit),
                Expression.Block(new[] {currentVar}, readAllNext));
        }

        private static Expression GetterFactory(MemberInfo memberInfo, Expression valueExp)
        {
            return memberInfo is PropertyInfo
                       ? Expression.Property(valueExp, (PropertyInfo) memberInfo)
                       : Expression.Field(valueExp, (FieldInfo) memberInfo);
        }
    }
}