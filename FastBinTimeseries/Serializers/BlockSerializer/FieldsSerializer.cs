using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using NYurik.EmitExtensions;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class FieldsSerializer : BaseSerializer
    {
        private IList<MemberSerializerInfo> _memberSerializers;

        public FieldsSerializer([NotNull] Type valueType)
            : base(valueType)
        {
            FieldInfo[] fis = valueType.GetFields(TypeExtensions.AllInstanceMembers);
            _memberSerializers = new List<MemberSerializerInfo>(fis.Length);

            foreach (FieldInfo fi in fis)
            {
                Type fieldType = fi.FieldType;
                BaseSerializer srl;

                if (fieldType.IsPrimitive)
                {
                    switch (Type.GetTypeCode(fieldType))
                    {
                        case TypeCode.Char:
                        case TypeCode.SByte:
                        case TypeCode.Byte:
                        case TypeCode.Int16:
                        case TypeCode.UInt16:
                        case TypeCode.Int32:
                        case TypeCode.UInt32:
                        case TypeCode.Int64:
                        case TypeCode.UInt64:
                        case TypeCode.Single:
                        case TypeCode.Double:
                        case TypeCode.Decimal:
                            srl = new DeltaWithMultiplierSerializer(fieldType, fi.Name);
                            break;

                        default:
                            throw new SerializerException("Unsupported field type {0}", fieldType);
                    }
                }
                else
                {
                    srl = new FieldsSerializer(fieldType);
                }

                _memberSerializers.Add(new MemberSerializerInfo(fi, srl));
            }
        }

        public IList<MemberSerializerInfo> MemberSerializers
        {
            get { return _memberSerializers; }
            set
            {
                ThrowOnInitialized();
                _memberSerializers = value.ToList();
            }
        }

        public override void Validate()
        {
            foreach (MemberSerializerInfo ms in _memberSerializers)
                ms.Validate();
            _memberSerializers = new ReadOnlyCollection<MemberSerializerInfo>(_memberSerializers);
            base.Validate();
        }

        protected override Expression GetSerializerExp(Expression valueExp, Expression codec,
                                                       List<ParameterExpression> stateVariables,
                                                       List<Expression> initBlock)
        {
            ThrowOnNotInitialized();

            // result = writeDelta1() && writeDelta2() && ...

            Expression result = null;
            foreach (MemberSerializerInfo member in _memberSerializers)
            {
                Expression t = member.Serializer.GetSerializer(
                    member.GetterFactory(valueExp), codec, stateVariables, initBlock);

                Expression exp = Expression.IsTrue(t);
                result = result == null ? exp : Expression.And(result, exp);
            }

            return result ?? Expression.Constant(true);
        }

        protected override void GetDeSerializerExp(Expression valueExp, Expression codec,
                                                   List<ParameterExpression> stateVariables,
                                                   List<Expression> initBlock, List<Expression> deltaBlock)
        {
            ThrowOnNotInitialized();
            foreach (MemberSerializerInfo member in _memberSerializers)
                member.Serializer.GetDeSerializer(
                    member.SetterFactory(valueExp), codec, stateVariables, initBlock, deltaBlock);
        }
    }
}