using System.Linq.Expressions;
using System.Reflection;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class MemberSerializerInfo
    {
        public MemberSerializerInfo(PropertyInfo property, BaseSerializer serializer)
            : this((MemberInfo) property, serializer)
        {
        }

        public MemberSerializerInfo(FieldInfo property, BaseSerializer serializer)
            : this((MemberInfo) property, serializer)
        {
        }

        private MemberSerializerInfo(MemberInfo member, BaseSerializer serializer)
        {
            MemberInfo = member;
            Serializer = serializer;
        }

        public bool IsProperty
        {
            get { return MemberInfo is PropertyInfo; }
        }

        public MemberInfo MemberInfo { get; private set; }
        public BaseSerializer Serializer { get; private set; }

        public virtual Expression GetterFactory(Expression valueExp)
        {
            return IsProperty
                       ? Expression.Property(valueExp, (PropertyInfo) MemberInfo)
                       : Expression.Field(valueExp, (FieldInfo) MemberInfo);
        }

        public void Validate()
        {
            Serializer.Validate();
        }
    }
}