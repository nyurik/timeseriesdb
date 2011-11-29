using System.Reflection;

namespace NYurik.FastBinTimeseries.Serializers.BlockSerializer
{
    public class SubFieldInfo
    {
        public SubFieldInfo(PropertyInfo property, BaseField field)
            : this((MemberInfo) property, field)
        {
        }

        public SubFieldInfo(FieldInfo property, BaseField field)
            : this((MemberInfo) property, field)
        {
        }

        private SubFieldInfo(MemberInfo member, BaseField field)
        {
            MemberInfo = member;
            Field = field;
        }

        public MemberInfo MemberInfo { get; private set; }

        public BaseField Field { get; private set; }

        public void Validate()
        {
            Field.Validate();
        }

        public override string ToString()
        {
            return string.Format("{0} {1}", MemberInfo.Name, Field);
        }
    }
}