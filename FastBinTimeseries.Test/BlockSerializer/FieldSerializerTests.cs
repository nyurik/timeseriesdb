using System;
using System.Linq.Expressions;
using System.Reflection;
using NUnit.Framework;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test.BlockSerializer
{
    internal class FieldSerializer
    {
        [Test]
        public void Expressions()
        {
            var v = new Strct();
            var fld = v.GetType().GetField("_c", TypeExtensions.AllInstanceMembers);
            var srl = new FloatSerializer(0, fld, 10);
            var x = srl.GetSerializerExpr();
            var init = x.Item1;
            var delta = x.Item2;

            var mergedBlock1 = Expression.Block(init, delta);
            var merged = Expression.Lambda<Strct>(mergedBlock1);
        }

        [Test, Ignore]
        public void Serialize()
        {
            var data = new Strct[100];
            var buffer = new byte[50];

            var sr = new FieldSerializer<Strct>();

//            int count = sr.Serialize(data, 0, buffer);
//            count = sr.Deserialize(data, 0, buffer);


        
        }

        #region Nested type: Strct

        private struct Strct
        {
            [Field] private int _a;
            [Field] private double _b;
            [Field] private float _c;
            [Field(typeof (UtcDateTimeSerializer))] private UtcDateTime _timestamp;

            #region Nested type: UtcDateTimeSerializer

            private class UtcDateTimeSerializer : TypeSerializer
            {
                public UtcDateTimeSerializer(byte index, FieldInfo field)
                    : base(index, field)
                {
                }

                protected internal override Tuple<Expression, Expression> GetSerializerExpr()
                {
                    throw new NotImplementedException();
//                    return Expression.Call(
//                        FldSerializerExp,
//                        FldSerializerExp.Type.GetMethod("WriteInt64"),
//                        IndexExp,
//                        Expression.PropertyOrField(FieldExp, "Ticks"));
                }

                protected override Expression GetDeSerializerExpr()
                {
                    return Expression.New(
                        typeof (UtcDateTime).GetConstructor(new[] {typeof (long)}),
                        Expression.Call(
                            FldSerializerExp, FldSerializerExp.Type.GetMethod("ReadInt64"), IndexExp));
                }
            }

            #endregion
        }

        #endregion
    }
}