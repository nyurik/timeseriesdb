using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using NUnit.Framework;
using NYurik.EmitExtensions;
using NYurik.FastBinTimeseries.CommonCode;
using NYurik.FastBinTimeseries.Serializers;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test.BlockSerializer
{
    internal class FieldSerializer
    {
        [Test]
        public void Expressions()
        {
            FieldInfo fldA = typeof (Strct).GetField("_a", TypeExtensions.AllInstanceMembers);
            var srlA = new IntSerializer(fldA);

            FieldInfo fldC = typeof (Strct).GetField("_c", TypeExtensions.AllInstanceMembers);
            var srlC = new FloatSerializer(fldC, 10);


            Func<StreamCodec, IEnumerator<Strct>, bool> serialize = DynamicSerializer<Strct>.GenerateSerializer(new[] { srlC, srlC });

            var codec = new StreamCodec(1000);

            var v = new Strct[10];
            v[0] = new Strct(12);
            v[1] = new Strct(13);

            var enmr = ((IEnumerable<Strct>) v).GetEnumerator();
            if (enmr.MoveNext())
            {
                bool res = serialize(codec, enmr);
                Console.WriteLine(res);
            }
        }

        #region Nested type: Strct

        private struct Strct
        {
            [Field] private int _a;
            [Field] private double _b;
            [Field] private float _c;
            [Field(typeof (UtcDateTimeSerializer))] private UtcDateTime _timestamp;

            public Strct(int i)
            {
                _timestamp = new UtcDateTime(i);
                _a = i + 1;
                _b = i + 2;
                _c = i/10.0f;
            }

            #region Nested type: UtcDateTimeSerializer

            private class UtcDateTimeSerializer : TypeSerializer
            {
                public UtcDateTimeSerializer(FieldInfo field)
                    : base(field)
                {
                }

                protected internal override Expression GetSerializerExp(Expression valueT, ParameterExpression codec,
                                                                        List<ParameterExpression> stateVariables,
                                                                        List<Expression> initBlock)
                {
                    throw new NotImplementedException();
                    //                    return Expression.Call(
                    //                        FldSerializerExp,
                    //                        FldSerializerExp.Type.GetMethod("WriteInt64"),
                    //                        IndexExp,
                    //                        Expression.PropertyOrField(FieldExp, "Ticks"));
                }

                protected override Expression GetDeSerializerExp()
                {
                    throw new NotImplementedException();
//                    return Expression.New(
//                        typeof (UtcDateTime).GetConstructor(new[] {typeof (long)}),
//                        Expression.Call(
//                            FldSerializerExp, FldSerializerExp.Type.GetMethod("ReadInt64"), IndexExp));
                }
            }

            #endregion
        }

        #endregion
    }
}