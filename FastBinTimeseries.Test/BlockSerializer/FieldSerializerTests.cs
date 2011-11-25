using System;
using System.Collections.Generic;
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
//            FieldInfo fldA = typeof (Strct).GetField("_a", TypeExtensions.AllInstanceMembers);
//            var srlA = new DeltaWithMultiplierSerializer(fldA.FieldType, fldA.Name);

            FieldInfo fldC = typeof (Strct).GetField("_c", TypeExtensions.AllInstanceMembers);
            var srlC = new DeltaWithMultiplierSerializer(fldC.FieldType, fldC.Name, 10);

            var srlT = new FieldsSerializer(typeof (Strct));
            srlT.MemberSerializers.Clear();
            srlT.MemberSerializers.Add(new MemberSerializerInfo(fldC, srlC));

            Func<StreamCodec, IEnumerator<Strct>, bool> serialize =
                DynamicSerializer<Strct>.GenerateSerializer(srlT);

            Action<StreamCodec, Buff<Strct>, int> deserialize =
                DynamicSerializer<Strct>.GenerateDeSerializer(srlT);

            var codec = new StreamCodec(1000);

            var v = new[]
                        {
                            new Strct(12),
                            new Strct(13),
                        };

            IEnumerator<Strct> enmr = ((IEnumerable<Strct>) v).GetEnumerator();
            if (enmr.MoveNext())
            {
                bool res = serialize(codec, enmr);
                Console.WriteLine(res);
            }

            var outBuff = new Buff<Strct>();
            codec.BufferPos = 0;
            deserialize(codec, outBuff, 10);
        }

        #region Nested type: Strct

        private struct Strct
        {
            [Field] private int _a;
            [Field] private double _b;
            [Field] private float _c;
            [Field] private UtcDateTime _timestamp;

            public Strct(int i)
            {
                _timestamp = new UtcDateTime(i);
                _a = i + 1;
                _b = i + 2;
                _c = i/10.0f;
            }
        }

        #endregion
    }
}