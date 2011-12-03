using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;
using NYurik.FastBinTimeseries.Serializers.BlockSerializer;

namespace NYurik.FastBinTimeseries.Test.BlockSerializer
{
    [TestFixture]
    public class StreamCodecTests : TestsBase
    {
        private const int BufferSize = 10000000;

        public static IEnumerable<ulong> TestValuesGenerator(bool sixBits = true, bool sevenBits = true,
                                                             bool eightBits = true)
        {
            var vals6Bit = new byte[] {0x00, 0x01, 0x20, 0x21, 0x3F};
            var vals6BitHigh = new byte[] {0x00, 0x01, 0x08, 0x09, 0x0F}; // like a 4-bit value
            var vals7Bit = new byte[] {0x00, 0x01, 0x40, 0x41, 0x7F};
            var vals7BitHigh = new byte[] {0x00, 0x01}; // like a 1-bit value
            var valsByte = new byte[] {0x00, 0x01, 0x80, 0x81, 0xFF};

            foreach (byte v in vals6Bit)
                Assert.Less(v, 64);
            foreach (byte v in vals7Bit)
                Assert.Less(v, 128);
            if (sixBits)
                foreach (byte val10 in vals6BitHigh)
                    foreach (byte val9 in vals6Bit)
                        foreach (byte val8 in vals6Bit)
                            foreach (byte val7 in vals6Bit)
                                foreach (byte val6 in vals6Bit)
                                    foreach (byte val5 in vals6Bit)
                                        foreach (byte val4 in vals6Bit)
                                            foreach (byte val3 in vals6Bit)
                                                foreach (byte val2 in vals6Bit)
                                                    foreach (byte val1 in vals6Bit)
                                                        foreach (byte val0 in vals6Bit)
                                                        {
                                                            ulong value = (ulong) val10 << 6*10 | (ulong) val9 << 6*9
                                                                          | (ulong) val8 << 6*8 | (ulong) val7 << 6*7
                                                                          | (ulong) val6 << 6*6 | (ulong) val5 << 6*5
                                                                          | (ulong) val4 << 6*4 | (ulong) val3 << 6*3
                                                                          | (ulong) val2 << 6*2 | (ulong) val1 << 6*1
                                                                          | (ulong) val0 << 6*0;
                                                            yield return value;
                                                        }

            if (sevenBits)
                foreach (byte val9 in vals7BitHigh)
                    foreach (byte val8 in vals7Bit)
                        foreach (byte val7 in vals7Bit)
                            foreach (byte val6 in vals7Bit)
                                foreach (byte val5 in vals7Bit)
                                    foreach (byte val4 in vals7Bit)
                                        foreach (byte val3 in vals7Bit)
                                            foreach (byte val2 in vals7Bit)
                                                foreach (byte val1 in vals7Bit)
                                                    foreach (byte val0 in vals7Bit)
                                                    {
                                                        ulong value = (ulong) val9 << 7*9 | (ulong) val8 << 7*8
                                                                      | (ulong) val7 << 7*7 | (ulong) val6 << 7*6
                                                                      | (ulong) val5 << 7*5 | (ulong) val4 << 7*4
                                                                      | (ulong) val3 << 7*3 | (ulong) val2 << 7*2
                                                                      | (ulong) val1 << 7*1 | (ulong) val0 << 7*0;
                                                        yield return value;
                                                    }

            if (eightBits)
                foreach (byte val7 in valsByte)
                    foreach (byte val6 in valsByte)
                        foreach (byte val5 in valsByte)
                            foreach (byte val4 in valsByte)
                                foreach (byte val3 in valsByte)
                                    foreach (byte val2 in valsByte)
                                        foreach (byte val1 in valsByte)
                                            foreach (byte val0 in valsByte)
                                            {
                                                ulong value = (ulong) val7 << 8*7 | (ulong) val6 << 8*6
                                                              | (ulong) val5 << 8*5 | (ulong) val4 << 8*4
                                                              | (ulong) val3 << 8*3 | (ulong) val2 << 8*2
                                                              | (ulong) val1 << 8*1 | (ulong) val0 << 8*0;
                                                yield return value;
                                            }
        }

        /// <summary>
        /// Batch sequential values as an IEnumerable of arrays (same array is resued for each yield, do not cache).
        /// </summary>
        public static IEnumerable<T[]> BatchGroup<T>(IEnumerable<T> source, int groupSize = 1000)
        {
            T[] result = null;
            int ind = 0;

            foreach (T t in source)
            {
                if (result == null || ind == groupSize)
                {
                    if (result != null)
                    {
                        yield return result;
                        ind = 0;
                    }
                    result = new T[groupSize];
                }

                result[ind++] = t;
            }

            if (result != null && ind > 0)
            {
                var res2 = new T[ind];
                Array.Copy(result, res2, ind);
                yield return res2;
            }
        }

        [Test, Explicit("Performance testing")]
        public unsafe void CompareLoopAndSequenceUnsigned()
        {
            const int bufSize = BufferSize;
            const int valCount = bufSize/10;
            const int runs = 100;

            var codec = new CodecWriter(bufSize);

            ulong[] valList = TestValuesGenerator().Take(valCount).ToArray();
            foreach (ulong val in valList)
                codec.WriteUnsignedValue(val);

            // precache
            fixed (byte* buff2 = codec.Buffer)
            {
                int pos = 0;
                CodecReader.ReadSignedValueUnsafe(buff2, ref pos);
                CodecReader.ReadUnsignedValueUnsafe(buff2, ref pos);
            }

            Stopwatch sw = Stopwatch.StartNew();
            for (int i = 0; i < runs; i++)
            {
                fixed (byte* pbuff = codec.Buffer)
                {
                    int pos = 0;
                    for (int r = 0; r < valCount; r++)
                        CodecReader.ReadSignedValueUnsafe(pbuff, ref pos);
                }
            }
            Console.WriteLine("{0} ReadSignedValueUnsafe() time", sw.Elapsed);

            sw.Restart();
            for (int i = 0; i < runs; i++)
            {
                fixed (byte* pbuff = codec.Buffer)
                {
                    int pos = 0;
                    for (int r = 0; r < valCount; r++)
                        CodecReader.ReadUnsignedValueUnsafe(pbuff, ref pos);
                }
            }
            Console.WriteLine("{0} ReadUnsignedValueUnsafe() time", sw.Elapsed);
        }

        [Test]
        public unsafe void OneValueTest()
        {
            var codec = new CodecWriter(10);
            const long signedVal = unchecked((long) 0xFFFFFFFFFF000000UL);
            codec.BufferPos = 0;
            codec.WriteSignedValue(signedVal);

            fixed (byte* pbuf = codec.Buffer)
            {
                int pos = 0;
                long v = CodecReader.ReadSignedValueUnsafe(pbuf, ref pos);

                if (signedVal != v)
                    Assert.Fail("Failed signed long {0:X}", signedVal);
            }
        }

        [Test]
        public unsafe void SignedValues()
        {
            var codec = new CodecWriter(BufferSize);

            foreach (var valList in BatchGroup(TestValuesGenerator(), codec.BufferSize/10))
            {
                codec.BufferPos = 0;
                foreach (long val in valList)
                    codec.WriteSignedValue(val);

                fixed (byte* pbuf = codec.Buffer)
                {
                    int pos = 0;
                    foreach (long val in valList)
                        if (val != CodecReader.ReadSignedValueUnsafe(pbuf, ref pos))
                            Assert.Fail("Failed ulong {0:X}", val);
                    codec.BufferPos = pos;
                }
            }
        }

        [Test]
        public unsafe void UnsignedValues()
        {
            var codec = new CodecWriter(BufferSize);

            foreach (var valList in BatchGroup(TestValuesGenerator(), codec.BufferSize/10))
            {
                codec.BufferPos = 0;
                foreach (ulong val in valList)
                    codec.WriteUnsignedValue(val);

                fixed (byte* pbuf = codec.Buffer)
                {
                    int pos = 0;
                    foreach (ulong val in valList)
                        if (val != CodecReader.ReadUnsignedValueUnsafe(pbuf, ref pos))
                            Assert.Fail("Failed ulong {0:X}", val);
                    codec.BufferPos = pos;
                }
            }
        }
    }
}