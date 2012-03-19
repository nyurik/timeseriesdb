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
using System.Runtime.InteropServices;
using JetBrains.Annotations;
using NUnit.Framework;
using NYurik.TimeSeriesDb.Serializers;

namespace NYurik.TimeSeriesDb.Test
{
#pragma warning disable 169, 649
    // ReSharper disable UnusedTypeParameter

    [TestFixture]
    public class SerializerErrorsTests : TestsBase
    {
        [UsedImplicitly]
        private class RefType
        {
        }

        [UsedImplicitly]
        private struct GenType<T>
        {
            #region Nested type: GenSubType

            public struct GenSubType
            {
            }

            #endregion
        }

        private struct ValTypeWithRef
        {
            public RefType Val;
        }

        private struct ValTypeWithGen
        {
            public GenType<int> Val;
        }

        private struct Struct0
        {
            private readonly byte _a;
            private readonly long _b;
            private readonly int _c;
        }

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        private struct Struct1
        {
            private readonly byte _a;
            private readonly long _b;
            private readonly int _c;
        }

        [StructLayout(LayoutKind.Sequential, Pack = 2)]
        private struct Struct2
        {
            private readonly byte _a;
            private readonly long _b;
            private readonly int _c;
        }

        [StructLayout(LayoutKind.Sequential, Pack = 4)]
        private struct Struct4
        {
            private readonly byte _a;
            private readonly long _b;
            private readonly int _c;
        }

        [StructLayout(LayoutKind.Sequential, Pack = 8)]
        private struct Struct8
        {
            private readonly byte _a;
            private readonly long _b;
            private readonly int _c;
        }

        [Test]
        public void CreateSerializer()
        {
            Assert.Throws<SerializerException>(
                () => DynamicCodeFactory.Instance.Value.CreateSerializer<DateTime>());

            Assert.Throws<SerializerException>(
                () => DynamicCodeFactory.Instance.Value.CreateSerializer<RefType>());

            Assert.Throws<SerializerException>(
                () => DynamicCodeFactory.Instance.Value.CreateSerializer<GenType<int>>());

            Assert.Throws<SerializerException>(
                () => DynamicCodeFactory.Instance.Value.CreateSerializer<GenType<int>.GenSubType>());

            Assert.Throws<SerializerException>(
                () => DynamicCodeFactory.Instance.Value.CreateSerializer<ValTypeWithRef>());

            Assert.Throws<SerializerException>(
                () => DynamicCodeFactory.Instance.Value.CreateSerializer<ValTypeWithGen>());
        }

        [Test]
        public void SerializerPacked()
        {
            Console.WriteLine("Running in {0} bit process", Environment.Is64BitProcess ? "64" : "32");
            Assert.AreEqual(24, DynamicCodeFactory.Instance.Value.CreateSerializer<Struct0>().TypeSize, "#0");
            Assert.AreEqual(13, DynamicCodeFactory.Instance.Value.CreateSerializer<Struct1>().TypeSize, "#1");
            Assert.AreEqual(14, DynamicCodeFactory.Instance.Value.CreateSerializer<Struct2>().TypeSize, "#2");
            Assert.AreEqual(16, DynamicCodeFactory.Instance.Value.CreateSerializer<Struct4>().TypeSize, "#4");
            Assert.AreEqual(24, DynamicCodeFactory.Instance.Value.CreateSerializer<Struct8>().TypeSize, "#8");
        }
    }
}