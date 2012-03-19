#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 *     This file is part of FastBinTimeseries library
 * 
 *  FastBinTimeseries is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  FastBinTimeseries is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with FastBinTimeseries.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#endregion

using System;
using JetBrains.Annotations;
using NUnit.Framework;
using NYurik.FastBinTimeseries.Serializers;

namespace NYurik.FastBinTimeseries.Test
{
    [TestFixture]
    public class SerializerErrorsTests : TestsBase
    {
        // ReSharper disable UnusedTypeParameter
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

#pragma warning disable 649
        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        private struct ValTypeWithRef
        {
            public RefType Val;
        }

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        private struct ValTypeWithGen
        {
            public GenType<int> Val;
        }
#pragma warning restore 649

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
    }
}