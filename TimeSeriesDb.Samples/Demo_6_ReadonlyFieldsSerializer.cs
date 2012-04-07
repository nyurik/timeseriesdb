#region COPYRIGHT

/*
 *     Copyright 2009-2012 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

#endregion

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using NYurik.TimeSeriesDb.Serializers;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

// Do not disable these Resharper checks in your code. Demo purposes only.
// ReSharper disable PossibleMultipleEnumeration
// ReSharper disable InconsistentNaming

namespace NYurik.TimeSeriesDb.Samples
{
    /// <summary>
    /// This sample demonstrates how to configure a compressed file with calculated sequential index.
    /// </summary>
    internal class Demo_6_ReadonlyFieldsSerializer : ISample
    {
        #region ISample Members

        public void Run()
        {
            // Create filenames, deleting existing files if exist
            string filename = CreateFilename();

            // 
            // Set up sample data.
            // Note that we might get minor rounding errors when storing.
            // The Equals implementation of the ReadonlyItemLngDbl accounts for that.
            //
            const int itemCount = 10000;
            IEnumerable<ArraySegment<ReadonlyItemLngDbl>> data = Utils.GenerateData(
                0, itemCount, i => new ReadonlyItemLngDbl(i, (i/100.0)*65.0));


            // Create new BinCompressedSeriesFile file that stores a sequence of ReadonlyItemLngDbl structs
            // The file is indexed by a long value inside ReadonlyItemLngDbl marked with the [Index] attribute.
            // Here we provide a custom field factory that will analyze each field as it is being created,
            // and may choose to supply a custom field or null to use the default.
            // The name is the automatically generated, starting with the "root" for the TVal with each
            // subfield appended afterwards, separated by a dot.
            // Alternatively, ReadonlyItemLngDbl.SequenceNum can be marked with [Field(typeof(IncrementalIndex))]
            // For complex types, [Field] attribute can also be set on the type itself.
            using (var bf = new BinCompressedSeriesFile<long, ReadonlyItemLngDbl>(filename))
            {
                // When a new instance of BinCompressedSeriesFile is created,
                // RootField will be pre-populated with default configuration objects.
                // Some fields, such as doubles, require additional configuration before the file can be initialized.
                var root = (ReadonlyItemLngDblField) bf.RootField;

                // Index is always increasing
                var seq = (ScaledDeltaIntField) root.SequenceNumField;
                seq.DeltaType = DeltaType.Positive;

                // This double will contain values with no more than 2 digits after the decimal points.
                // Before serializing, multiply the value by 100 to convert to long.
                var val1 = (ScaledDeltaFloatField) root.ValueField;
                val1.Multiplier = 100;

                bf.UniqueIndexes = true; // enforce index uniqueness - each index is +1
                bf.InitializeNewFile(); // Finish new file initialization and create an empty file


                bf.AppendData(data);

                //
                // Verify that the created files are identical (use the default bitwise value type Equals)
                //
                if (!bf.Stream().SequenceEqual(data.Stream()))
                    throw new BinaryFileException("File does not have the right data");

                Console.WriteLine("File {0} created with {1,10:#,#} bytes", filename, bf.BaseStream.Length);
            }

            //
            // Check that the settings are stored ok in the file and can be re-initialized on open
            //
            using (var bf1 = (IWritableFeed<long, ReadonlyItemLngDbl>) BinaryFile.Open(filename))
            {
                if (!bf1.Stream().SequenceEqual(data.Stream()))
                    throw new BinaryFileException("File does not have the right data on the second check");
            }

            // cleanup
            CreateFilename();
        }

        #endregion

        private string CreateFilename()
        {
            string filename = GetType().Name + ".bts";
            if (File.Exists(filename)) File.Delete(filename);
            return filename;
        }

        #region Nested type: ReadonlyItemLngDbl

        /// <summary>
        /// This object is readonly, so at this time we are unable to automatically serialize it (might change in the future)
        /// Use [Field(type)] attribute to specify which field serializer to use
        /// </summary>
        [Field(typeof (ReadonlyItemLngDblField))]
        internal struct ReadonlyItemLngDbl
        {
            /// <summary> Ignore rounding errors up to this value </summary>
            private const double _valueEpsilon = 0.000001;

            [Index] public readonly long SequenceNum;
            public readonly double Value;

            public ReadonlyItemLngDbl(long sequenceNum, double value)
            {
                SequenceNum = sequenceNum;
                Value = value;
            }

            public override string ToString()
            {
                return string.Format("{0,3}: {1}", SequenceNum, Value);
            }

            public bool Equals(ReadonlyItemLngDbl other)
            {
                return SequenceNum == other.SequenceNum && Math.Abs(Value - other.Value) < _valueEpsilon;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (obj.GetType() != typeof (ReadonlyItemLngDbl)) return false;
                return Equals((ReadonlyItemLngDbl) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (SequenceNum.GetHashCode()*397) ^ Math.Round(Value/_valueEpsilon).GetHashCode();
                }
            }
        }

        #endregion

        #region Nested type: ReadonlyItemLngDblField

        private class ReadonlyItemLngDblField : BaseField
        {
            private static readonly Type FldType = typeof (ReadonlyItemLngDbl);
            private static readonly FieldInfo _sequenceFieldInfo = FldType.GetField("SequenceNum");
            private static readonly FieldInfo _valueFieldInfo = FldType.GetField("Value");
            private BaseField _sequenceNumField;
            private BaseField _valueField;

            // ReSharper disable UnusedMember.Local
            /// <summary>
            /// Used by reflection when an existing file is opened
            /// </summary>
            protected ReadonlyItemLngDblField()
            {
            }

            // ReSharper restore UnusedMember.Local

            /// <summary>
            /// Keep the parameters intact, the field creator will call it through reflection.
            /// </summary>
            /// <param name="stateStore">Serializer with the state</param>
            /// <param name="fieldType">Type of value to store</param>
            /// <param name="stateName">Name of the value (default state variable in the form "root.SubField.SubSubField...")</param>
// ReSharper disable UnusedMember.Local
            public ReadonlyItemLngDblField(IStateStore stateStore, Type fieldType, string stateName)
// ReSharper restore UnusedMember.Local
                : base(Versions.Ver0, stateStore, fieldType, stateName)
            {
                ValidateType(fieldType, stateName);

                _sequenceNumField = stateStore.CreateField(
                    _sequenceFieldInfo.FieldType, stateName + "." + _sequenceFieldInfo.Name, true);
                _valueField = stateStore.CreateField(
                    _valueFieldInfo.FieldType, stateName + "." + _valueFieldInfo.Name, true);
            }

            public override int MaxByteSize
            {
                get { return _sequenceNumField.MaxByteSize + _valueField.MaxByteSize; }
            }

            public BaseField SequenceNumField
            {
                get { return _sequenceNumField; }
            }

            public BaseField ValueField
            {
                get { return _valueField; }
            }

            protected override bool IsValidVersion(Version ver)
            {
                return ver == Versions.Ver0;
            }

            protected override void InitNewField(BinaryWriter writer)
            {
                base.InitNewField(writer);
                _sequenceNumField.InitNew(writer);
                _valueField.InitNew(writer);
            }

            protected override void InitExistingField(BinaryReader reader, Func<string, Type> typeResolver)
            {
                base.InitExistingField(reader, typeResolver);
                _sequenceNumField = FieldFromReader(StateStore, reader, typeResolver);
                _valueField = FieldFromReader(StateStore, reader, typeResolver);
            }

            protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
            {
                // Init:
                //   initSequenceNum();
                //   initValue();
                // Following:
                //   return writeSequenceNum() && writeValue();

                var seqSrl = _sequenceNumField.GetSerializer(
                    Expression.MakeMemberAccess(valueExp, _sequenceFieldInfo), codec);
                var valSrl = _valueField.GetSerializer(
                    Expression.MakeMemberAccess(valueExp, _valueFieldInfo), codec);

                return
                    new Tuple<Expression, Expression>(
                        Expression.Block(seqSrl.Item1, valSrl.Item1),
                        Expression.And(
                            Expression.IsTrue(seqSrl.Item2),
                            Expression.IsTrue(valSrl.Item2)));
            }

            protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
            {
                // return new ReadonlyItemLngDbl(seq, val);

                var seqSrl = _sequenceNumField.GetDeSerializer(codec);
                var valSrl = _valueField.GetDeSerializer(codec);

                ConstructorInfo ctor =
                    FldType.GetConstructor(new[] {_sequenceFieldInfo.FieldType, _valueFieldInfo.FieldType});

                if (ctor == null)
                    throw new SerializerException("Unable to find constructor for field {0}", FldType.FullName);

                return new Tuple<Expression, Expression>(
                    Expression.New(ctor, seqSrl.Item1, valSrl.Item1),
                    Expression.New(ctor, seqSrl.Item2, valSrl.Item2));
            }

            protected override void MakeReadonly()
            {
                ValidateType(FieldType, StateName);
                base.MakeReadonly();
            }

            private static void ValidateType(Type fieldType, string stateName)
            {
                if (fieldType != FldType)
                    throw new SerializerException(
                        "Value {0} has an unsupported type {1}", stateName, fieldType.AssemblyQualifiedName);
            }

            /// <summary>
            /// Override to compare the state of this object with another.
            /// </summary>
            /// <param name="baseOther">This object will always be of the same type as current</param>
            protected override bool Equals(BaseField baseOther)
            {
                var other = (ReadonlyItemLngDblField) baseOther;
                return _sequenceNumField.Equals(other._sequenceNumField) && _valueField.Equals(other._valueField);
            }

            /// <summary>
            /// Override to calculate hash code.
            /// There is need to override it unless this field has any state variables.
            /// </summary>
            public override int GetHashCode()
            {
                unchecked
                {
                    // ReSharper disable NonReadonlyFieldInGetHashCode
                    var hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ _sequenceNumField.GetHashCode();
                    hashCode = (hashCode*397) ^ _valueField.GetHashCode();
                    return hashCode;
                    // ReSharper restore NonReadonlyFieldInGetHashCode
                }
            }
        }

        #endregion
    }
}