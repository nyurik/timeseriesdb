using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using NYurik.TimeSeriesDb.Serializers;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

// ReSharper disable PossibleMultipleEnumeration

namespace NYurik.TimeSeriesDb.Samples
{
    /// <summary>
    /// This sample demonstrates how to configure a compressed file with calculated sequential index.
    /// </summary>
    internal class DemoIndexWithNoStateCompressed : ISample
    {
        #region ISample Members

        public void Run()
        {
            string filename1 = GetType().Name + "1.bts";
            if (File.Exists(filename1)) File.Delete(filename1);
            string filename2 = GetType().Name + "2.bts";
            if (File.Exists(filename2)) File.Delete(filename2);
            string filename3 = GetType().Name + "3.bts";
            if (File.Exists(filename3)) File.Delete(filename3);

            // Create new BinCompressedSeriesFile file that stores a sequence of ItemLngDbl structs
            // The file is indexed by a long value inside ItemLngDbl marked with the [Index] attribute.
            using (var bf1 = new BinCompressedSeriesFile<long, ItemLngDbl>(filename1))
            using (var bf2 = new BinCompressedSeriesFile<long, ItemLngDbl>(filename2))
            using (var bf3 = new BinSeriesFile<long, ItemLngDbl>(filename3))
            {
                //
                // Initialize new file parameters and create it
                //
                bf1.UniqueIndexes = true; // enforce index uniqueness - each index is +1

                //
                // Configure value storage. This is the only difference with using BinSeriesFile.
                //
                // When a new instance of BinCompressedSeriesFile is created,
                // RootField will be pre-populated with default configuration objects.
                // Some fields, such as doubles, require additional configuration before the file can be initialized.
                //
                var root1 = (ComplexField) bf1.FieldSerializer.RootField;

                // This double will contain values with no more than 2 digits after the decimal points.
                // Before serializing, multiply the value by 100 to convert to long.
                ((ScaledDeltaField) root1["Value"].Field).Multiplier = 100;

                // Set up index field as incremental
                SubFieldInfo old = root1["SequenceNum"];
                root1["SequenceNum"] =
                    old.Clone(
                        new IncrementalIndex(old.Field.StateStore, old.Field.ValueType, old.Field.StateName));

                bf1.InitializeNewFile(); // Finish new file initialization and create an empty file


                // 
                // Set up data generator to generate 10 items starting with index 3
                //
                IEnumerable<ArraySegment<ItemLngDbl>> data = Utils.GenerateData(
                    3, 10000, i => new ItemLngDbl(i, Math.Round(i/100.0, 2)));

                //
                // Append data to the file
                //

                bf1.AppendData(data);


                //
                // Initialize the second in an identical fashion without linking the states and append the same data
                //
                var root2 = (ComplexField) bf2.FieldSerializer.RootField;
                ((ScaledDeltaField) root2["Value"].Field).Multiplier = 100;
                bf2.InitializeNewFile();
                bf2.AppendData(data);

                //
                // Initialize the third uncompressed file and append the same data.
                //
                bf3.InitializeNewFile();
                bf3.AppendData(data);

                //
                // Print file sizes to see if there was any benefit
                //
                Console.WriteLine("  Calculated: {0,10:#,#} bytes", bf1.BaseStream.Length);
                Console.WriteLine("      Stored: {0,10:#,#} bytes", bf2.BaseStream.Length);
                Console.WriteLine("Uncompressed: {0,10:#,#} bytes", bf3.BaseStream.Length);
                Console.WriteLine();

                if (!bf1.Stream().SequenceEqual(bf2.Stream()))
                    throw new BinaryFileException("File #1 != #2");
                if (!bf1.Stream().SequenceEqual(bf3.Stream()))
                    throw new BinaryFileException("File #1 != #3");
            }

            //
            // Check that the settings are stored ok in the file and can be re-initialized on open
            //
            using (var bf1 = (IEnumerableFeed<long, ItemLngDbl>) BinaryFile.Open(filename1))
            using (var bf2 = (IEnumerableFeed<long, ItemLngDbl>) BinaryFile.Open(filename2))
            {
                if (!bf1.Stream().SequenceEqual(bf2.Stream()))
                    throw new BinaryFileException("File #1 != #2");
            }

            // cleanup
            File.Delete(filename1);
            File.Delete(filename2);
            File.Delete(filename3);
        }

        #endregion

        #region Nested type: IncrementalIndex

        private class IncrementalIndex : BaseField
        {
            // ReSharper disable UnusedMember.Local
            protected IncrementalIndex()
                // ReSharper restore UnusedMember.Local
            {
            }

            /// <summary>
            /// Integer and Float delta serializer.
            /// </summary>
            /// <param name="stateStore">Serializer with the state</param>
            /// <param name="valueType">Type of value to store</param>
            /// <param name="stateName">Name of the value (for debugging)</param>
            public IncrementalIndex(IStateStore stateStore, Type valueType, string stateName)
                : base(Version10, stateStore, valueType, stateName)
            {
            }

            public override int GetMaxByteSize()
            {
                return CodecBase.MaxBytesFor8;
            }

            protected override bool IsValidVersion(Version ver)
            {
                return ver == Version10;
            }

            protected override Tuple<Expression, Expression> GetSerializerExp(Expression valueExp, Expression codec)
            {
                //
                // long stateVar;
                //
                bool needToInit;
                ParameterExpression stateVarExp = StateStore.GetOrCreateStateVar(
                    StateName, typeof (long), out needToInit);

                ParameterExpression varState2Exp = Expression.Variable(typeof (long), "state2");

                //
                // stateVar2 = valueGetter();
                // if (checked(stateVar2 - stateVar) != (long) 1)
                //     throw new SerializerException();
                // stateVar = stateVar2;
                // return true;
                //
                Expression deltaExp =
                    Expression.Block(
                        typeof (bool),
                        new[] {varState2Exp},
                        Expression.Assign(varState2Exp, valueExp),
                        Expression.IfThen(
                            Expression.NotEqual(
                                Expression.SubtractChecked(varState2Exp, stateVarExp),
                                Expression.Constant((long) 1)),
                            Expression.Throw(
                                Expression.New(
                                    // ReSharper disable AssignNullToNotNullAttribute
                                    typeof (SerializerException).GetConstructor(new[] {typeof (string)}),
                                    // ReSharper restore AssignNullToNotNullAttribute
                                    Expression.Constant("Index values are not sequential")))),
                        Expression.Assign(stateVarExp, varState2Exp),
                        DebugValueExp(codec, stateVarExp, "MultFld WriteDelta"),
                        Expression.Constant(true)
                        );

                //
                // stateVar = valueGetter();
                // codec.WriteSignedValue(stateVar);
                //
                Expression initExp =
                    needToInit
                        ? Expression.Block(
                            Expression.Assign(stateVarExp, valueExp),
                            DebugValueExp(codec, stateVarExp, "MultFld WriteInit"),
                            WriteSignedValue(codec, stateVarExp))
                        : deltaExp;

                return new Tuple<Expression, Expression>(initExp, deltaExp);
            }

            protected override Tuple<Expression, Expression> GetDeSerializerExp(Expression codec)
            {
                //
                // long stateVar;
                //
                bool needToInit;
                ParameterExpression stateVarExp = StateStore.GetOrCreateStateVar(
                    StateName, typeof (long), out needToInit);

                //
                // ++stateVar;
                // return stateVar;
                //
                Expression deltaExp =
                    Expression.Block(
                        Expression.PreIncrementAssign(stateVarExp),
                        DebugValueExp(codec, stateVarExp, "MultFld ReadDelta"),
                        stateVarExp);

                //
                // stateVar = codec.ReadSignedValue();
                // return stateVar;
                //
                Expression initExp =
                    needToInit
                        ? Expression.Block(
                            Expression.Assign(stateVarExp, ReadSignedValue(codec)),
                            DebugValueExp(codec, stateVarExp, "MultFld ReadInit"),
                            stateVarExp)
                        : deltaExp;

                return new Tuple<Expression, Expression>(initExp, deltaExp);
            }

            protected override void MakeReadonly()
            {
                ThrowOnInitialized();
                if (ValueTypeCode != TypeCode.Int64)
                    throw new SerializerException(
                        "Value {0} has an unsupported type {1}", StateName, ValueType.AssemblyQualifiedName);

                base.MakeReadonly();
            }
        }

        #endregion
    }
}