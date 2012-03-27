using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
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
    internal class Demo_5_CompressedCustom : ISample
    {
        #region ISample Members

        public void Run()
        {
            // Create filenames, deleting existing files if exist
            string filename1 = CreateFilename(1);
            string filename2 = CreateFilename(2);
            string filename3 = CreateFilename(3);
            string filename4 = CreateFilename(4);

            var sw1 = new Stopwatch();
            var sw2 = new Stopwatch();
            var sw3 = new Stopwatch();
            var sw4 = new Stopwatch();

            // 
            // Set up sample data so that the delta between index's long is 1
            // and the delta between values is 0.65 => 65 with multiplier, which is bigger than
            // would fit into a 7 bit signed integer, but would fit into 7 bit unsigned one
            //
            const int itemCount = 500000;
            IEnumerable<ArraySegment<ItemLngDbl>> data = Utils.GenerateData(
                0, itemCount, i => new ItemLngDbl(i, Math.Round((i / 100.0) * 65.0, 2)));


            // Create new BinCompressedSeriesFile file that stores a sequence of ItemLngDbl structs
            // The file is indexed by a long value inside ItemLngDbl marked with the [Index] attribute.
            using (var bf1 = new BinCompressedSeriesFile<long, ItemLngDbl>(filename1))
            using (var bf2 = new BinCompressedSeriesFile<long, ItemLngDbl>(filename2))
            using (var bf3 = new BinCompressedSeriesFile<long, ItemLngDbl>(filename3))
            using (var bf4 = new BinSeriesFile<long, ItemLngDbl>(filename4))
            {
                //
                // Configure bf1 to be the most compression optimized:
                //  * use custom incremental field serializer IncrementalIndex
                //  * use positive-only DeltaType for the value (it always increases in this test)
                //
                // When a new instance of BinCompressedSeriesFile is created,
                // RootField will be pre-populated with default configuration objects.
                // Some fields, such as doubles, require additional configuration before the file can be initialized.
                //
                var root1 = (ComplexField) bf1.FieldSerializer.RootField;

                // This double will contain values with no more than 2 digits after the decimal points.
                // Before serializing, multiply the value by 100 to convert to long.
                // Next value will always be same or larger than the previous one
                var val1 = (ScaledDeltaFloatField) root1["Value"].Field;
                val1.Multiplier = 100;
                val1.DeltaType = DeltaType.Positive;

                // Set up index field as incremental
                SubFieldInfo ind1 = root1["SequenceNum"];
                root1["SequenceNum"] =
                    ind1.Clone(new IncrementalIndex(ind1.Field.StateStore, ind1.Field.ValueType, ind1.Field.StateName));

                bf1.UniqueIndexes = true; // enforce index uniqueness - each index is +1
                bf1.InitializeNewFile(); // Finish new file initialization and create an empty file


                //
                // Initialize bf2 same as bf1, but without custom serializer
                //
                var val2 = (ScaledDeltaFloatField) ((ComplexField) bf2.FieldSerializer.RootField)["Value"].Field;
                val2.Multiplier = 100;
                val2.DeltaType = DeltaType.Positive;
                bf2.UniqueIndexes = true; 
                bf2.InitializeNewFile();

                //
                // Initialize bf3 in an identical fashion as bf2, but without positive-only delta type.
                //
                var val3 = ((ScaledDeltaFloatField) ((ComplexField) bf3.FieldSerializer.RootField)["Value"].Field);
                val3.Multiplier = 100;
                bf3.UniqueIndexes = true; 
                bf3.InitializeNewFile();

                //
                // Initialize the third uncompressed file without any parameters.
                //
                bf4.UniqueIndexes = true;
                bf4.InitializeNewFile();

                //
                // Append the same data to all files, measuring how long it takes
                // Please note that the timing is not very accurate here, and will give different results depending on the order
                //
                sw4.Start();
                bf4.AppendData(data);
                sw4.Stop();

                sw3.Start();
                bf3.AppendData(data);
                sw3.Stop();

                sw2.Start();
                bf2.AppendData(data);
                sw2.Stop();

                sw1.Start();
                bf1.AppendData(data);
                sw1.Stop();

                //
                // Verify that the created files are identical (use the default bitwise value type Equals)
                //
                if (!bf1.Stream().SequenceEqual(bf2.Stream()))
                    throw new BinaryFileException("File #1 != #2");
                if (!bf1.Stream().SequenceEqual(bf3.Stream()))
                    throw new BinaryFileException("File #1 != #3");
                if (!bf1.Stream().SequenceEqual(bf4.Stream()))
                    throw new BinaryFileException("File #1 != #4");

                //
                // Print file sizes to see if there was any benefit
                //
                Console.WriteLine("Finished creating files with {0:#,#} items:\n", itemCount);
                Console.WriteLine("{2,40}: {0,10:#,#} bytes in {1}", bf1.BaseStream.Length, sw1.Elapsed, "DeltaType.Positive and Calculated index");
                Console.WriteLine("{2,40}: {0,10:#,#} bytes in {1}", bf2.BaseStream.Length, sw2.Elapsed, "DeltaType.Positive");
                Console.WriteLine("{2,40}: {0,10:#,#} bytes in {1}", bf3.BaseStream.Length, sw3.Elapsed, "No optimizations");
                Console.WriteLine("{2,40}: {0,10:#,#} bytes in {1}", bf4.BaseStream.Length, sw4.Elapsed, "Uncompressed");
                Console.WriteLine();
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
            CreateFilename(1);
            CreateFilename(2);
            CreateFilename(3);
            CreateFilename(4);
        }

        #endregion

        private string CreateFilename(int i)
        {
            string filename = GetType().Name + i + ".bts";
            if (File.Exists(filename)) File.Delete(filename);
            return filename;
        }

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