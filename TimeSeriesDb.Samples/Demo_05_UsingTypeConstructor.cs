using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NYurik.TimeSeriesDb.Serializers.BlockSerializer;

namespace NYurik.TimeSeriesDb.Samples
{
    // Do not disable these Resharper checks in your code. Demo purposes only.
    // ReSharper disable PossibleMultipleEnumeration
    // ReSharper disable InconsistentNaming

    /// <summary>
    /// This sample demonstrates how to configure a compressed file with read-only struct using a custom constructor.
    /// </summary>
    internal class Demo_05_UsingTypeConstructor : ISample
    {
        #region ISample Members

        public void Run()
        {
            // Create filenames, deleting existing files if exist
            string filename = CreateFilename();

            // 
            // Set up sample data.
            // Note that we might get minor rounding errors when storing.
            // The Equals implementation of the Item accounts for that.
            //
            const int itemCount = 10000;
            IEnumerable<ArraySegment<Item>> data = Utils.GenerateData(
                0, itemCount, i => new Item(i));


            // Create new BinCompressedSeriesFile file that stores a sequence of Item structs
            // The file is indexed by a long value inside Item marked with the [Index] attribute.
            // See the Item struct declaration
            using (var bf = new BinCompressedSeriesFile<long, Item>(filename))
            {
                // Automatically pick the constructor that would set all the public fields in the struct
                var cmpxFld = ((ComplexField) bf.RootField);
                cmpxFld.PopulateFields(ComplexField.Mode.Constructor | ComplexField.Mode.Fields);

                Console.WriteLine("Serialized Fields:\n  {0}\n", string.Join(Environment.NewLine + "  ", cmpxFld.Fields));
                Console.WriteLine("Deserialized with constrtuctor:\n  {0}\n", cmpxFld.Constructor);


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
            using (var bf1 = (IWritableFeed<long, Item>) BinaryFile.Open(filename))
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

        #region Nested type: Item

        /// <summary>
        /// This object is readonly, so at this time we are unable to automatically serialize it (might change in the future)
        /// Use [<see cref="CtorFieldAttribute"/>] on the constructor to explicitly choose which one should be used to deserialize this object.
        /// Without this attribute, a constructor will be chosen (must be the only one non-default public, or only one non-default private)
        /// Use [<see cref="CtorFieldMapToAttribute"/>] on each parameter of the constructor to specify the field or property to map it to.
        /// Without it, a field or property with the same name / capitalized first letter / with prefix "_" and "m_" will be used.
        /// </summary>
        internal struct Item
        {
            [Index] public readonly long SequenceNum;

            [CtorField]
            public Item(
                [CtorFieldMapTo("SequenceNum")] 
                long sequenceNum)
            {
                SequenceNum = sequenceNum;
            }

            public override string ToString()
            {
                return string.Format("{0}", SequenceNum);
            }

            public bool Equals(Item other)
            {
                return SequenceNum == other.SequenceNum;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (obj.GetType() != typeof (Item)) return false;
                return Equals((Item) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return SequenceNum.GetHashCode();
                }
            }
        }

        #endregion
    }
}