using System;
using System.Collections.Generic;
using System.IO;

namespace NYurik.FastBinTimeseries
{
    public class BinCompressedSeriesFile2<TInd, TVal> : BinaryFile<byte>, IEnumerableFeed<TInd, TVal>
        where TInd : struct, IComparable<TInd>
    {
//        public BinCompressedSeriesFile(string fileName, FieldInfo indexFieldInfo = null)
//            : base(fileName, indexFieldInfo)
//        {
//            Serializer = new DeltaSerializer();
//        }

        #region IEnumerableFeed<TInd,TVal> Members

        public Func<TVal, TInd> IndexAccessor
        {
            get { throw new NotImplementedException(); }
        }

        public IEnumerable<Buffer<TVal>> StreamSegments(TInd fromInd, bool inReverse = false, IEnumerable<Buffer<TVal>> bufferProvider = null, long maxItemCount = long.MaxValue)
        {
            throw new NotImplementedException();
        }

        public void AppendData(IEnumerable<ArraySegment<TVal>> bufferStream, bool allowFileTruncation = false)
        {
            throw new NotImplementedException();
        }

        #endregion

        protected override Version Init(BinaryReader reader, IDictionary<string, Type> typeMap)
        {
            throw new NotImplementedException();
        }

        protected override Version WriteCustomHeader(BinaryWriter writer)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<ArraySegment<TVal>> StreamSegments(TInd fromInd, bool inReverse = false, int bufferSize = 0)
        {
            throw new NotImplementedException();
        }

        protected void PerformWriteStreaming(IEnumerable<ArraySegment<TVal>> stream, long firstItemIdx = long.MaxValue)
        {
        }
    }
}