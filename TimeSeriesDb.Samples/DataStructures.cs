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

namespace NYurik.TimeSeriesDb.Samples
{
    internal struct ItemLngDbl
    {
        // ReSharper disable FieldCanBeMadeReadOnly.Global
        // ReSharper disable MemberCanBePrivate.Global
        [Index] public long SequenceNum;
        public double Value;
        // ReSharper restore MemberCanBePrivate.Global
        // ReSharper restore FieldCanBeMadeReadOnly.Global

        public ItemLngDbl(long sequenceNum, double value)
        {
            SequenceNum = sequenceNum;
            Value = value;
        }

        public override string ToString()
        {
            return string.Format("{0,3}: {1}", SequenceNum, Value);
        }
    }

    internal struct ItemLngDblDbl
    {
        // ReSharper disable FieldCanBeMadeReadOnly.Global
        // ReSharper disable MemberCanBePrivate.Global
        [Index] public long SequenceNum;
        public double Value1;
        public double Value2;
        // ReSharper restore MemberCanBePrivate.Global
        // ReSharper restore FieldCanBeMadeReadOnly.Global

        public ItemLngDblDbl(long sequenceNum, double value1, double value2)
        {
            SequenceNum = sequenceNum;
            Value1 = value1;
            Value2 = value2;
        }

        public override string ToString()
        {
            return string.Format("{0,3}: {1} {2}", SequenceNum, Value1, Value2);
        }
    }
}