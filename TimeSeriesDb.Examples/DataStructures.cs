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

namespace NYurik.TimeSeriesDb.Examples
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