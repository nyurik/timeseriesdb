#region COPYRIGHT

/*
 *     Copyright 2009-2011 Yuri Astrakhan  (<Firstname><Lastname>@gmail.com)
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

namespace NYurik.FastBinTimeseries
{
    /// <summary>
    /// The implementor of this interface can execute <see cref="IGenericCallable{TDst,TArg}.Run{T}"/> method.
    /// </summary>
    public interface IGenericInvoker
    {
        /// <summary>
        /// Calls a factory method without explicitly specifying the type of the sub-item.
        /// </summary>
        TDst RunGenericMethod<TDst, TArg>(IGenericCallable<TDst, TArg> callable, TArg arg);
    }

    /// <summary>
    /// This interface is used to run a generic method without referencing the generic subtype
    /// </summary>
    public interface IGenericCallable<out TDst, in TArg>
    {
        TDst Run<T>(IGenericInvoker source, TArg arg);
    }
}