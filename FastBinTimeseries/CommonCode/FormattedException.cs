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
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using JetBrains.Annotations;

namespace NYurik.FastBinTimeseries.CommonCode
{
    /// <summary>
    /// Generic exception capable of delayed message formatting.
    /// Inherit for more specific exceptions.
    /// </summary>
    [Serializable]
    public class FormattedException : Exception
    {
        private readonly object[] _arguments;
        private readonly string _formatStr;
        private readonly bool _useFormat;

        [StringFormatMethod("message")]
        private FormattedException(bool useFormat, Exception inner, string message, object[] args)
            : base(message, inner)
        {
            _useFormat = useFormat;
            _formatStr = message;
            _arguments = args;

            if (useFormat && args != null)
            {
                // In case any of the arguments are non-basic types, convert to string
                // Otherwise their state might change by the time FormatString() is called.
                Assembly coreAssmbly = typeof (object).Assembly;
                if (args.Any(arg => arg != null && arg.GetType().Assembly != coreAssmbly))
                {
                    _formatStr = FormatString();
                    _useFormat = false;
                    _arguments = null;
                }
            }
        }

        public FormattedException()
            : this(false, null, null, null)
        {
        }

        public FormattedException(string message)
            : this(false, null, message, null)
        {
        }

        [StringFormatMethod("message")]
        public FormattedException(string message, params object[] args)
            : this(true, null, message, args)
        {
        }

        public FormattedException(Exception inner, string message)
            : this(false, inner, message, null)
        {
        }

        [StringFormatMethod("message")]
        public FormattedException(Exception inner, string message, params object[] args)
            : this(true, inner, message, args)
        {
        }

        public override string Message
        {
            get { return FormatString(); }
        }

        private string FormatString()
        {
            if (!_useFormat)
                return _formatStr;

            try
            {
                return string.Format(_formatStr, _arguments);
            }
            catch (Exception ex)
            {
                var sb = new StringBuilder();

                sb.Append("Error formatting exception: ");
                sb.Append(ex.Message);
                sb.Append("\nFormat string: ");
                sb.Append(_formatStr);
                if (_arguments != null && _arguments.Length > 0)
                {
                    sb.Append("\nArguments: ");
                    for (int i = 0; i < _arguments.Length; i++)
                    {
                        if (i > 0) sb.Append(", ");
                        try
                        {
                            sb.Append(_arguments[i]);
                        }
                        catch (Exception ex2)
                        {
                            sb.AppendFormat("(Argument #{0} cannot be shown: {1})", i, ex2.Message);
                        }
                    }
                }

                return sb.ToString();
            }
        }

        #region Serialization

        private const string SerializationField = "FormatString";

        protected FormattedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            _formatStr = (string) info.GetValue(SerializationField, typeof (string));
            // Leave other values at their default
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            // To avoid any serialization issues with param objects, format message now
            info.AddValue(SerializationField, Message, typeof (string));
        }

        #endregion
    }
}