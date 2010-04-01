using System;
using System.Runtime.Serialization;
using System.Text;

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

        private FormattedException(bool useFormat, Exception inner, string message, params object[] args)
            : base(message, inner)
        {
            _useFormat = useFormat;
            _formatStr = message;
            _arguments = args;
        }

        public FormattedException()
            : this(false, null, null, null)
        {}

        public FormattedException(string message)
            : this(false, null, message, null)
        {}

        public FormattedException(string message, params object[] args)
            : this(true, null, message, args)
        {}

        public FormattedException(Exception inner, string message)
            : this(false, inner, message, null)
        {}

        public FormattedException(Exception inner, string message, params object[] args)
            : this(true, inner, message, args)
        {}

        public override string Message
        {
            get
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