namespace NServiceBus.Transports.OracleAQ
{
    using System;

    internal class ReceiveResult
    {
        private readonly Exception exception;
        private readonly TransportMessage message;

        private ReceiveResult(Exception exception, TransportMessage message)
        {
            this.exception = exception;
            this.message = message;
        }

        public Exception Exception
        {
            get
            {
                return this.exception;
            }
        }

        public TransportMessage Message
        {
            get
            {
                return this.message;
            }
        }

        public bool HasReceivedMessage
        {
            get
            {
                return this.message != null;
            }
        }

        public static ReceiveResult NoMessage()
        {
            return new ReceiveResult(null, null);
        }

        internal static ReceiveResult Received(TransportMessage message)
        {
            return new ReceiveResult(null, message);
        }

        internal ReceiveResult FailedProcessing(Exception ex)
        {
            return new ReceiveResult(ex, null);
        }
    }
}
