using System;

namespace NServiceBus.Transports.OracleAQ
{
    internal class MessageReadResult : IDisposable
    {
        public static readonly MessageReadResult NoMessage = new MessageReadResult(null, false, null);

        private readonly TransportMessage message;
        private readonly bool poison;
        private readonly RawMessage rawMessage;

        private MessageReadResult(TransportMessage message, bool poison, RawMessage rawMessage)
        {
            this.message = message;
            this.poison = poison;
            this.rawMessage = rawMessage;
        }

        public bool IsPoison
        {
            get
            {
                return this.poison;
            }
        }

        public bool Successful
        {
            get
            {
                return this.message != null;
            }
        }

        public TransportMessage Message
        {
            get
            {
                return this.message;
            }
        }

        public RawMessage RawMessage
        {
            get
            {
                return this.rawMessage;
            }
        }

        public static MessageReadResult Poison(RawMessage rawMessage)
        {
            return new MessageReadResult(null, true, rawMessage);
        }

        public static MessageReadResult Success(TransportMessage message)
        {
            return new MessageReadResult(message, false, null);
        }

        public void Dispose()
        {
            if (this.rawMessage != null)
            {
                ((IDisposable)this.rawMessage.Payload).Dispose();
            }
        }
    }
}
