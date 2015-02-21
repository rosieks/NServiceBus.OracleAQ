using System;
using System.IO;
using System.Text;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;

namespace NServiceBus.Transports.OracleAQ
{
    public class RawMessage : IDisposable
    {
        private readonly Stream payload;
        private readonly string correlationId;
        private readonly byte[] messageId;

        public RawMessage(OracleAQMessage message)
        {
            using (var messagePayload = (OracleXmlType)message.Payload)
            {
                    this.correlationId = message.Correlation;
                    this.messageId = message.MessageId;
                    this.payload = new MemoryStream(Encoding.UTF8.GetBytes(messagePayload.Value));
            }
        }

        public RawMessage(Stream payload, string correlationId, byte[] messageId)
        {
            this.payload = payload;
            this.correlationId = correlationId;
            this.messageId = messageId;
        }

        public Stream Payload
        {
            get
            {
                return this.payload;
            }
        }

        public byte[] MessageId
        {
            get
            {
                return this.messageId;
            }
        }

        public string CorrelationId
        {
            get
            {
                return this.correlationId;
            }
        }

        public OracleAQMessage ToNativeMessage()
        {
            using (var stream = new MemoryStream())
            {
                this.payload.CopyTo(stream);
                return new OracleAQMessage(Encoding.UTF8.GetString(stream.ToArray()))
                {
                    Correlation = this.correlationId,
                };
            }
        }

        public void Dispose()
        {
            this.payload.Dispose();
        }

        public RawMessage Clone()
        {
            MemoryStream ms = null;
            try
            {
                ms = new MemoryStream();
                this.payload.CopyTo(ms);
                var result = new RawMessage(ms, this.correlationId, this.messageId);
                ms = null;
                return result;
            }
            finally
            {
                if (ms != null)
                {
                    ms.Dispose();
                    ms = null;
                }

            }
        }
    }
}
