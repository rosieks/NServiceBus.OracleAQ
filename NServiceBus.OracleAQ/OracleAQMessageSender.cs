namespace NServiceBus.Transports.OracleAQ
{
    using System.IO;
    using System.Text;
    using System.Transactions;
    using Oracle.DataAccess.Client;

    /// <summary>
    /// Sends a message via Oracle AQ.
    /// </summary>
    public class OracleAQMessageSender : ISendMessages
    {
        /// <summary>
        /// Gets or sets connection String to the service hosting the service broker
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets queues name policy.
        /// </summary>
        public IQueueNamePolicy NamePolicy { get; set; }

        /// <summary>
        /// Sends the given message to the address.
        /// </summary>
        /// <param name="message">Message to send.</param>
        /// <param name="address">Message destination address.</param>
        public void Send(TransportMessage message, Address address)
        {
            using (OracleConnection conn = new OracleConnection(this.ConnectionString))
            {
                conn.Open();

                using (OracleAQQueue queue = new OracleAQQueue(this.NamePolicy.GetQueueName(address), conn, OracleAQMessageType.Xml))
                {
                    queue.EnqueueOptions.Visibility = Transaction.Current == null ? OracleAQVisibilityMode.Immediate : OracleAQVisibilityMode.OnCommit;

                    using (var stream = new MemoryStream())
                    {
                        TransportMessageMapper.SerializeToXml(message, stream);
                        OracleAQMessage aqMessage = new OracleAQMessage(Encoding.UTF8.GetString(stream.ToArray()));
                        aqMessage.Correlation = message.CorrelationId;
                        queue.Enqueue(aqMessage);
                    }
                }
            }
        }
    }
}
