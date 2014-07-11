namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.IO;
    using System.Text;
    using System.Transactions;
    using NServiceBus.Unicast;
    using NServiceBus.Unicast.Queuing;
    using Oracle.DataAccess.Client;

    /// <summary>
    /// Sends a message via Oracle AQ.
    /// </summary>
    public class OracleAQMessageSender : ISendMessages
    {
        private bool canEnlist;
        private string connectionString;

        /// <summary>
        /// Gets or sets connection String to the service hosting the service broker
        /// </summary>
        public string ConnectionString
        {
            get
            {
                return this.connectionString;
            }

            set
            {
                this.canEnlist = OracleAQMessageSender.CanEnlist(value);
                this.connectionString = value;
            }
        }

        /// <summary>
        /// Gets or sets queues name policy.
        /// </summary>
        public IQueueNamePolicy NamePolicy { get; set; }

        /// <summary>
        /// Sends the given message to the address.
        /// </summary>
        /// <param name="message">Message to send.</param>
        /// <param name="address">Message destination address.</param>
        public void Send(TransportMessage message, SendOptions sendOptions)
        {
            var address = sendOptions.Destination;
            using (OracleConnection conn = new OracleConnection(this.ConnectionString))
            {
                conn.Open();

                using (OracleAQQueue queue = new OracleAQQueue(this.NamePolicy.GetQueueName(address), conn, OracleAQMessageType.Xml))
                {
                    queue.EnqueueOptions.Visibility = this.GetVisibilityMode();

                    using (var stream = new MemoryStream())
                    {
                        TransportMessageMapper.SerializeToXml(message, stream);
                        OracleAQMessage aqMessage = new OracleAQMessage(Encoding.UTF8.GetString(stream.ToArray()));
                        aqMessage.Correlation = message.CorrelationId;
                        try
                        {
                            queue.Enqueue(aqMessage);
                        }
                        catch (OracleException ex)
                        {
                            if (ex.Number == OraCodes.QueueDoesNotExist)
                            {
                                throw new QueueNotFoundException { Queue = address };
                            }
                            else
                            {
                                throw;
                            }
                        }
                    }
                }
            }
        }

        private static bool CanEnlist(string connectionString)
        {
            // We can enlist connection if connectionString doesn't have "enlist=false;".
            OracleConnectionStringBuilder builder = new OracleConnectionStringBuilder(connectionString);
            return !string.Equals(builder.Enlist, "false", StringComparison.OrdinalIgnoreCase);
        }

        private OracleAQVisibilityMode GetVisibilityMode()
        {
            if (this.canEnlist && Transaction.Current != null)
            {
                return OracleAQVisibilityMode.OnCommit;
            }
            else
            {
                return OracleAQVisibilityMode.Immediate;
            }
        }
    }
}
