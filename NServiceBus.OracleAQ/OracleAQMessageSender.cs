namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Transactions;
    using NServiceBus.Unicast.Queuing;
    using Oracle.DataAccess.Client;

    /// <summary>
    /// Sends a message via Oracle AQ.
    /// </summary>
    public class OracleAQMessageSender : ISendMessages
    {
        private static ConcurrentDictionary<string, bool> CanEnlistConnectionString = new ConcurrentDictionary<string, bool>();

        /// <summary>
        /// Gets or sets connection String to the service hosting the service broker
        /// </summary>
        public string DefaultConnectionString { get; set; }

        public IDictionary<string, string> ConnectionStringCollection { get; set; }

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
            try
            {
                var queueConnectionString = this.DefaultConnectionString;
                if (ConnectionStringCollection.Keys.Contains(address.Queue))
                {
                    queueConnectionString = ConnectionStringCollection[address.Queue];
                }

                using (OracleConnection conn = new OracleConnection(queueConnectionString))
                {
                    conn.Open();

                    using (OracleAQQueue queue = new OracleAQQueue(this.NamePolicy.GetQueueName(address), conn, OracleAQMessageType.Xml))
                    {
                        queue.EnqueueOptions.Visibility = this.GetVisibilityMode(queueConnectionString);

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
            catch (OracleException ex)
            {
                if (ex.Number == OraCodes.QueueDoesNotExist && address != null)
                {
                    throw new QueueNotFoundException { Queue = address };
                }
                else
                {
                    OracleAQMessageSender.ThrowFailedToSendException(address, ex);
                }
            }
            catch (Exception ex)
            {
                OracleAQMessageSender.ThrowFailedToSendException(address, ex);
            }
        }

        private static void ThrowFailedToSendException(Address address, Exception ex)
        {
            if (address == null)
            {
                throw new FailedToSendMessageException("Failed to send message.", ex);
            }
            else
            {
                throw new FailedToSendMessageException(
                    string.Format("Failed to send message to address: {0}@{1}", address.Queue, address.Machine), ex);
            }

        }

        private static bool CanEnlist(string connectionString)
        {
            bool canEnlist;
            if (!OracleAQMessageSender.CanEnlistConnectionString.TryGetValue(connectionString, out canEnlist))
            {
                // We can enlist connection if connectionString doesn't have "enlist=false;".
                OracleConnectionStringBuilder builder = new OracleConnectionStringBuilder(connectionString);
                canEnlist = !string.Equals(builder.Enlist, "false", StringComparison.OrdinalIgnoreCase);
                OracleAQMessageSender.CanEnlistConnectionString.TryAdd(connectionString, canEnlist);
            }

            return canEnlist;
        }

        private OracleAQVisibilityMode GetVisibilityMode(string connectionString)
        {
            if (Transaction.Current != null && CanEnlist(connectionString))
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
