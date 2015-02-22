﻿namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using NServiceBus.Pipeline;
    using NServiceBus.Unicast;
    using NServiceBus.Unicast.Queuing;
    using Oracle.DataAccess.Client;

    /// <summary>
    /// Sends a message via Oracle AQ.
    /// </summary>
    internal class OracleAQMessageSender : ISendMessages
    {
        /// <summary>
        /// Gets or sets connection String to the service hosting the service broker
        /// </summary>
        public string DefaultConnectionString { get; set; }

        public IDictionary<string, string> ConnectionStringCollection { get; set; }

        public string Schema { get; set; }

        /// <summary>
        /// Gets or sets queues name policy.
        /// </summary>
        public IQueueNamePolicy NamePolicy { get; set; }

        public PipelineExecutor PipelineExecutor { get; set; }

        /// <summary>
        /// Sends the given message to the address.
        /// </summary>
        /// <param name="message">Message to send.</param>
        public void Send(TransportMessage message, SendOptions sendOptions)
        {
            var address = sendOptions.Destination;
            var queue = address.Queue;
            var schema = this.Schema;

            try
            {
                var queueConnectionString = this.DefaultConnectionString;
                if (this.ConnectionStringCollection.Keys.Contains(queue))
                {
                    queueConnectionString = this.ConnectionStringCollection[queue];
                    schema = null;
                }

                OracleConnection conn;
                if (this.PipelineExecutor.TryGetConnection(queueConnectionString, out conn))
                {
                    this.SendMessage(message, sendOptions, schema, conn);
                }
                else
                {
                    using (conn = new OracleConnection(queueConnectionString))
                    {
                        conn.Open();
                        this.SendMessage(message, sendOptions, schema, conn);
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

        private void SendMessage(TransportMessage message, SendOptions options, string schema, OracleConnection conn)
        {
            var queue = new OracleAQQueueWrapper(options.Destination, schema, this.NamePolicy);

            using (var stream = new MemoryStream())
            {
                TransportMessageMapper.SerializeToXml(message, options, stream);
                OracleAQMessage aqMessage = new OracleAQMessage(Encoding.UTF8.GetString(stream.ToArray()));
                aqMessage.Correlation = message.CorrelationId;

                queue.Send(aqMessage, conn);
            }
        }

        private static void ThrowFailedToSendException(Address address, Exception ex)
        {
            if (address == null)
            {
                throw new Exception("Failed to send message.", ex);
            }
            else
            {
                throw new Exception(
                    string.Format("Failed to send message to address: {0}@{1}", address.Queue, address.Machine), ex);
            }
        }
    }
}
