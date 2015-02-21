namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.Transactions;
    using NServiceBus.Logging;
    using Oracle.DataAccess.Client;

    internal class OracleAQQueueWrapper
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(OracleAQQueueWrapper));
        private static readonly OracleAQDequeueOptions DequeueOptions = new OracleAQDequeueOptions
        {
            DequeueMode = OracleAQDequeueMode.Remove,
            ProviderSpecificType = true,
        };

        private readonly string clientInfo;
        private readonly string workQueue;

    private OracleConnection connection;
        private OracleAQQueue queue;

        public OracleAQQueueWrapper(Address address, string schema, IQueueNamePolicy namePolicy)
        {
            this.workQueue = namePolicy.GetQueueName(address);
            if (!string.IsNullOrEmpty(schema))
            {
                this.workQueue = schema += "." + this.workQueue;
            }

            this.clientInfo = string.Format("OracleAQDequeueStrategy for {0}", this.workQueue);
        }

        public OracleConnection StartWaiting(string connectionString)
        {
            try
            {
                this.connection = new OracleConnection(connectionString);

                this.queue = new OracleAQQueue(this.workQueue, this.connection, OracleAQMessageType.Xml);

                this.connection.Open();

                this.SetupClientInfo(this.connection);

                this.queue.Listen(null);

                return this.connection;
            }
            catch
            {
                this.Complete();

                throw;
            }
        }

        public void Complete()
        {
            if (this.connection != null)
            {
                this.connection.Dispose();
                this.connection = null;
            }

            if (this.queue != null)
            {
                this.queue.Dispose();
                this.queue = null;
            }
        }

        internal void Send(OracleAQMessage rawMessage, OracleConnection connection)
        {
            using (OracleAQQueue queue = new OracleAQQueue(this.workQueue, connection, OracleAQMessageType.Xml))
            {
                queue.EnqueueOptions.Visibility = this.GetVisibilityMode(connection.ConnectionString);

                queue.Enqueue(rawMessage);
            }
        }

        public MessageReadResult TryReceive()
        {
            return this.Receive();
        }

        private MessageReadResult Receive()
        {
            RawMessage message = null;
            try
            {
                message = new RawMessage(this.queue.Dequeue(DequeueOptions));
            }
            catch (OracleException ex)
            {
                if (ex.Number != OraCodes.TimeoutOrEndOfFetch)
                {
                    throw;
                }
            }

            try
            {
                return MessageReadResult.Success(TransportMessageMapper.DeserializeFromXml(message));
            }
            catch (Exception ex)
            {
                Logger.Error("Error receiving message. Probable message metadata corruption. Moving to error queue.", ex);

                return MessageReadResult.Poison(message);
            }
        }

        private void SetupClientInfo(OracleConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "begin DBMS_APPLICATION_INFO.SET_CLIENT_INFO(:p1); end;";
                command.Parameters.Add("p1", this.clientInfo);
                command.ExecuteNonQuery();
            }
        }

        private OracleAQVisibilityMode GetVisibilityMode(string connectionString)
        {
            if (Transaction.Current != null && OracleConnectionStringHelper.CanEnlist(connectionString))
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
