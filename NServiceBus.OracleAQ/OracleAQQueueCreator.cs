namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using NServiceBus.Logging;
    using Oracle.DataAccess.Client;

    /// <summary>
    /// OracleAQQueueCreator is used to create queues in Oracle database.
    /// </summary>
    internal class OracleAQQueueCreator : ICreateQueues
    {
        private const string CreateQueueSql = @"
            DECLARE 
                cnt NUMBER;
                schemaPrefix VARCHAR2(61);
            BEGIN 
                SELECT count(*) INTO cnt FROM all_queues WHERE name = :queue AND owner = NVL(:schema, sys_context('userenv', 'current_schema'));
                SELECT decode(:schema, null, '', :schema || '.') INTO schemaPrefix FROM dual;
                IF cnt = 0 THEN
                    dbms_aqadm.create_queue(
                        queue_name => schemaPrefix || :queue,
                        queue_table => schemaPrefix || :queueTable,
                        max_retries => 999);
                END IF;
                DBMS_AQADM.START_QUEUE(schemaPrefix || :queue);
            END;";

        private const string CreateQueueTableSql = @"
            DECLARE 
                cnt NUMBER;
                schemaPrefix VARCHAR2(61);
            BEGIN 
                SELECT count(*) INTO cnt FROM all_tables WHERE table_name = :queueTable AND owner = NVL(:schema, sys_context('userenv', 'current_schema'));
                SELECT decode(:schema, null, '', :schema || '.') INTO schemaPrefix FROM dual;
                IF cnt = 0 THEN
                    dbms_aqadm.create_queue_table(schemaPrefix || :queueTable, 'SYS.XMLType');
                END IF;
            END;";

        private const string DoesQueueExistSql = @"SELECT count(*) FROM all_queues WHERE name = :queue AND owner = NVL(:schema, sys_context('userenv', 'current_schema'))";

        private static readonly ILog Logger = LogManager.GetLogger(typeof(OracleAQQueueCreator));

        public string ConnectionString { get; set; }

        public string Schema { get; set; }

        public IQueueNamePolicy NamePolicy { get; set; }

        /// <summary>
        /// Create a messages queue where its name is the address parameter.
        /// </summary>
        /// <param name="address">Address of queue</param>
        /// <param name="account">Not used parameter</param>
        public void CreateQueueIfNecessary(Address address, string account)
        {
            Logger.DebugFormat("Checking if queue exists: {0}.", address);

            try
            {
                if (!this.DoesQueueExist(address))
                {
                    Logger.WarnFormat("Queue {0} does not exist.", address);
                    Logger.DebugFormat("Going to create queue table: {0}", address);

                    this.CreateQueueTable(address);
                    this.CreateQueue(address);
                }
            }
            catch (Exception ex)
            {
                Logger.Error(
                    string.Format("Could not create queue {0} or check its existence. Processing will still continue.", address),
                    ex);
            }
        }

        private void CreateQueue(Address address)
        {
            string queue = this.NamePolicy.GetQueueName(address);
            string queueTable = this.NamePolicy.GetQueueTableName(address);

            using (OracleConnection conn = new OracleConnection(this.ConnectionString))
            {
                conn.Open();
                using (OracleCommand createQueue = conn.CreateCommand())
                {
                    createQueue.BindByName = true;
                    createQueue.CommandText = CreateQueueSql;
                    createQueue.Parameters.Add("queue", queue);
                    createQueue.Parameters.Add("queueTable", queueTable);
                    createQueue.Parameters.Add("schema", this.GetDbSchema());
                    createQueue.ExecuteNonQuery();
                }

                Logger.DebugFormat("Created queue, name: [{0}], queue table: [{1}]", queue, queueTable);
            }
        }

        private void CreateQueueTable(Address address)
        {
            string queueTable = this.NamePolicy.GetQueueTableName(address);

            using (OracleConnection conn = new OracleConnection(this.ConnectionString))
            {
                conn.Open();

                using (OracleCommand createTable = conn.CreateCommand())
                {
                    createTable.BindByName = true;
                    createTable.CommandText = CreateQueueTableSql;
                    createTable.Parameters.Add("queueTable", queueTable);
                    createTable.Parameters.Add("schema", this.GetDbSchema());
                    createTable.ExecuteNonQuery();
                }

                Logger.DebugFormat("Created queue table: [{0}]", queueTable);
            }
        }

        private bool DoesQueueExist(Address address)
        {
            using (OracleConnection conn = new OracleConnection(this.ConnectionString))
            {
                conn.Open();

                using (OracleCommand doesQueueExist = conn.CreateCommand())
                {
                    doesQueueExist.BindByName = true;
                    doesQueueExist.CommandText = DoesQueueExistSql;
                    doesQueueExist.Parameters.Add("queue", this.NamePolicy.GetQueueName(address));
                    doesQueueExist.Parameters.Add("schema", this.GetDbSchema());
                    return Convert.ToBoolean(doesQueueExist.ExecuteScalar());
                }
            }
        }

        private object GetDbSchema()
        {
            if (string.IsNullOrEmpty(this.Schema))
            {
                return DBNull.Value;
            }
            else
            {
                return this.Schema.ToUpper();
            }
        }
    }
}
