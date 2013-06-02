namespace NServiceBus.Transports.OracleAQ
{
    using Oracle.DataAccess.Client;

    public class OracleAQQueueCreator : ICreateQueues
    {
        private const string CreateQueue = @"
            DECLARE 
                cnt NUMBER; 
            BEGIN 
                SELECT count(*) INTO cnt FROM dba_queues WHERE name = :queue;
                IF cnt = 0 THEN
                    dbms_aqadm.create_queue(
                        queue_name => :queue,
                        queue_table => :queueTable,
                        max_retries => 999);
                END IF;
                DBMS_AQADM.START_QUEUE(:queue);
            END;";

        private const string CreateQueueTable = @"
            DECLARE 
                cnt NUMBER; 
            BEGIN 
                SELECT count(*) INTO cnt FROM all_tables WHERE table_name = :queueTable;
                IF cnt = 0 THEN
                    dbms_aqadm.create_queue_table(:queueTable, 'SYS.XMLType');
                END IF;
            END;";

        public string ConnectionString { get; set; }

        public IQueueNamePolicy NamePolicy { get; set; }

        public void CreateQueueIfNecessary(Address address, string account)
        {
            using (OracleConnection conn = new OracleConnection(this.ConnectionString))
            {
                conn.Open();

                using (OracleCommand createTable = conn.CreateCommand())
                {
                    createTable.BindByName = true;
                    createTable.CommandText = CreateQueueTable;
                    createTable.Parameters.Add("queueTable", this.NamePolicy.GetQueueTableName(address));
                    createTable.ExecuteNonQuery();
                }

                using (OracleCommand createQueue = conn.CreateCommand())
                {
                    createQueue.BindByName = true;
                    createQueue.CommandText = CreateQueue;
                    createQueue.Parameters.Add("queue", this.NamePolicy.GetQueueName(address));
                    createQueue.Parameters.Add("queueTable", this.NamePolicy.GetQueueTableName(address));
                    createQueue.ExecuteNonQuery();
                }
            }
        }
    }
}
