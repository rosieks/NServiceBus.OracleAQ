namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using Oracle.DataAccess.Client;

    public class OracleAQPurger
    {
        private const string PurgeSql = @"
            declare
                po_t dbms_aqadm.aq$_purge_options_t;
                qt varchar2(30);
            begin
                select queue_table into qt from all_queues where name = :queue and owner = NVL(:schema, sys_context('userenv', 'current_schema'));
                po_t.block := TRUE;
                dbms_aqadm.purge_queue_table(
                    queue_table => qt,
                    purge_condition => :queueCondition,
                    purge_options => po_t);
            end;";

        public string ConnectionString { get; set; }

        public string Schema { get; set; }

        public void Purge(string queue)
        {
            using (OracleConnection conn = new OracleConnection(this.ConnectionString))
            {
                conn.Open();
                using (OracleCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = PurgeSql;
                    cmd.Parameters.Add("queue", queue);
                    cmd.Parameters.Add("schema", (object)this.Schema ?? DBNull.Value);
                    cmd.Parameters.Add("queueCondition", string.Format("qtview.queue = '{0}'", queue));
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}
