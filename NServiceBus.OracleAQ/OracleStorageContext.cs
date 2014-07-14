namespace NServiceBus.Transports.OracleAQ
{
    using System.Data;
    using NServiceBus.Pipeline;

    /// <summary>
    /// Provides users with access to the current Oracle transport <see cref="IDbConnection"/>. 
    /// </summary>
    public class OracleStorageContext
    {
        private readonly string connectionString;
        private readonly PipelineExecutor pipelineExecutor;

        public OracleStorageContext(PipelineExecutor pipelineExecutor, string connectionString)
        {
            this.pipelineExecutor = pipelineExecutor;
            this.connectionString = connectionString;
        }

        /// <summary>
        /// Gets the current context Oracle transport <see cref="IDbConnection"/> or <code>null</code> if no current context Oracle transport <see cref="IDbConnection"/> available.
        /// </summary>
        public IDbConnection Connection
        {
            get
            {
                IDbConnection connection;
                if (this.pipelineExecutor.CurrentContext.TryGet(string.Format("SqlConnection-{0}", this.connectionString), out connection))
                {
                    return connection;
                }

                return null;
            }
        }
    }
}
