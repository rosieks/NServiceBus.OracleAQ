namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using NServiceBus.Pipeline;
    using Oracle.DataAccess.Client;

    internal static class PipelineExecutorExtensions
    {
        public static IDisposable SetConnection(this PipelineExecutor pipelineExecutor, string connectionString, OracleConnection connection)
        {
            var key = MakeConnectionKey(connectionString);
            pipelineExecutor.CurrentContext.Set(key, connection);
            return new ContextItemRemovalDisposable(key, pipelineExecutor);
        }

        private static string MakeConnectionKey(string connectionString)
        {
            return string.Format("SqlConnection-{0}", connectionString);
        }

        private class ContextItemRemovalDisposable : IDisposable
        {
            private readonly string contextKey;
            private readonly PipelineExecutor pipelineExecutor;

            public ContextItemRemovalDisposable(string contextKey, PipelineExecutor pipelineExecutor)
            {
                this.contextKey = contextKey;
                this.pipelineExecutor = pipelineExecutor;
            }

            public void Dispose()
            {
                this.pipelineExecutor.CurrentContext.Remove(this.contextKey);
            }
        }
    }
}
