namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using NServiceBus.Pipeline;
    using NServiceBus.Unicast.Transport;

    internal class ReceiveStrategyFactory
    {
        private readonly IQueueNamePolicy namePolicy;
        private readonly PipelineExecutor pipelineExecutor;

        public ReceiveStrategyFactory(PipelineExecutor pipelineExecutor, IQueueNamePolicy namePolicy)
        {
            this.pipelineExecutor = pipelineExecutor;
            this.namePolicy = namePolicy;
        }

        public ConnectionParams ConnectionInfo { get; set; }

        public Address ErrorQueue { get; set; }

        public IReceiveStrategy Create(TransactionSettings settings, Func<TransportMessage, bool> tryProcessMessageCallback)
        {
            var errorQueue = new OracleAQQueueWrapper(this.ErrorQueue, this.ConnectionInfo.Schema, this.namePolicy);
            if (settings.IsTransactional)
            {
                if (settings.SuppressDistributedTransactions)
                {
                    throw new NotSupportedException("Native transaction is not supported");
                }

                return new AmbientTransactionReceiveStrategy(this.ConnectionInfo.ConnectionString, errorQueue, tryProcessMessageCallback, this.pipelineExecutor, settings);
            }
            else
            {
                return new NoTransactionReceiveStrategy(this.ConnectionInfo.ConnectionString, errorQueue, tryProcessMessageCallback);
            }
        }
    }
}