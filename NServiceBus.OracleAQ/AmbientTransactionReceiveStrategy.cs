namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.Transactions;
    using NServiceBus.Pipeline;
    using NServiceBus.Unicast.Transport;

    internal class AmbientTransactionReceiveStrategy : IReceiveStrategy
    {
        private readonly TransactionOptions transactionOptions;
        private readonly string connectionString;
        private readonly PipelineExecutor pipelineExecutor;
        private readonly OracleAQQueueWrapper errorQueue;
        private readonly Func<TransportMessage, bool> tryProcessMessageCallback;

        public AmbientTransactionReceiveStrategy(string connectionString, OracleAQQueueWrapper errorQueue, Func<TransportMessage, bool> tryProcessMessageCallback, PipelineExecutor pipelineExecutor, TransactionSettings transactionSettings)
        {
            this.pipelineExecutor = pipelineExecutor;
            this.tryProcessMessageCallback = tryProcessMessageCallback;
            this.errorQueue = errorQueue;
            this.connectionString = connectionString;
            this.transactionOptions = new TransactionOptions
            {
                IsolationLevel = transactionSettings.IsolationLevel,
                Timeout = transactionSettings.TransactionTimeout,
            };
        }

        public ReceiveResult TryReceiveFrom(OracleAQQueueWrapper queue)
        {
            try
            {
                var connection = queue.StartWaiting(this.connectionString);
                using (var scope = new TransactionScope(TransactionScopeOption.Required, this.transactionOptions))
                {
                    connection.EnlistTransaction(Transaction.Current);
                    using (this.pipelineExecutor.SetConnection(this.connectionString, connection))
                    {
                        using (var readResult = queue.TryReceive())
                        {
                            if (readResult.IsPoison)
                            {
                                this.errorQueue.Send(readResult.RawMessage.ToNativeMessage(), connection);
                                scope.Complete();
                                return ReceiveResult.NoMessage();
                            }

                            if (!readResult.Successful)
                            {
                                scope.Complete();
                                return ReceiveResult.NoMessage();
                            }

                            var result = ReceiveResult.Received(readResult.Message);

                            try
                            {
                                if (this.tryProcessMessageCallback(readResult.Message))
                                {
                                    // NOTE: We explicitly calling Dispose so that we force any exception to not bubble,
                                    // eg Concurrency/Deadlock exception.
                                    scope.Complete();
                                    scope.Dispose();
                                }

                                return result;
                            }
                            catch (Exception ex)
                            {
                                return result.FailedProcessing(ex);
                            }
                        }
                    }
                }
            }
            finally
            {
                queue.Complete();
            }
        }
    }
}
