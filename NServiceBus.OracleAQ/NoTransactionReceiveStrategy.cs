using System;

namespace NServiceBus.Transports.OracleAQ
{
    internal class NoTransactionReceiveStrategy : IReceiveStrategy
    {
        private readonly string connectionString;
        private readonly OracleAQQueueWrapper errorQueue;
        private readonly Func<TransportMessage, bool> tryProcessMessageCallback;

        public NoTransactionReceiveStrategy(string connectionString, OracleAQQueueWrapper errorQueue, Func<TransportMessage, bool> tryProcessMessageCallback)
        {
            this.connectionString = connectionString;
            this.errorQueue = errorQueue;
            this.tryProcessMessageCallback = tryProcessMessageCallback;
        }


        public ReceiveResult TryReceiveFrom(OracleAQQueueWrapper queue)
        {
            try
            {
                var connection = queue.StartWaiting(this.connectionString);
                using (var readResult = queue.TryReceive())
                {
                    if (readResult.IsPoison)
                    {
                        this.errorQueue.Send(readResult.RawMessage.ToNativeMessage(), connection);
                        return ReceiveResult.NoMessage();
                    }

                    if (!readResult.Successful)
                    {
                        return ReceiveResult.NoMessage();
                    }

                    var result = ReceiveResult.Received(readResult.Message);
                    try
                    {
                        this.tryProcessMessageCallback(readResult.Message);
                        return result;
                    }
                    catch (Exception ex)
                    {
                        return result.FailedProcessing(ex);
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
