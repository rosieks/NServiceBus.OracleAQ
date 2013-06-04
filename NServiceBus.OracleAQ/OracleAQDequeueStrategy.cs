namespace NServiceBus.Transports.OracleAQ
{
    using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Schedulers;
using System.Transactions;
using NServiceBus.CircuitBreakers;
using NServiceBus.Logging;
using NServiceBus.Unicast.Transport;
using Oracle.DataAccess.Client;

    public class OracleAQDequeueStrategy : IDequeueMessages
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(OracleAQDequeueStrategy));
        private readonly ICircuitBreaker circuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker(
            "OracleAQTransportConnectivity",
            TimeSpan.FromMinutes(2),
            ex => Configure.Instance.RaiseCriticalError("Repeated failures when communicating with Oracle database", ex),
            TimeSpan.FromSeconds(10));

        private TransactionOptions transactionOptions;
        private Func<TransportMessage, bool> tryProcessMessage;
        private Action<TransportMessage, Exception> endProcessMessage;
        private string workQueue;
        private MTATaskScheduler scheduler;
        private CancellationTokenSource tokenSource;
        private OracleAQDequeueOptions dequeueOptions;

        public OracleAQPurger Purger { get; set; }

        public IQueueNamePolicy NamePolicy { get; set; }

        public bool PurgeOnStartup { get; set; }

        public string ConnectionString { get; set; }

        public void Init(Address address, TransactionSettings transactionSettings, Func<TransportMessage, bool> tryProcessMessage, Action<TransportMessage, Exception> endProcessMessage)
        {
            this.tryProcessMessage = tryProcessMessage;
            this.endProcessMessage = endProcessMessage;
            this.workQueue = this.NamePolicy.GetQueueName(address);

            this.transactionOptions = new TransactionOptions
            {
                IsolationLevel = transactionSettings.IsolationLevel,
                Timeout = transactionSettings.TransactionTimeout,
            };

            this.dequeueOptions = new OracleAQDequeueOptions
            {
                DequeueMode = OracleAQDequeueMode.Remove,
                ProviderSpecificType = true,
            };

            if (this.PurgeOnStartup)
            {
                this.Purger.Purge(this.workQueue);
            }
        }

        public void Start(int maximumConcurrencyLevel)
        {
            this.tokenSource = new CancellationTokenSource();
            this.scheduler = new MTATaskScheduler(
                maximumConcurrencyLevel,
                string.Format("NServiceBus Dequeuer Worker Thread for [{0}]", this.workQueue));

            for (int i = 0; i < maximumConcurrencyLevel; i++)
            {
                this.StartThread();
            }
        }

        public void Stop()
        {
            this.tokenSource.Cancel();
            this.scheduler.Dispose();
        }

        private void StartThread()
        {
            CancellationToken token = this.tokenSource.Token;

            Task.Factory
                .StartNew(this.Action, token, token, TaskCreationOptions.None, this.scheduler)
                .ContinueWith(t =>
                {
                    t.Exception.Handle(ex =>
                        {
                            Logger.Warn("Failed to connect to the configured Oracle database");
                            circuitBreaker.Failure(ex);
                            return true;
                        });

                    this.StartThread();
                }, TaskContinuationOptions.OnlyOnFaulted);
        }

        private void Action(object obj)
        {
            var cancellationToken = (CancellationToken)obj;

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = new ReceiveResult();

                try
                {
                    using (var connection = new OracleConnection(this.ConnectionString))
                    {
                        using (var queue = new OracleAQQueue(this.workQueue, connection, OracleAQMessageType.Xml))
                        {
                            connection.Open();

                            queue.Listen(null);

                            result = this.TryReceive(queue);
                        }
                    }
                }
                catch (Exception ex)
                {
                    result.Exception = ex;
                }
                finally
                {
                    if (result.Message != null)
                    {
                        this.endProcessMessage(result.Message, result.Exception);
                    }
                }
            }
        }

        private ReceiveResult TryReceive(OracleAQQueue queue)
        {
            var result = new ReceiveResult();

            using (var ts = new TransactionScope(TransactionScopeOption.Required, this.transactionOptions))
            {
                queue.Connection.EnlistTransaction(Transaction.Current);
                result.Message = this.Receive(queue);

                try
                {
                    if (result.Message == null || this.tryProcessMessage(result.Message))
                    {
                        ts.Complete();
                    }
                }
                catch (Exception ex)
                {
                    result.Exception = ex;
                }

                return result;
            }
        }

        private TransportMessage Receive(OracleAQQueue queue)
        {
            OracleAQMessage aqMessage = null;
            try
            {
                aqMessage = queue.Dequeue(this.dequeueOptions);
            }
            catch (OracleException ex)
            {
                if (ex.Number != 25228)
                {
                    throw;
                }
            }

            if (null == aqMessage)
            {
                return null;
            }

            return TransportMessageMapper.DeserializeFromXml(aqMessage);
        }

        private class ReceiveResult
        {
            public Exception Exception { get; set; }

            public TransportMessage Message { get; set; }
        }
    }
}
