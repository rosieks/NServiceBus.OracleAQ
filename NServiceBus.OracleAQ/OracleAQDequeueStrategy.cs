namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using NServiceBus.CircuitBreakers;
    using NServiceBus.Logging;
    using NServiceBus.Unicast.Transport;
    using Oracle.DataAccess.Client;

    /// <summary>
    /// Default implementation of <see cref="IDequeueMessages"/> for OracleAQ.
    /// </summary>
    public class OracleAQDequeueStrategy : IDequeueMessages, IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(OracleAQDequeueStrategy));
        private readonly RepeatedFailuresOverTimeCircuitBreaker circuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker(
            "OracleAQTransportConnectivity",
            TimeSpan.FromMinutes(2),
            ex => ConfigureCriticalErrorAction.RaiseCriticalError("Repeated failures when communicating with Oracle database", ex),
            TimeSpan.FromSeconds(10));

        private TransactionOptions transactionOptions;
        private Func<TransportMessage, bool> tryProcessMessage;
        private Action<TransportMessage, Exception> endProcessMessage;
        private string workQueue;
        private string clientInfo;
        private MTATaskScheduler scheduler;
        private CancellationTokenSource tokenSource;
        private OracleAQDequeueOptions dequeueOptions;

        public OracleAQPurger Purger { get; set; }

        /// <summary>
        /// Gets or sets queues name policy.
        /// </summary>
        public IQueueNamePolicy NamePolicy { get; set; }

        /// <summary>
        /// Determines if the queue should be purged when the transport starts.
        /// </summary>
        public bool PurgeOnStartup { get; set; }

        /// <summary>
        /// Gets or sets the connection string used to open the Oracle database.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Initializes the <see cref="IDequeueMessages" />.
        /// </summary>
        /// <param name="address">The address to listen on.</param>
        /// <param name="transactionSettings">The <see cref="TransactionSettings" /> to be used by <see cref="IDequeueMessages" />.</param>
        /// <param name="tryProcessMessage">Called when a message has been dequeued and is ready for processing.</param>
        /// <param name="endProcessMessage">Needs to be called by <see cref="IDequeueMessages" /> after the message has been processed regardless if the outcome was successful or not.</param>
        public void Init(Address address, TransactionSettings transactionSettings, Func<TransportMessage, bool> tryProcessMessage, Action<TransportMessage, Exception> endProcessMessage)
        {
            this.tryProcessMessage = tryProcessMessage;
            this.endProcessMessage = endProcessMessage;
            this.workQueue = this.NamePolicy.GetQueueName(address);
            this.clientInfo = string.Format("OracleAQDequeueStrategy for {0}", this.workQueue);

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

        /// <summary>
        /// Starts the dequeuing of message using the specified <paramref name="maximumConcurrencyLevel" />.
        /// </summary>
        /// <param name="maximumConcurrencyLevel">Indicates the maximum concurrency level this <see cref="IDequeueMessages" /> is able to support.</param>
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

        /// <summary>
        /// Stops the dequeuing of messages.
        /// </summary>
        public void Stop()
        {
            this.tokenSource.Cancel();
            this.scheduler.Dispose();
            this.circuitBreaker.Dispose();
        }

        void IDisposable.Dispose()
        {
            this.Stop();
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

                            this.SetupClientInfo(connection);

                            queue.Listen(null);

                            result = this.TryReceive(queue);
                        }
                    }
                }
                finally
                {
                    if (result.Message != null)
                    {
                        this.endProcessMessage(result.Message, result.Exception);
                    }
                }

                this.circuitBreaker.Success();
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
                        // NOTE: We explicitly calling Dispose so that we force any exception to not bubble,
                        // eg Concurrency/Deadlock exception.
                        ts.Complete();
                        ts.Dispose();
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
                if (ex.Number != OraCodes.TimeoutOrEndOfFetch)
                {
                    throw;
                }
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
