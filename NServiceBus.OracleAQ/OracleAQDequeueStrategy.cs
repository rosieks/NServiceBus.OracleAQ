namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using NServiceBus.CircuitBreakers;
    using NServiceBus.Logging;
    using NServiceBus.Pipeline;
    using NServiceBus.Unicast.Transport;

    /// <summary>
    /// Default implementation of <see cref="IDequeueMessages"/> for OracleAQ.
    /// </summary>
    internal class OracleAQDequeueStrategy : IDequeueMessages, IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(OracleAQDequeueStrategy));
        private readonly RepeatedFailuresOverTimeCircuitBreaker circuitBreaker;
        private readonly ReceiveStrategyFactory receiveStrategyFactory;

        private bool purgeOnStartup;
        private Action<TransportMessage, Exception> endProcessMessage;
        private string workQueue;
        private CancellationTokenSource tokenSource;
        private Address primaryAddress;
        private IReceiveStrategy receiveStrategy;

        public OracleAQDequeueStrategy(ReceiveStrategyFactory receiveStrategyFactory, CriticalError criticalError, Configure config)
        {
            this.receiveStrategyFactory = receiveStrategyFactory;
            this.circuitBreaker = new RepeatedFailuresOverTimeCircuitBreaker(
                "OracleAQTransportConnectivity",
                TimeSpan.FromMinutes(2),
                ex => criticalError.Raise("Repeated failures when communicating with Oracle database", ex),
                TimeSpan.FromSeconds(10));
            this.purgeOnStartup = config.PurgeOnStartup();
        }

        public OracleAQPurger Purger { get; set; }

        /// <summary>
        /// Gets or sets queues name policy.
        /// </summary>
        public IQueueNamePolicy NamePolicy { get; set; }

        /// <summary>
        /// Gets or sets the name of the schema where queues are located
        /// </summary>
        /// <returns></returns>
        public string SchemaName { get; set; }

        public PipelineExecutor PipelineExecutor { get; set; }

        /// <summary>
        /// Initializes the <see cref="IDequeueMessages" />.
        /// </summary>
        /// <param name="address">The address to listen on.</param>
        /// <param name="transactionSettings">The <see cref="TransactionSettings" /> to be used by <see cref="IDequeueMessages" />.</param>
        /// <param name="tryProcessMessage">Called when a message has been dequeued and is ready for processing.</param>
        /// <param name="endProcessMessage">Needs to be called by <see cref="IDequeueMessages" /> after the message has been processed regardless if the outcome was successful or not.</param>
        public void Init(Address address, TransactionSettings transactionSettings, Func<TransportMessage, bool> tryProcessMessage, Action<TransportMessage, Exception> endProcessMessage)
        {
            this.endProcessMessage = endProcessMessage;
            this.primaryAddress = address;
            this.workQueue = this.NamePolicy.GetQueueName(address);

            this.receiveStrategy = this.receiveStrategyFactory.Create(transactionSettings, tryProcessMessage);

            if (this.purgeOnStartup)
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

            for (int i = 0; i < maximumConcurrencyLevel; i++)
            {
                this.StartReceiveThread(new OracleAQQueueWrapper(this.primaryAddress, this.SchemaName, this.NamePolicy));
            }
        }

        /// <summary>
        /// Stops the dequeuing of messages.
        /// </summary>
        public void Stop()
        {
            this.tokenSource.Cancel();
            this.circuitBreaker.Dispose();
        }

        void IDisposable.Dispose()
        {
            this.Stop();
        }

        private void StartReceiveThread(OracleAQQueueWrapper queue)
        {
            CancellationToken token = this.tokenSource.Token;

            Task.Factory
                .StartNew(this.ReceiveLoop, new ReceiveLoopArgs(token, queue), token, TaskCreationOptions.LongRunning, TaskScheduler.Default)
                .ContinueWith(t =>
                {
                    t.Exception.Handle(ex =>
                        {
                            Logger.Warn("An exeception occurred when connecting to the configured Oracle database", ex);
                            this.circuitBreaker.Failure(ex);
                            return true;
                        });

                    if (!this.tokenSource.IsCancellationRequested)
                    {
                        this.StartReceiveThread(queue);
                    }
                }, TaskContinuationOptions.OnlyOnFaulted);
        }

        private void ReceiveLoop(object obj)
        {
            var args = (ReceiveLoopArgs)obj;

            while (!args.Token.IsCancellationRequested)
            {
                var result = ReceiveResult.NoMessage();

                try
                {
                    result = this.receiveStrategy.TryReceiveFrom(args.Queue);
                }
                finally
                {
                    if (result.HasReceivedMessage)
                    {
                        this.endProcessMessage(result.Message, result.Exception);
                    }
                }

                this.circuitBreaker.Success();
            }
        }

        private class ReceiveLoopArgs
        {
            public readonly OracleAQQueueWrapper Queue;
            public readonly CancellationToken Token;

            public ReceiveLoopArgs(CancellationToken token, OracleAQQueueWrapper queue)
            {
                this.Token = token;
                this.Queue = queue;
            }
        }
    }
}
