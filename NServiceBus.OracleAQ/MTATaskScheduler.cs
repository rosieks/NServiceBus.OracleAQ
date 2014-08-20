namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    internal sealed class MTATaskScheduler : TaskScheduler, IDisposable
    {
        private bool disposed;

        /// <summary>
        /// Stores the queued tasks to be executed by our pool of STA threads.
        /// </summary>
        private BlockingCollection<Task> tasks;

        /// <summary>
        /// The MTA threads used by the scheduler.
        /// </summary>
        private readonly List<Thread> threads;

        /// <summary>Initializes a new instance of the MTATaskScheduler class with the specified concurrency level.</summary>
        /// <param name="numberOfThreads">The number of threads that should be created and used by this scheduler.</param>
        /// <param name="nameFormat">The template name form to use to name threads.</param>
        public MTATaskScheduler(int numberOfThreads, string nameFormat)
        {
            if (numberOfThreads < 1)
            {
                throw new ArgumentOutOfRangeException("numberOfThreads");
            }

            this.tasks = new BlockingCollection<Task>();

            // Create the threads to be used by this scheduler
            this.threads = Enumerable.Range(0, numberOfThreads).Select(i =>
            {
                var thread = new Thread(
                    () =>
                        {
                            // Continually get the next task and try to execute it.
                            // This will continue until the scheduler is disposed and no more tasks remain.
                            foreach (var t in tasks.GetConsumingEnumerable())
                            {
                                this.TryExecuteTask(t);
                            }
                        });

                thread.IsBackground = true;
                thread.SetApartmentState(ApartmentState.MTA);
                thread.Name = string.Format("{0} - {1}", nameFormat, thread.ManagedThreadId);
                return thread;
            }).ToList();

            // Start all of the threads
            foreach (var thread in this.threads)
            {
                thread.Start();
            }
        }

        /// <summary>Queues a Task to be executed by this scheduler.</summary>
        /// <param name="task">The task to be executed.</param>
        protected override void QueueTask(Task task)
        {
            this.tasks.Add(task);
        }

        /// <summary>Provides a list of the scheduled tasks for the debugger to consume.</summary>
        /// <returns>An enumerable of all tasks currently scheduled.</returns>
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            return this.tasks.ToArray();
        }

        /// <summary>Determines whether a Task may be inlined.</summary>
        /// <param name="task">The task to be executed.</param>
        /// <param name="taskWasPreviouslyQueued">Whether the task was previously queued.</param>
        /// <returns>true if the task was successfully inlined; otherwise, false.</returns>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return false;
        }

        /// <summary>
        /// Gets the maximum concurrency level supported by this scheduler.
        /// </summary>
        public override int MaximumConcurrencyLevel
        {
            get
            {
                return this.threads.Count;
            }
        }

        /// <summary>
        /// Cleans up the scheduler by indicating that no more tasks will be queued.
        /// This method blocks until all threads successfully shutdown.
        /// </summary>
        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
            if (this.tasks != null)
            {
                // Indicate that no new tasks will be coming in
                this.tasks.CompleteAdding();

                // Wait for all threads to finish processing tasks
                foreach (var thread in this.threads)
                {
                    thread.Join(5 * 1000);
                    if (thread.ThreadState != ThreadState.Stopped)
                    {
                        thread.Abort();
                    }
                }

                // Cleanup
                this.tasks.Dispose();
                this.tasks = null;
            }
        }
    }
}
