namespace NServiceBus.Transports.OracleAQ
{
    /// <summary>
    /// Interface that definie policy used to convert queue name to database objects.
    /// </summary>
    public interface IQueueNamePolicy
    {
        /// <summary>
        /// Gets queue name in database based on address.
        /// </summary>
        /// <param name="address">Address for which queue name must be returned.</param>
        /// <returns>Queue name for provided address.</returns>
        string GetQueueName(Address address);

        /// <summary>
        /// Gets queue table name in database based on address.
        /// </summary>
        /// <param name="address">Address for which queue table name must be returned.</param>
        /// <returns>Queue table name for provided address.</returns>
        string GetQueueTableName(Address address);
    }

    public class DefaultQueueNamePolicy : IQueueNamePolicy
    {
        public string GetQueueName(Address address)
        {
            return address.Queue.Replace(".", "_").ToUpper();
        }

        public string GetQueueTableName(Address address)
        {
            return string.Format("AQ_{0}", address.Queue.Replace(".", "_")).ToUpper();
        }
    }
}
