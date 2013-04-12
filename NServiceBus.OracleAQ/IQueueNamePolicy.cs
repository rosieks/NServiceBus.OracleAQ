namespace NServiceBus.Transports.OracleAQ
{
    public interface IQueueNamePolicy
    {
        string GetQueueName(Address address);

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
