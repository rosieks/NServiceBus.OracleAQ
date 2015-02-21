namespace NServiceBus.Transports.OracleAQ
{
    internal interface IReceiveStrategy
    {
        ReceiveResult TryReceiveFrom(OracleAQQueueWrapper queue);
    }
}
