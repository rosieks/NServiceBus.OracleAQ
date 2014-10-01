namespace NServiceBus
{
    using NServiceBus.Configuration.AdvanceExtensibility;

    public static class OracleAQSettingsExtensions
    {
        public static void UseSchema(this TransportExtensions<OracleAQTransport> transportExtensions, string schemaName)
        {
            transportExtensions.GetSettings().Set("NServiceBus.OracleAQ.Schema", schemaName);
        }
    }
}
