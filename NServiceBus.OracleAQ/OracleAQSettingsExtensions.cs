namespace NServiceBus
{
    using NServiceBus.Configuration.AdvanceExtensibility;

    public static class OracleAQSettingsExtensions
    {
        internal static readonly string SchemaKey = "NServiceBus.OracleAQ.Schema";
        internal static readonly string PublishFromDatabaseKey = "NServiceBus.OracleAQ.PublishFromDatabase";

        public static void UseSchema(this TransportExtensions<OracleAQTransport> transportExtensions, string schemaName)
        {
            transportExtensions.GetSettings().Set(SchemaKey, schemaName);
        }

        public static void EnablePublishFromDatabase(this TransportExtensions<OracleAQTransport> transportExtensions)
        {
            transportExtensions.GetSettings().Set("NServiceBus.OracleAQ.PublishFromDatabase", true);
        }
    }
}
