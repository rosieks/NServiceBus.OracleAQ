namespace NServiceBus
{
    public static class OracleAQSettingsExtensions
    {
        public static void UseSchema(this TransportConfiguration transportConfiguration, string schemaName)
        {
            transportConfiguration.Config.Settings.Set("NServiceBus.OracleAQ.Schema", schemaName);
        }
    }
}
