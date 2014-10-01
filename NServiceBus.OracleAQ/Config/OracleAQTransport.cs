namespace NServiceBus
{
    using NServiceBus.Configuration.AdvanceExtensibility;
    using NServiceBus.Features;
    using NServiceBus.Transports;

    /// <summary>
    /// Transport definition for OracleAQ.
    /// </summary>
    public class OracleAQTransport : TransportDefinition
    {
        protected override void Configure(BusConfiguration config)
        {
            config.EnableFeature<Features.OracleAQTransportFeature>();
            config.EnableFeature<MessageDrivenSubscriptions>();
            config.EnableFeature<TimeoutManagerBasedDeferral>();
            config.GetSettings().EnableFeatureByDefault<StorageDrivenPublishing>();
            config.GetSettings().EnableFeatureByDefault<TimeoutManager>();
        }
    }
}
