namespace NServiceBus.Features
{
    using System;
    using System.Configuration;
    using System.Linq;
    using NServiceBus.Settings;
    using NServiceBus.Transports;
    using NServiceBus.Transports.OracleAQ;

    /// <summary>
    /// Configures NServiceBus to use OracleAQ as the default transport.
    /// </summary>
    public class OracleAQTransport : ConfigureTransport<OracleAQ>
    {
        protected override string ExampleConnectionStringForErrorMessage
        {
            get
            {
                return @"user id=scott;password=Pwd4Sct;data source=oracle";
            }
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            OracleAQTransport.CustomizeAddress(context.Settings);

            string defaultConnectionString = context.Settings.Get<string>("NServiceBus.Transport.ConnectionString");
            string schema = context.Settings.Get<string>("NServiceBus.OracleAQ.Schema");

            var collection = ConfigurationManager
                .ConnectionStrings
                .Cast<ConnectionStringSettings>()
                .Where(x => x.Name.StartsWith("NServiceBus/Transport/"))
                .ToDictionary(x => x.Name.Replace("NServiceBus/Transport/", string.Empty), y => y.ConnectionString);

            if (string.IsNullOrEmpty(defaultConnectionString))
            {
                throw new ArgumentException("OracleAQ Transport connection string cannot be empty or null");
            }

            var container = context.Container;
            container.ConfigureComponent<DefaultQueueNamePolicy>(DependencyLifecycle.SingleInstance);

            container.ConfigureComponent<OracleAQPurger>(DependencyLifecycle.SingleInstance)
                .ConfigureProperty(p => p.ConnectionString, defaultConnectionString);

            container.ConfigureComponent<OracleAQQueueCreator>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, defaultConnectionString)
                .ConfigureProperty(p => p.Schema, schema);

            container.ConfigureComponent<OracleAQMessageSender>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.DefaultConnectionString, defaultConnectionString)
                .ConfigureProperty(p => p.ConnectionStringCollection, collection)
                .ConfigureProperty(p => p.Schema, schema);

            container.ConfigureComponent<OracleAQDequeueStrategy>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, defaultConnectionString)
                .ConfigureProperty(p => p.PurgeOnStartup, ConfigurePurging.PurgeRequested)
                .ConfigureProperty(p => p.Schema, schema);
        }

        protected override void InternalConfigure(Configure config)
        {
            config.EnableFeature<OracleAQTransport>();
            config.EnableFeature<MessageDrivenSubscriptions>();
            config.EnableFeature<TimeoutManagerBasedDeferral>();
            config.Settings.EnableFeatureByDefault<StorageDrivenPublishing>();
            config.Settings.EnableFeatureByDefault<TimeoutManager>();
        }

        private static void CustomizeAddress(ReadOnlySettings settings)
        {
            Address.IgnoreMachineName();

            if (!settings.GetOrDefault<bool>("ScaleOut.UseSingleBrokerQueue"))
            {
                Address.InitializeLocalAddress(Address.Local.Queue + "." + Address.Local.Machine);
            }
        }
    }
}
