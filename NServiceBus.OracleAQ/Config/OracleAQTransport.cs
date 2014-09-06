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

        public override void Initialize()
        {
            OracleAQTransport.CustomizeAddress();

            string defaultConnectionString = SettingsHolder.Get<string>("NServiceBus.Transport.ConnectionString");

            var collection = ConfigurationManager
                .ConnectionStrings
                .Cast<ConnectionStringSettings>()
                .Where(x => x.Name.StartsWith("NServiceBus/Transport/"))
                .ToDictionary(x => x.Name.Replace("NServiceBus/Transport/", string.Empty), y => y.ConnectionString);

            if (string.IsNullOrEmpty(defaultConnectionString))
            {
                throw new ArgumentException("OracleAQ Transport connection string cannot be empty or null");
            }

            NServiceBus.Configure.Component<DefaultQueueNamePolicy>(DependencyLifecycle.SingleInstance);

            NServiceBus.Configure.Component<OracleAQPurger>(DependencyLifecycle.SingleInstance)
                .ConfigureProperty(p => p.ConnectionString, defaultConnectionString);

            NServiceBus.Configure.Component<OracleAQQueueCreator>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, defaultConnectionString);

            NServiceBus.Configure.Component<OracleAQMessageSender>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.DefaultConnectionString, defaultConnectionString)
                .ConfigureProperty(p => p.ConnectionStringCollection, collection);

            NServiceBus.Configure.Component<OracleAQDequeueStrategy>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, defaultConnectionString)
                .ConfigureProperty(p => p.PurgeOnStartup, ConfigurePurging.PurgeRequested);
        }

        protected override void InternalConfigure(Configure config)
        {
            Feature.Enable<OracleAQTransport>();
            Feature.Enable<MessageDrivenSubscriptions>();
        }

        private static void CustomizeAddress()
        {
            Address.IgnoreMachineName();

            if (!SettingsHolder.GetOrDefault<bool>("ScaleOut.UseSingleBrokerQueue"))
            {
                Address.InitializeLocalAddress(Address.Local.Queue + "." + Address.Local.Machine);
            }
        }
    }
}
