namespace NServiceBus.Features
{
    using System;
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

            string connectionString = SettingsHolder.Get<string>("NServiceBus.Transport.ConnectionString");

            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("OracleAQ Transport connection string cannot be empty or null", "connectionString");
            }

            NServiceBus.Configure.Component<DefaultQueueNamePolicy>(DependencyLifecycle.SingleInstance);

            NServiceBus.Configure.Component<OracleAQPurger>(DependencyLifecycle.SingleInstance)
                .ConfigureProperty(p => p.ConnectionString, connectionString);

            NServiceBus.Configure.Component<OracleAQQueueCreator>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, connectionString);

            NServiceBus.Configure.Component<OracleAQMessageSender>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, connectionString);

            NServiceBus.Configure.Component<OracleAQDequeueStrategy>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, connectionString)
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
