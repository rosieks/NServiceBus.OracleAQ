namespace NServiceBus.Transports.OracleAQ.Config
{
    using System;
    using NServiceBus.Unicast.Queuing.Installers;
    using OracleAQ = NServiceBus.OracleAQ;

    public class OracleAQTransportConfigurer : ConfigureTransport<OracleAQ>
    {
        protected override string ExampleConnectionStringForErrorMessage
        {
            get
            {
                return @"user id=scott;password=Pwd4Sct;data source=oracle";
            }
        }

        protected override void InternalConfigure(Configure config, string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("OracleAQ Transport connection string cannot be empty or null", "connectionString");
            }

            config.Configurer.ConfigureComponent<DefaultQueueNamePolicy>(DependencyLifecycle.SingleInstance);

            config.Configurer.ConfigureComponent<OracleAQPurger>(DependencyLifecycle.SingleInstance)
                .ConfigureProperty(p => p.ConnectionString, connectionString);

            config.Configurer.ConfigureComponent<OracleAQQueueCreator>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, connectionString);

            config.Configurer.ConfigureComponent<OracleAQMessageSender>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, connectionString);

            config.Configurer.ConfigureComponent<OracleAQDequeueStrategy>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, connectionString)
                .ConfigureProperty(p => p.PurgeOnStartup, ConfigurePurging.PurgeRequested);

            EndpointInputQueueCreator.Enabled = true;
        }
    }
}
