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
    public class OracleAQTransportFeature : ConfigureTransport
    {
        protected override string ExampleConnectionStringForErrorMessage
        {
            get
            {
                return @"user id=scott;password=Pwd4Sct;data source=oracle";
            }
        }

        protected override string GetLocalAddress(ReadOnlySettings settings)
        {
            return settings.EndpointName();
        }

        protected override void Configure(FeatureConfigurationContext context, string connectionString)
        {
            Address.IgnoreMachineName();

            string schema;
            context.Settings.TryGet<string>("NServiceBus.OracleAQ.Schema", out schema);

            var collection = ConfigurationManager
                .ConnectionStrings
                .Cast<ConnectionStringSettings>()
                .Where(x => x.Name.StartsWith("NServiceBus/Transport/"))
                .ToDictionary(x => x.Name.Replace("NServiceBus/Transport/", string.Empty), y => y.ConnectionString);

            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("OracleAQ Transport connection string cannot be empty or null");
            }

            var container = context.Container;
            container.ConfigureComponent<DefaultQueueNamePolicy>(DependencyLifecycle.SingleInstance);

            container.ConfigureComponent<OracleAQPurger>(DependencyLifecycle.SingleInstance)
                .ConfigureProperty(p => p.ConnectionString, connectionString)
                .ConfigureProperty(p => p.Schema, schema);

            container.ConfigureComponent<OracleAQQueueCreator>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, connectionString)
                .ConfigureProperty(p => p.Schema, schema);

            container.ConfigureComponent<OracleAQMessageSender>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.DefaultConnectionString, connectionString)
                .ConfigureProperty(p => p.ConnectionStringCollection, collection)
                .ConfigureProperty(p => p.Schema, schema);

            container.ConfigureComponent<OracleAQDequeueStrategy>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, connectionString)
                .ConfigureProperty(p => p.Schema, schema);
        }
    }
}
