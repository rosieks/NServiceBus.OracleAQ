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
    internal class OracleAQTransportFeature : ConfigureTransport
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

        protected override void Configure(FeatureConfigurationContext context, string connectionStringWithSchema)
        {
            Address.IgnoreMachineName();

            string defaultSchema = context.Settings.GetOrDefault<string>("NServiceBus.OracleAQ.Schema");

            string configStringSchema;
            var connectionString = connectionStringWithSchema.ExtractSchemaName(out configStringSchema);
            var localConnectionParams = new ConnectionParams(null, configStringSchema, connectionString, defaultSchema);

            var errorQueue = ErrorQueueSettings.GetConfiguredErrorQueue(context.Settings);

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

            container.ConfigureComponent<ReceiveStrategyFactory>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ErrorQueue, errorQueue)
                .ConfigureProperty(p => p.ConnectionInfo, localConnectionParams);

            container.ConfigureComponent<OracleAQPurger>(DependencyLifecycle.SingleInstance)
                .ConfigureProperty(p => p.ConnectionInfo, localConnectionParams);

            container.ConfigureComponent<OracleAQQueueCreator>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.ConnectionString, connectionString);

            container.ConfigureComponent<OracleAQMessageSender>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.DefaultConnectionString, connectionString)
                .ConfigureProperty(p => p.ConnectionStringCollection, collection);

            container.ConfigureComponent<OracleAQDequeueStrategy>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.SchemaName, localConnectionParams.Schema);
        }
    }
}
