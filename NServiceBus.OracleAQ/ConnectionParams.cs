namespace NServiceBus.Transports.OracleAQ
{
    using System;

    internal class ConnectionParams
    {
        private readonly string connectionString;
        private readonly string schema;

        public ConnectionParams(string specificConnectionString, string specificSchema, string defaultConnectionString, string defaultSchema)
        {
            if (defaultConnectionString == null)
            {
                throw new ArgumentNullException("defaultConnectionString");
            }

            this.connectionString = specificConnectionString ?? defaultConnectionString;
            this.schema = specificSchema ?? defaultSchema;
        }

        public string ConnectionString
        {
            get { return this.connectionString; }
        }

        public string Schema
        {
            get { return this.schema; }
        }

        public ConnectionParams MakeSpecific(string specificConnectionString, string specificSchema)
        {
            return new ConnectionParams(specificConnectionString, specificSchema, this.ConnectionString, this.Schema);
        }
    }
}
