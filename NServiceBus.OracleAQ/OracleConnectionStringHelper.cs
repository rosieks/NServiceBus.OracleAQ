using System;
using System.Collections.Concurrent;
using System.Data.Common;
using Oracle.DataAccess.Client;

namespace NServiceBus.Transports.OracleAQ
{
    internal static class OracleConnectionStringHelper
    {
        private static ConcurrentDictionary<string, bool> canEnlistConnectionString = new ConcurrentDictionary<string, bool>();

        public static bool CanEnlist(string connectionString)
        {
            bool canEnlist;
            if (!OracleConnectionStringHelper.canEnlistConnectionString.TryGetValue(connectionString, out canEnlist))
            {
                // We can enlist connection if connectionString doesn't have "enlist=false;".
                OracleConnectionStringBuilder builder = new OracleConnectionStringBuilder(connectionString);
                canEnlist = !string.Equals(builder.Enlist, "false", StringComparison.OrdinalIgnoreCase);
                OracleConnectionStringHelper.canEnlistConnectionString.TryAdd(connectionString, canEnlist);
            }

            return canEnlist;
        }

        public static string ExtractSchemaName(this string connectionString, out string schemaName)
        {
            const string key = "Queue Schema";

            var connectionStringParser = new DbConnectionStringBuilder
            {
                ConnectionString = connectionString,
            };

            if (connectionStringParser.ContainsKey(key))
            {
                schemaName = (string)connectionStringParser[key];
                connectionStringParser.ContainsKey(key);
                connectionString = connectionStringParser.ConnectionString;
            }
            else
            {
                schemaName = null;
            }

            return connectionString;
        }
    }
}
