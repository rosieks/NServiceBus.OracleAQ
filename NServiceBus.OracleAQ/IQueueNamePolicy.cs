namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.Collections;

    /// <summary>
    /// Interface that definie policy used to convert queue name to database objects.
    /// </summary>
    public interface IQueueNamePolicy
    {
        /// <summary>
        /// Gets queue name in database based on address.
        /// </summary>
        /// <param name="address">Address for which queue name must be returned.</param>
        /// <returns>Queue name for provided address.</returns>
        string GetQueueName(Address address);

        /// <summary>
        /// Gets queue table name in database based on address.
        /// </summary>
        /// <param name="address">Address for which queue table name must be returned.</param>
        /// <returns>Queue table name for provided address.</returns>
        string GetQueueTableName(Address address);
    }

    /// <summary>
    /// Default policy to convert queue name to database objects.
    /// </summary>
    /// <remarks>
    /// Policy run <see cref="NameTransformation"/> function and cache result in hashtable.
    /// 
    /// Default version of <see cref="NameTransaformation"/> function replace dot (.) to underscore (_)
    /// and replace <i>TimeoutsDispatcher</i> to <i>TD</i>.
    /// </remarks>
    public class DefaultQueueNamePolicy : IQueueNamePolicy
    {
        /// <summary>
        /// Name transformation function that convert <see cref="Address.Queue"/> to database object name.
        /// </summary>
        public static Func<string, string> NameTransformation = DefaultNameTransformation;

        private static Hashtable names = new Hashtable();
        private static object lockObject = new object();

        /// <summary>
        /// Gets queue name in database based on address.
        /// </summary>
        /// <param name="address">Address for which queue name must be returned.</param>
        /// <returns>Queue name for provided address.</returns>
        public string GetQueueName(Address address)
        {
            return DefaultQueueNamePolicy.NormalizeName(address);
        }

        /// <summary>
        /// Gets queue table name in database based on address.
        /// </summary>
        /// <param name="address">Address for which queue table name must be returned.</param>
        /// <returns>Queue table name for provided address.</returns>
        /// <remarks>
        /// It returns the same name as <see cref="GetQueueName"/> but with prefix <i>AQ_</i>.
        /// </remarks>
        public string GetQueueTableName(Address address)
        {
            return string.Format("AQ_{0}", DefaultQueueNamePolicy.NormalizeName(address));
        }

        private static string NormalizeName(Address address)
        {
            string name = (string)DefaultQueueNamePolicy.names[address.Queue];

            if (name == null)
            {
                name = DefaultQueueNamePolicy.NameTransformation(address.Queue);
                
                DefaultQueueNamePolicy.SafeAdd(address.Queue, name);
            }

            return name;
        }

        private static string SafeAdd(string queue, string name)
        {
            lock (lockObject)
            {
                var names = (Hashtable)DefaultQueueNamePolicy.names.Clone();
                names[queue] = name;

                DefaultQueueNamePolicy.names = names;
            }

            return name;
        }

        private static string DefaultNameTransformation(string name)
        {
            return name
                .Replace('_', '.')                      // Dot (.) is not allowed as table name.
                .Replace("TimeoutsDispatcher", "TD")    // Oracle object name is limited to 30 characters so we create shortcut.
                .ToUpper();
        }
    }
}
