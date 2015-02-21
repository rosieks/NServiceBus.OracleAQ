# NServiceBus OracleAQ

This library provide ability to use Oracle Advanced Queuing (Oracle AQ) as transport in NServiceBus framework.

### How to configure


You can specify transport as part of the IConfigureThisEndpoint class declaration, e.g:

    public class EndpointConfig : IConfigureThisEndpoint, AsA_Server, UsingTransport<OracleAQ>
    {
    }

Or you can specify it using fluent configuration:

    Configure.With()
        .UseTransport<OracleAQ>();

### Send message from PL/SQL

One benefit of using NServiceBus OracleAQ transport is ability to send message from database PL/SQL procedure or code block. 
The following snippet ilustrate how to do that:

    DECLARE
        headers NSB.HEADERS;
    BEGIN

		-- Configure message headers
		headers('User') := sys_context('USERENV','CURRENT_USER');

		-- Send message
		NSB.SEND(
			endpoint => 'Customers',
			namespace => 'Amazon.Customers.Messages.Commands',
			message => 'CreateCustomer',
			data => '<FirstName>John</FirstName><LastName>Doe</LastName>',
			headers => headers
		);
	END;

### Create custom queue name policy

If you want to create your own queue name policy you should implement `IQueueNamePolicy`.
Here is sample implementation of it:

    public class MyQueueNamePolicy : IQueueNamePolicy
    {
        public string GetQueueName(Address address)
        {
            return address.Queue.Replace(".", "_").ToUpper();
        }

        public string GetQueueTableName(Address address)
        {
            return "AQ_" + address.Queue.Replace(".", "_").ToUpper();
        }
    }

### Configure different schema

If you want to keep queues in different schema than rest of your application but still have single connection string you can 
configure your endpoint in such way:

    var config = new BusConfiguration();
    config.UseTransport<OracleAQTransport>()
        .UseSchema("queues");