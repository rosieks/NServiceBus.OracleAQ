# NServiceBus OracleAQ

This library provide ability to use Oracle Advanced Queuing (Oracle AQ) as transport in NServiceBus framework.

## How to configure

    Configure.With()
        .DefaultBuilder()
        .UseTransport<OracleAQ>();

## Send message from PL/SQL

One benefit of using NServiceBus OracleAQ transport is ability to send message from database PL/SQL procedure or code block. 
The following snippet ilustrate how to do that:

    NSB.SEND_MSG(
        endpoint => 'Customers',
        namespace => 'Amazon.Customers.Messages.Commands',
        message => 'CreateCustomer',
        data => '<FirstName>John</FirstName><LastName>Doe</LastName>'
    );

## Create custom queue name policy

If you want to create your own queue name policy you should implement `IQueueNamePolicy`.
Here is sample implementation of it:

    public class MyQueueNamePolicy : IQueueNamePolicy
    {
        public string GetQueueName(Address address)
        {
            return address.Queue.Replace(".", "_").ToUpper();
        }

        public string GetQueueName(Address address)
        {
            return "AQ_" + address.Queue.Replace(".", "_").ToUpper();
        }
    }